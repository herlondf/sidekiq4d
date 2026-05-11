unit Sidekiq4D.Queue.RedisStreams;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.SyncObjs,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Redis.Client;

type
  TSidekiqRedisStreamsAdapter = class(
    TInterfacedObject,
    ISidekiqQueueAdapter,
    ISidekiqQueuePublisher,
    ISidekiqQueueBatchPublisher)
  private
    FLock: TCriticalSection;
    FConnectionString: string;
    FStreamKey: string;
    FGroupName: string;
    FConsumerName: string;
    FDeadLetterStreamKey: string;
    FClient: ISidekiqRedisClient;  // Persistent connection — avoids per-call churn

    function GetClient: ISidekiqRedisClient;
    function ParseActionFromBody(const ABody: string): string;
    function BuildXAddCommand(
      const ARequest: TSidekiqPublishRequest): TArray<string>;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqRedisStreamsAdapter; static;

    function ConnectionString(const AValue: string): TSidekiqRedisStreamsAdapter;
    function StreamKey(const AValue: string): TSidekiqRedisStreamsAdapter;
    function GroupName(const AValue: string): TSidekiqRedisStreamsAdapter;
    function ConsumerName(const AValue: string): TSidekiqRedisStreamsAdapter;
    function DeadLetterStreamKey(const AValue: string): TSidekiqRedisStreamsAdapter;

    { ISidekiqQueueAdapter }
    function Name: string;
    function Fetch(
      const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(
      const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(
      const AJob: ISidekiqJobEnvelope; const AReason: string);

    { ISidekiqQueuePublisher }
    procedure Publish(const ARequest: TSidekiqPublishRequest);

    { ISidekiqQueueBatchPublisher }
    procedure PublishBatch(const ARequests: TArray<TSidekiqPublishRequest>);
  end;

implementation

uses
  Sidekiq4D.Redis4D.Client;

{ TSidekiqRedisStreamsAdapter }

constructor TSidekiqRedisStreamsAdapter.Create;
begin
  inherited Create;
  FLock                := TCriticalSection.Create;
  FConnectionString    := 'redis://localhost:6379/0';
  FStreamKey           := 'sidekiq4d:jobs';
  FGroupName           := 'workers';
  FConsumerName        := 'worker-1';
  FDeadLetterStreamKey := '';
end;

destructor TSidekiqRedisStreamsAdapter.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function TSidekiqRedisStreamsAdapter.New: TSidekiqRedisStreamsAdapter;
begin
  Result := TSidekiqRedisStreamsAdapter.Create;
end;

function TSidekiqRedisStreamsAdapter.ConnectionString(
  const AValue: string): TSidekiqRedisStreamsAdapter;
begin
  Result := Self;
  FConnectionString := AValue.Trim;
end;

function TSidekiqRedisStreamsAdapter.StreamKey(
  const AValue: string): TSidekiqRedisStreamsAdapter;
begin
  Result := Self;
  FStreamKey := AValue.Trim;
end;

function TSidekiqRedisStreamsAdapter.GroupName(
  const AValue: string): TSidekiqRedisStreamsAdapter;
begin
  Result := Self;
  FGroupName := AValue.Trim;
end;

function TSidekiqRedisStreamsAdapter.ConsumerName(
  const AValue: string): TSidekiqRedisStreamsAdapter;
begin
  Result := Self;
  FConsumerName := AValue.Trim;
end;

function TSidekiqRedisStreamsAdapter.DeadLetterStreamKey(
  const AValue: string): TSidekiqRedisStreamsAdapter;
begin
  Result := Self;
  FDeadLetterStreamKey := AValue.Trim;
end;

function TSidekiqRedisStreamsAdapter.GetClient: ISidekiqRedisClient;
begin
  if not Assigned(FClient) then
  begin
    FLock.Acquire;
    try
      if not Assigned(FClient) then
        FClient := TRedis4DClientBridge.NewFromConnectionString(FConnectionString);
    finally
      FLock.Release;
    end;
  end;
  Result := FClient;
end;

function TSidekiqRedisStreamsAdapter.Name: string;
begin
  Result := 'redis-streams';
end;

function TSidekiqRedisStreamsAdapter.Fetch(
  const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
var
  LClient: ISidekiqRedisClient;
  LStreams: ISidekiqRedisStreams;
  LMessages: TArray<TSidekiqStreamMessage>;
  LMsg: TSidekiqStreamMessage;
  LEnvelope: TSidekiqJobEnvelope;
  LBody, LAction: string;
  LAttempts, LBatchSize, LCount: Integer;
  LAttrsJson: TJSONValue;
  LPair: TJSONPair;
begin
  SetLength(Result, 0);
  LBatchSize := AOptions.BatchSize;
  if LBatchSize <= 0 then
    LBatchSize := 1;

  LClient  := GetClient;
  LStreams := LClient.Streams;
  LStreams.CreateGroup(FStreamKey, FGroupName, '0');

  LMessages := LStreams.ReadGroup(
    FGroupName, FConsumerName, FStreamKey, '>',
    LBatchSize, AOptions.WaitTimeSeconds * 1000);

  LCount := 0;
  SetLength(Result, Length(LMessages));

  for LMsg in LMessages do
  begin
    LBody    := LMsg.FieldValue('body');
    LAction  := LMsg.FieldValue('action');
    if LAction.IsEmpty then
      LAction := ParseActionFromBody(LBody);
    LAttempts := StrToIntDef(LMsg.FieldValue('attempts'), 0);

    LEnvelope := TSidekiqJobEnvelope.Create(
      LMsg.ID, FStreamKey, LMsg.ID, LBody, LAction, LAttempts);

    if LMsg.HasField('attributes') then
    begin
      LAttrsJson := TJSONObject.ParseJSONValue(LMsg.FieldValue('attributes'));
      try
        if LAttrsJson is TJSONObject then
          for LPair in TJSONObject(LAttrsJson) do
            LEnvelope.AddAttribute(LPair.JsonString.Value, LPair.JsonValue.Value);
      finally
        LAttrsJson.Free;
      end;
    end;

    Result[LCount] := LEnvelope;
    Inc(LCount);
  end;

  SetLength(Result, LCount);
end;

procedure TSidekiqRedisStreamsAdapter.Ack(const AJob: ISidekiqJobEnvelope);
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;
  GetClient.Streams.Acknowledge(FStreamKey, FGroupName, [AJob.ReceiptHandle]);
end;

procedure TSidekiqRedisStreamsAdapter.Nack(
  const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
begin
  { Redis Streams does not natively support delayed redelivery.
    The message remains pending in the consumer group and will be
    reclaimed via XCLAIM or XAUTOCLAIM after the idle timeout. }
end;

procedure TSidekiqRedisStreamsAdapter.MoveToDeadLetter(
  const AJob: ISidekiqJobEnvelope; const AReason: string);
var
  LFields: TStringList;
begin
  if not FDeadLetterStreamKey.IsEmpty then
  begin
    LFields := TStringList.Create;
    try
      LFields.Values['body']                    := AJob.Body;
      LFields.Values['action']                  := AJob.Action;
      LFields.Values['x-sidekiq-reason']        := AReason;
      LFields.Values['x-sidekiq-source-stream'] := FStreamKey;
      GetClient.Streams.Add(FDeadLetterStreamKey, '*', LFields);
    finally
      LFields.Free;
    end;
  end;
  GetClient.Streams.Acknowledge(FStreamKey, FGroupName, [AJob.ReceiptHandle]);
end;

function TSidekiqRedisStreamsAdapter.BuildXAddCommand(
  const ARequest: TSidekiqPublishRequest): TArray<string>;
var
  LAttrsJson: TJSONObject;
  LIndex: Integer;
  LHasAttrs: Boolean;
begin
  LHasAttrs := Length(ARequest.Attributes) > 0;
  if LHasAttrs then
  begin
    LAttrsJson := TJSONObject.Create;
    try
      for LIndex := 0 to High(ARequest.Attributes) do
        LAttrsJson.AddPair(
          ARequest.Attributes[LIndex].Key,
          ARequest.Attributes[LIndex].Value);
      Result := ['XADD', FStreamKey, '*',
        'body',       ARequest.Body,
        'action',     ARequest.Action,
        'attributes', LAttrsJson.ToJSON];
    finally
      LAttrsJson.Free;
    end;
  end
  else
    Result := ['XADD', FStreamKey, '*',
      'body',   ARequest.Body,
      'action', ARequest.Action];
end;

procedure TSidekiqRedisStreamsAdapter.Publish(
  const ARequest: TSidekiqPublishRequest);
var
  LPipeline: ISidekiqRedisPipeline;
begin
  LPipeline := GetClient.BeginPipeline;
  LPipeline.QueueCommand(BuildXAddCommand(ARequest));
  LPipeline.Execute;
end;

procedure TSidekiqRedisStreamsAdapter.PublishBatch(
  const ARequests: TArray<TSidekiqPublishRequest>);
var
  LPipeline: ISidekiqRedisPipeline;
  LRequest: TSidekiqPublishRequest;
begin
  if Length(ARequests) = 0 then
    Exit;
  LPipeline := GetClient.BeginPipeline;
  for LRequest in ARequests do
    LPipeline.QueueCommand(BuildXAddCommand(LRequest));
  LPipeline.Execute;
end;

function TSidekiqRedisStreamsAdapter.ParseActionFromBody(
  const ABody: string): string;
var
  LJsonValue: TJSONValue;
  LJsonObject: TJSONObject;
begin
  Result := '';
  if ABody.Trim.IsEmpty then
    Exit;
  LJsonValue := TJSONObject.ParseJSONValue(ABody);
  try
    if not (LJsonValue is TJSONObject) then
      Exit;
    LJsonObject := TJSONObject(LJsonValue);
    if Assigned(LJsonObject.GetValue('action')) then
      Result := LJsonObject.GetValue<string>('action');
  finally
    LJsonValue.Free;
  end;
end;

end.
