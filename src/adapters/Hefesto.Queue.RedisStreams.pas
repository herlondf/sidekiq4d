unit Hefesto.Queue.RedisStreams;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.SyncObjs,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Redis.Client;

type
  THefestoRedisStreamsAdapter = class(
    TInterfacedObject,
    IHefestoQueueAdapter,
    IHefestoQueuePublisher,
    IHefestoQueueBatchPublisher)
  private
    FLock: TCriticalSection;
    FConnectionString: string;
    FStreamKey: string;
    FGroupName: string;
    FConsumerName: string;
    FDeadLetterStreamKey: string;
    FClient: IHefestoRedisClient;  // Persistent connection — avoids per-call churn

    function GetClient: IHefestoRedisClient;
    function ParseActionFromBody(const ABody: string): string;
    function BuildXAddCommand(
      const ARequest: THefestoPublishRequest): TArray<string>;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoRedisStreamsAdapter; static;

    function ConnectionString(const AValue: string): THefestoRedisStreamsAdapter;
    function StreamKey(const AValue: string): THefestoRedisStreamsAdapter;
    function GroupName(const AValue: string): THefestoRedisStreamsAdapter;
    function ConsumerName(const AValue: string): THefestoRedisStreamsAdapter;
    function DeadLetterStreamKey(const AValue: string): THefestoRedisStreamsAdapter;

    { IHefestoQueueAdapter }
    function Name: string;
    function Fetch(
      const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(
      const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(
      const AJob: IHefestoJobEnvelope; const AReason: string);

    { IHefestoQueuePublisher }
    procedure Publish(const ARequest: THefestoPublishRequest);

    { IHefestoQueueBatchPublisher }
    procedure PublishBatch(const ARequests: TArray<THefestoPublishRequest>);
  end;

implementation

uses
  Hefesto.Redis4D.Client;

{ THefestoRedisStreamsAdapter }

constructor THefestoRedisStreamsAdapter.Create;
begin
  inherited Create;
  FLock                := TCriticalSection.Create;
  FConnectionString    := 'redis://localhost:6379/0';
  FStreamKey           := 'sidekiq4d:jobs';
  FGroupName           := 'workers';
  FConsumerName        := 'worker-1';
  FDeadLetterStreamKey := '';
end;

destructor THefestoRedisStreamsAdapter.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function THefestoRedisStreamsAdapter.New: THefestoRedisStreamsAdapter;
begin
  Result := THefestoRedisStreamsAdapter.Create;
end;

function THefestoRedisStreamsAdapter.ConnectionString(
  const AValue: string): THefestoRedisStreamsAdapter;
begin
  Result := Self;
  FConnectionString := AValue.Trim;
end;

function THefestoRedisStreamsAdapter.StreamKey(
  const AValue: string): THefestoRedisStreamsAdapter;
begin
  Result := Self;
  FStreamKey := AValue.Trim;
end;

function THefestoRedisStreamsAdapter.GroupName(
  const AValue: string): THefestoRedisStreamsAdapter;
begin
  Result := Self;
  FGroupName := AValue.Trim;
end;

function THefestoRedisStreamsAdapter.ConsumerName(
  const AValue: string): THefestoRedisStreamsAdapter;
begin
  Result := Self;
  FConsumerName := AValue.Trim;
end;

function THefestoRedisStreamsAdapter.DeadLetterStreamKey(
  const AValue: string): THefestoRedisStreamsAdapter;
begin
  Result := Self;
  FDeadLetterStreamKey := AValue.Trim;
end;

function THefestoRedisStreamsAdapter.GetClient: IHefestoRedisClient;
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

function THefestoRedisStreamsAdapter.Name: string;
begin
  Result := 'redis-streams';
end;

function THefestoRedisStreamsAdapter.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
var
  LClient: IHefestoRedisClient;
  LStreams: IHefestoRedisStreams;
  LMessages: TArray<THefestoStreamMessage>;
  LMsg: THefestoStreamMessage;
  LEnvelope: THefestoJobEnvelope;
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

    LEnvelope := THefestoJobEnvelope.Create(
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

procedure THefestoRedisStreamsAdapter.Ack(const AJob: IHefestoJobEnvelope);
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;
  GetClient.Streams.Acknowledge(FStreamKey, FGroupName, [AJob.ReceiptHandle]);
end;

procedure THefestoRedisStreamsAdapter.Nack(
  const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
begin
  { Redis Streams does not natively support delayed redelivery.
    The message remains pending in the consumer group and will be
    reclaimed via XCLAIM or XAUTOCLAIM after the idle timeout. }
end;

procedure THefestoRedisStreamsAdapter.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope; const AReason: string);
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

function THefestoRedisStreamsAdapter.BuildXAddCommand(
  const ARequest: THefestoPublishRequest): TArray<string>;
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

procedure THefestoRedisStreamsAdapter.Publish(
  const ARequest: THefestoPublishRequest);
var
  LPipeline: IHefestoRedisPipeline;
begin
  LPipeline := GetClient.BeginPipeline;
  LPipeline.QueueCommand(BuildXAddCommand(ARequest));
  LPipeline.Execute;
end;

procedure THefestoRedisStreamsAdapter.PublishBatch(
  const ARequests: TArray<THefestoPublishRequest>);
var
  LPipeline: IHefestoRedisPipeline;
  LRequest: THefestoPublishRequest;
begin
  if Length(ARequests) = 0 then
    Exit;
  LPipeline := GetClient.BeginPipeline;
  for LRequest in ARequests do
    LPipeline.QueueCommand(BuildXAddCommand(LRequest));
  LPipeline.Execute;
end;

function THefestoRedisStreamsAdapter.ParseActionFromBody(
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
