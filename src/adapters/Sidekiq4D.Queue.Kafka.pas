unit Sidekiq4D.Queue.Kafka;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  System.JSON,
  System.NetEncoding,
  System.Net.HttpClient,
  System.Net.URLClient,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces;

type
  TSidekiqKafkaAdapter = class(TInterfacedObject, ISidekiqQueueAdapter, ISidekiqQueuePublisher)
  private
    // Configuration fields: set once before first use. Thread-safe after init.
    FBaseUrl: string;
    FGroupId: string;
    FTopic: string;
    FInstanceId: string;
    FDeadLetterTopic: string;
    FConsumerCreated: Integer;
    FLock: TCriticalSection;

    function ConsumerBaseUrl: string;
    function CreateHttpClient: THTTPClient;
    function DoGet(const AUrl: string): string;
    function DoPost(const AUrl, ABody, AContentType: string): string;
    procedure DoDelete(const AUrl: string);
    procedure EnsureConsumerExists;
    procedure EnsureSubscription;
    function ParseActionFromBody(const ABody: string): string;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqKafkaAdapter; static;

    function BaseUrl(const AValue: string): TSidekiqKafkaAdapter;
    function GroupId(const AValue: string): TSidekiqKafkaAdapter;
    function Topic(const AValue: string): TSidekiqKafkaAdapter;
    function InstanceId(const AValue: string): TSidekiqKafkaAdapter;
    function DeadLetterTopic(const AValue: string): TSidekiqKafkaAdapter;

    { ISidekiqQueueAdapter }
    function Name: string;
    function Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);

    { ISidekiqQueuePublisher }
    procedure Publish(const ARequest: TSidekiqPublishRequest);
  end;

implementation

{ TSidekiqKafkaAdapter }

constructor TSidekiqKafkaAdapter.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FBaseUrl := 'http://localhost:8082';
  FGroupId := 'sidekiq4d';
  FTopic := 'jobs';
  FInstanceId := 'sidekiq4d-instance';
  FDeadLetterTopic := '';
  FConsumerCreated := False;
end;

destructor TSidekiqKafkaAdapter.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function TSidekiqKafkaAdapter.New: TSidekiqKafkaAdapter;
begin
  Result := TSidekiqKafkaAdapter.Create;
end;

function TSidekiqKafkaAdapter.BaseUrl(const AValue: string): TSidekiqKafkaAdapter;
begin
  Result := Self;
  FBaseUrl := AValue.TrimRight(['/']);
end;

function TSidekiqKafkaAdapter.GroupId(const AValue: string): TSidekiqKafkaAdapter;
begin
  Result := Self;
  FGroupId := AValue.Trim;
end;

function TSidekiqKafkaAdapter.Topic(const AValue: string): TSidekiqKafkaAdapter;
begin
  Result := Self;
  FTopic := AValue.Trim;
end;

function TSidekiqKafkaAdapter.InstanceId(const AValue: string): TSidekiqKafkaAdapter;
begin
  Result := Self;
  FInstanceId := AValue.Trim;
end;

function TSidekiqKafkaAdapter.DeadLetterTopic(const AValue: string): TSidekiqKafkaAdapter;
begin
  Result := Self;
  FDeadLetterTopic := AValue.Trim;
end;

function TSidekiqKafkaAdapter.ConsumerBaseUrl: string;
begin
  Result := Format('%s/consumers/%s/instances/%s', [FBaseUrl, FGroupId, FInstanceId]);
end;

function TSidekiqKafkaAdapter.CreateHttpClient: THTTPClient;
begin
  Result := THTTPClient.Create;
end;

function TSidekiqKafkaAdapter.DoGet(const AUrl: string): string;
var
  LClient: THTTPClient;
  LResponse: IHTTPResponse;
begin
  LClient := CreateHttpClient;
  try
    LClient.CustomHeaders['Accept'] := 'application/vnd.kafka.json.v2+json';
    LResponse := LClient.Get(AUrl);
    Result := LResponse.ContentAsString;
  finally
    LClient.Free;
  end;
end;

function TSidekiqKafkaAdapter.DoPost(const AUrl, ABody, AContentType: string): string;
var
  LClient: THTTPClient;
  LResponse: IHTTPResponse;
  LContent: TStringStream;
begin
  LClient := CreateHttpClient;
  try
    LContent := TStringStream.Create(ABody, TEncoding.UTF8);
    try
      LResponse := LClient.Post(AUrl, LContent, nil,
        [TNameValuePair.Create('Content-Type', AContentType)]);
      Result := LResponse.ContentAsString;
    finally
      LContent.Free;
    end;
  finally
    LClient.Free;
  end;
end;

procedure TSidekiqKafkaAdapter.DoDelete(const AUrl: string);
var
  LClient: THTTPClient;
begin
  LClient := CreateHttpClient;
  try
    LClient.CustomHeaders['Content-Type'] := 'application/vnd.kafka.v2+json';
    LClient.Delete(AUrl);
  finally
    LClient.Free;
  end;
end;

procedure TSidekiqKafkaAdapter.EnsureConsumerExists;
var
  LUrl: string;
  LBody: string;
begin
  if TInterlocked.CompareExchange(FConsumerCreated, 0, 0) <> 0 then
    Exit; // fast path — atomic read, no race on ARM/non-x86

  FLock.Acquire;
  try
    if TInterlocked.CompareExchange(FConsumerCreated, 0, 0) <> 0 then
      Exit; // another thread beat us here

    LUrl := Format('%s/consumers/%s', [FBaseUrl, FGroupId]);
    LBody := Format('{"name":"%s","format":"json","auto.offset.reset":"earliest","auto.commit.enable":"false"}',
      [FInstanceId]);

    DoPost(LUrl, LBody, 'application/vnd.kafka.v2+json');
    TInterlocked.Exchange(FConsumerCreated, 1);
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqKafkaAdapter.EnsureSubscription;
var
  LUrl: string;
  LBody: string;
begin
  LUrl := ConsumerBaseUrl + '/subscription';
  LBody := Format('{"topics":["%s"]}', [FTopic]);
  DoPost(LUrl, LBody, 'application/vnd.kafka.v2+json');
end;

function TSidekiqKafkaAdapter.Name: string;
begin
  Result := 'kafka';
end;

function TSidekiqKafkaAdapter.Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
var
  LUrl: string;
  LResponse: string;
  LJsonArray: TJSONArray;
  LJsonObj: TJSONObject;
  LEnvelope: TSidekiqJobEnvelope;
  LBody: string;
  LAction: string;
  LKey: string;
  LPartition: Integer;
  LOffset: Int64;
  LCount: Integer;
  LJsonValue: TJSONValue;
  LValueObj: TJSONObject;
  LValueStr: string;
begin
  SetLength(Result, 0);

  EnsureConsumerExists;
  EnsureSubscription;

  LUrl := ConsumerBaseUrl + '/records';
  LResponse := DoGet(LUrl);

  if LResponse.Trim.IsEmpty then
    Exit;

  LJsonValue := TJSONObject.ParseJSONValue(LResponse);
  try
    if not (LJsonValue is TJSONArray) then
      Exit;

    LJsonArray := TJSONArray(LJsonValue);
    if LJsonArray.Count = 0 then
      Exit;

    LCount := 0;
    SetLength(Result, LJsonArray.Count);

    for var I := 0 to LJsonArray.Count - 1 do
    begin
      LJsonObj := LJsonArray.Items[I] as TJSONObject;
      LPartition := LJsonObj.GetValue<Integer>('partition', 0);
      LOffset := LJsonObj.GetValue<Int64>('offset', 0);
      LKey := LJsonObj.GetValue<string>('key', '');

      { Value can be a JSON object or a string }
      if LJsonObj.GetValue('value') is TJSONObject then
      begin
        LValueObj := LJsonObj.GetValue<TJSONObject>('value');
        LBody := LValueObj.ToJSON;
      end
      else
        LBody := LJsonObj.GetValue<string>('value', '');

      LAction := ParseActionFromBody(LBody);

      LEnvelope := TSidekiqJobEnvelope.Create(
        Format('%s:%d:%d', [FTopic, LPartition, LOffset]),
        FTopic,
        Format('%d:%d', [LPartition, LOffset]),
        LBody,
        LAction,
        0);

      if not LKey.IsEmpty then
        LEnvelope.AddAttribute('kafka.key', LKey);
      LEnvelope.AddAttribute('kafka.partition', IntToStr(LPartition));
      LEnvelope.AddAttribute('kafka.offset', IntToStr(LOffset));

      Result[LCount] := LEnvelope;
      Inc(LCount);
    end;

    SetLength(Result, LCount);
  finally
    LJsonValue.Free;
  end;
end;

procedure TSidekiqKafkaAdapter.Ack(const AJob: ISidekiqJobEnvelope);
var
  LUrl: string;
  LBody: string;
  LParts: TArray<string>;
  LPartition: Integer;
  LOffset: Int64;
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LParts := AJob.ReceiptHandle.Split([':']);
  if Length(LParts) < 2 then
    Exit;

  LPartition := StrToIntDef(LParts[0], 0);
  LOffset := StrToInt64Def(LParts[1], 0);

  LUrl := ConsumerBaseUrl + '/offsets';
  LBody := Format('{"offsets":[{"topic":"%s","partition":%d,"offset":%d}]}',
    [FTopic, LPartition, LOffset + 1]);

  DoPost(LUrl, LBody, 'application/vnd.kafka.v2+json');
end;

procedure TSidekiqKafkaAdapter.Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
begin
  { Kafka REST Proxy does not support delayed redelivery.
    Not committing the offset means the message will be redelivered
    on the next fetch after consumer group rebalance or restart. }
end;

procedure TSidekiqKafkaAdapter.MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);
var
  LRequest: TSidekiqPublishRequest;
  LTargetTopic: string;
begin
  LTargetTopic := FDeadLetterTopic;
  if LTargetTopic.IsEmpty then
    LTargetTopic := FTopic + '.dead-letter';

  LRequest.QueueName := LTargetTopic;
  LRequest.Action := AJob.Action;
  LRequest.Body := AJob.Body;
  LRequest.DelaySeconds := 0;
  SetLength(LRequest.Attributes, 2);
  LRequest.Attributes[0] := TPair<string, string>.Create('x-sidekiq-reason', AReason);
  LRequest.Attributes[1] := TPair<string, string>.Create('x-sidekiq-source-topic', FTopic);

  Publish(LRequest);
  Ack(AJob);
end;

procedure TSidekiqKafkaAdapter.Publish(const ARequest: TSidekiqPublishRequest);
var
  LUrl: string;
  LTargetTopic: string;
  LJson: TJSONObject;
  LRecords: TJSONArray;
  LRecord: TJSONObject;
  LValue: TJSONObject;
  I: Integer;
begin
  LTargetTopic := ARequest.QueueName;
  if LTargetTopic.IsEmpty then
    LTargetTopic := FTopic;

  LUrl := Format('%s/topics/%s', [FBaseUrl, LTargetTopic]);

  LJson := TJSONObject.Create;
  try
    LRecords := TJSONArray.Create;
    LRecord := TJSONObject.Create;

    LValue := TJSONObject.Create;
    LValue.AddPair('body', ARequest.Body);
    if not ARequest.Action.IsEmpty then
      LValue.AddPair('action', ARequest.Action);

    for I := 0 to High(ARequest.Attributes) do
      LValue.AddPair(ARequest.Attributes[I].Key, ARequest.Attributes[I].Value);

    LRecord.AddPair('value', LValue);
    LRecords.AddElement(LRecord);
    LJson.AddPair('records', LRecords);

    DoPost(LUrl, LJson.ToJSON, 'application/vnd.kafka.json.v2+json');
  finally
    LJson.Free;
  end;
end;

function TSidekiqKafkaAdapter.ParseActionFromBody(const ABody: string): string;
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
