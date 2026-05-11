unit Sidekiq4D.Queue.RabbitMQ;

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
  TSidekiqRabbitMQAdapter = class(TInterfacedObject, ISidekiqQueueAdapter, ISidekiqQueuePublisher)
  private
    FHost: string;
    FPort: Integer;
    FVHost: string;
    FQueue: string;
    FExchange: string;
    FUsername: string;
    FPassword: string;
    FDeadLetterQueue: string;
    // Persistent HTTP client — reuses TCP connections (keep-alive).
    // FLock serialises access since THTTPClient is not thread-safe and
    // Ack/Nack may be called concurrently from worker threads.
    FHttpClient: THTTPClient;
    FLock: TCriticalSection;

    function BaseUrl: string;
    function EncodedVHost: string;
    procedure ApplyAuth(const AClient: THTTPClient);
    // Executes the HTTP call with up to AMaxAttempts retries on transient errors.
    function DoGet(const APath: string): string;
    function DoPost(const APath, ABody: string): string;
    procedure DoDelete(const APath: string);
    function ParseActionFromBody(const ABody: string): string;
    function IsTransientError(const E: Exception): Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqRabbitMQAdapter; static;

    function Host(const AValue: string): TSidekiqRabbitMQAdapter;
    function Port(const AValue: Integer): TSidekiqRabbitMQAdapter;
    function VHost(const AValue: string): TSidekiqRabbitMQAdapter;
    function Queue(const AValue: string): TSidekiqRabbitMQAdapter;
    function Exchange(const AValue: string): TSidekiqRabbitMQAdapter;
    function Username(const AValue: string): TSidekiqRabbitMQAdapter;
    function Password(const AValue: string): TSidekiqRabbitMQAdapter;
    function DeadLetterQueue(const AValue: string): TSidekiqRabbitMQAdapter;

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

uses
  System.Threading;

const
  MaxRetries   = 3;
  RetryDelayMs: array[1..MaxRetries] of Integer = (100, 500, 1000);

{ TSidekiqRabbitMQAdapter }

constructor TSidekiqRabbitMQAdapter.Create;
begin
  inherited Create;
  FHost := 'localhost';
  FPort := 15672;
  FVHost := '/';
  FQueue := 'jobs';
  FExchange := '';
  FUsername := 'guest';
  FPassword := 'guest';
  FDeadLetterQueue := '';
  FHttpClient := THTTPClient.Create;
  FHttpClient.ContentType := 'application/json';
  ApplyAuth(FHttpClient);
  FLock := TCriticalSection.Create;
end;

destructor TSidekiqRabbitMQAdapter.Destroy;
begin
  FLock.Free;
  FHttpClient.Free;
  inherited;
end;

class function TSidekiqRabbitMQAdapter.New: TSidekiqRabbitMQAdapter;
begin
  Result := TSidekiqRabbitMQAdapter.Create;
end;

function TSidekiqRabbitMQAdapter.Host(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FHost := AValue.Trim;
end;

function TSidekiqRabbitMQAdapter.Port(const AValue: Integer): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

function TSidekiqRabbitMQAdapter.VHost(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FVHost := AValue.Trim;
end;

function TSidekiqRabbitMQAdapter.Queue(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FQueue := AValue.Trim;
end;

function TSidekiqRabbitMQAdapter.Exchange(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FExchange := AValue.Trim;
end;

function TSidekiqRabbitMQAdapter.Username(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FUsername := AValue.Trim;
  ApplyAuth(FHttpClient);
end;

function TSidekiqRabbitMQAdapter.Password(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FPassword := AValue.Trim;
  ApplyAuth(FHttpClient);
end;

function TSidekiqRabbitMQAdapter.DeadLetterQueue(const AValue: string): TSidekiqRabbitMQAdapter;
begin
  Result := Self;
  FDeadLetterQueue := AValue.Trim;
end;

function TSidekiqRabbitMQAdapter.BaseUrl: string;
begin
  Result := Format('http://%s:%d/api', [FHost, FPort]);
end;

function TSidekiqRabbitMQAdapter.EncodedVHost: string;
begin
  Result := TNetEncoding.URL.Encode(FVHost);
end;

procedure TSidekiqRabbitMQAdapter.ApplyAuth(const AClient: THTTPClient);
var
  LCredentials: string;
begin
  LCredentials := TNetEncoding.Base64.Encode(FUsername + ':' + FPassword);
  AClient.CustomHeaders['Authorization'] := 'Basic ' + LCredentials;
end;

function TSidekiqRabbitMQAdapter.IsTransientError(const E: Exception): Boolean;
begin
  // Network errors (timeout, connection reset) are retryable
  Result := (E is ENetHTTPClientException) or (E is ENetHTTPException);
end;

function TSidekiqRabbitMQAdapter.DoGet(const APath: string): string;
var
  LAttempt: Integer;
  LResponse: IHTTPResponse;
begin
  for LAttempt := 1 to MaxRetries do
  begin
    try
      FLock.Acquire;
      try
        LResponse := FHttpClient.Get(BaseUrl + APath);
      finally
        FLock.Release;
      end;
      Result := LResponse.ContentAsString;
      Exit;
    except
      on E: Exception do
      begin
        if not IsTransientError(E) or (LAttempt = MaxRetries) then
          raise;
        TThread.Sleep(RetryDelayMs[LAttempt]);
      end;
    end;
  end;
end;

function TSidekiqRabbitMQAdapter.DoPost(const APath, ABody: string): string;
var
  LAttempt: Integer;
  LResponse: IHTTPResponse;
  LContent: TStringStream;
begin
  for LAttempt := 1 to MaxRetries do
  begin
    LContent := TStringStream.Create(ABody, TEncoding.UTF8);
    try
      try
        FLock.Acquire;
        try
          LResponse := FHttpClient.Post(BaseUrl + APath, LContent, nil,
            [TNameValuePair.Create('Content-Type', 'application/json')]);
        finally
          FLock.Release;
        end;
        Result := LResponse.ContentAsString;
        Exit;
      except
        on E: Exception do
        begin
          if not IsTransientError(E) or (LAttempt = MaxRetries) then
            raise;
          TThread.Sleep(RetryDelayMs[LAttempt]);
        end;
      end;
    finally
      LContent.Free;
    end;
  end;
end;

procedure TSidekiqRabbitMQAdapter.DoDelete(const APath: string);
var
  LAttempt: Integer;
begin
  for LAttempt := 1 to MaxRetries do
  begin
    try
      FLock.Acquire;
      try
        FHttpClient.Delete(BaseUrl + APath);
      finally
        FLock.Release;
      end;
      Exit;
    except
      on E: Exception do
      begin
        if not IsTransientError(E) or (LAttempt = MaxRetries) then
          raise;
        TThread.Sleep(RetryDelayMs[LAttempt]);
      end;
    end;
  end;
end;

function TSidekiqRabbitMQAdapter.Name: string;
begin
  Result := 'rabbitmq';
end;

function TSidekiqRabbitMQAdapter.Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
var
  LPath: string;
  LBody: string;
  LResponse: string;
  LJsonArray: TJSONArray;
  LJsonValue: TJSONValue;
  LJsonObj: TJSONObject;
  LEnvelope: TSidekiqJobEnvelope;
  LPayload: string;
  LAction: string;
  LMessageId: string;
  LDeliveryTag: string;
  LCount: Integer;
  LBatchSize: Integer;
begin
  SetLength(Result, 0);
  LBatchSize := AOptions.BatchSize;
  if LBatchSize <= 0 then
    LBatchSize := 1;

  LPath := Format('/queues/%s/%s/get', [EncodedVHost, TNetEncoding.URL.Encode(FQueue)]);
  LBody := Format('{"count":%d,"ackmode":"ack_requeue_true","encoding":"auto"}', [LBatchSize]);

  LResponse := DoPost(LPath, LBody);
  if LResponse.Trim.IsEmpty then
    Exit;

  LJsonValue := TJSONObject.ParseJSONValue(LResponse);
  try
    if not (LJsonValue is TJSONArray) then
      Exit;

    LJsonArray := TJSONArray(LJsonValue);
    LCount := 0;
    SetLength(Result, LJsonArray.Count);

    for var I := 0 to LJsonArray.Count - 1 do
    begin
      LJsonObj := LJsonArray.Items[I] as TJSONObject;
      LPayload := LJsonObj.GetValue<string>('payload', '');
      LDeliveryTag := LJsonObj.GetValue<string>('delivery_tag', '');
      LMessageId := LJsonObj.GetValue<string>('message_id', IntToStr(I));

      if LMessageId.IsEmpty then
        LMessageId := TGUID.NewGuid.ToString;

      LAction := ParseActionFromBody(LPayload);

      LEnvelope := TSidekiqJobEnvelope.Create(
        LMessageId,
        FQueue,
        LDeliveryTag,
        LPayload,
        LAction,
        LJsonObj.GetValue<Integer>('redelivered_count', 0));

      Result[LCount] := LEnvelope;
      Inc(LCount);
    end;

    SetLength(Result, LCount);
  finally
    LJsonValue.Free;
  end;
end;

procedure TSidekiqRabbitMQAdapter.Ack(const AJob: ISidekiqJobEnvelope);
var
  LPath: string;
  LBody: string;
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LPath := Format('/queues/%s/%s/get', [EncodedVHost, TNetEncoding.URL.Encode(FQueue)]);
  LBody := Format('{"count":1,"ackmode":"ack_requeue_false","encoding":"auto","delivery_tag":"%s"}',
    [AJob.ReceiptHandle]);
  DoPost(LPath, LBody);
end;

procedure TSidekiqRabbitMQAdapter.Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
begin
  { RabbitMQ HTTP API does not support delayed requeue natively.
    The message was fetched with ack_requeue_true so it remains in the queue. }
end;

procedure TSidekiqRabbitMQAdapter.MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);
var
  LRequest: TSidekiqPublishRequest;
begin
  if FDeadLetterQueue.IsEmpty then
  begin
    Ack(AJob);
    Exit;
  end;

  LRequest.QueueName := FDeadLetterQueue;
  LRequest.Action := AJob.Action;
  LRequest.Body := AJob.Body;
  LRequest.DelaySeconds := 0;
  SetLength(LRequest.Attributes, 2);
  LRequest.Attributes[0] := TPair<string, string>.Create('x-sidekiq-reason', AReason);
  LRequest.Attributes[1] := TPair<string, string>.Create('x-sidekiq-source-queue', FQueue);

  Publish(LRequest);
  Ack(AJob);
end;

procedure TSidekiqRabbitMQAdapter.Publish(const ARequest: TSidekiqPublishRequest);
var
  LPath: string;
  LRoutingKey: string;
  LExchange: string;
  LPayload: string;
  LJson: TJSONObject;
  LProps: TJSONObject;
  LHeaders: TJSONObject;
  I: Integer;
begin
  LExchange := FExchange;
  LRoutingKey := ARequest.QueueName;
  if LRoutingKey.IsEmpty then
    LRoutingKey := FQueue;

  LPath := Format('/exchanges/%s/%s/publish',
    [EncodedVHost, TNetEncoding.URL.Encode(LExchange)]);

  LJson := TJSONObject.Create;
  try
    LProps := TJSONObject.Create;
    LHeaders := TJSONObject.Create;

    if not ARequest.Action.IsEmpty then
      LHeaders.AddPair('action', ARequest.Action);

    for I := 0 to High(ARequest.Attributes) do
      LHeaders.AddPair(ARequest.Attributes[I].Key, ARequest.Attributes[I].Value);

    LProps.AddPair('headers', LHeaders);
    LJson.AddPair('properties', LProps);
    LJson.AddPair('routing_key', LRoutingKey);
    LJson.AddPair('payload', ARequest.Body);
    LJson.AddPair('payload_encoding', 'string');

    LPayload := LJson.ToJSON;
  finally
    LJson.Free;
  end;

  DoPost(LPath, LPayload);
end;

function TSidekiqRabbitMQAdapter.ParseActionFromBody(const ABody: string): string;
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
