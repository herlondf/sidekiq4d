unit Hefesto.Queue.RabbitMQ;

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
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  THefestoRabbitMQAdapter = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueuePublisher)
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

    class function New: THefestoRabbitMQAdapter; static;

    function Host(const AValue: string): THefestoRabbitMQAdapter;
    function Port(const AValue: Integer): THefestoRabbitMQAdapter;
    function VHost(const AValue: string): THefestoRabbitMQAdapter;
    function Queue(const AValue: string): THefestoRabbitMQAdapter;
    function Exchange(const AValue: string): THefestoRabbitMQAdapter;
    function Username(const AValue: string): THefestoRabbitMQAdapter;
    function Password(const AValue: string): THefestoRabbitMQAdapter;
    function DeadLetterQueue(const AValue: string): THefestoRabbitMQAdapter;

    { IHefestoQueueAdapter }
    function Name: string;
    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);

    { IHefestoQueuePublisher }
    procedure Publish(const ARequest: THefestoPublishRequest);
  end;

implementation

uses
  System.Threading;

const
  MaxRetries   = 3;
  RetryDelayMs: array[1..MaxRetries] of Integer = (100, 500, 1000);

{ THefestoRabbitMQAdapter }

constructor THefestoRabbitMQAdapter.Create;
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

destructor THefestoRabbitMQAdapter.Destroy;
begin
  FLock.Free;
  FHttpClient.Free;
  inherited;
end;

class function THefestoRabbitMQAdapter.New: THefestoRabbitMQAdapter;
begin
  Result := THefestoRabbitMQAdapter.Create;
end;

function THefestoRabbitMQAdapter.Host(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FHost := AValue.Trim;
end;

function THefestoRabbitMQAdapter.Port(const AValue: Integer): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

function THefestoRabbitMQAdapter.VHost(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FVHost := AValue.Trim;
end;

function THefestoRabbitMQAdapter.Queue(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FQueue := AValue.Trim;
end;

function THefestoRabbitMQAdapter.Exchange(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FExchange := AValue.Trim;
end;

function THefestoRabbitMQAdapter.Username(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FUsername := AValue.Trim;
  ApplyAuth(FHttpClient);
end;

function THefestoRabbitMQAdapter.Password(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FPassword := AValue.Trim;
  ApplyAuth(FHttpClient);
end;

function THefestoRabbitMQAdapter.DeadLetterQueue(const AValue: string): THefestoRabbitMQAdapter;
begin
  Result := Self;
  FDeadLetterQueue := AValue.Trim;
end;

function THefestoRabbitMQAdapter.BaseUrl: string;
begin
  Result := Format('http://%s:%d/api', [FHost, FPort]);
end;

function THefestoRabbitMQAdapter.EncodedVHost: string;
begin
  Result := TNetEncoding.URL.Encode(FVHost);
end;

procedure THefestoRabbitMQAdapter.ApplyAuth(const AClient: THTTPClient);
var
  LCredentials: string;
begin
  LCredentials := TNetEncoding.Base64.Encode(FUsername + ':' + FPassword);
  AClient.CustomHeaders['Authorization'] := 'Basic ' + LCredentials;
end;

function THefestoRabbitMQAdapter.IsTransientError(const E: Exception): Boolean;
begin
  // Network errors (timeout, connection reset) are retryable
  Result := (E is ENetHTTPClientException) or (E is ENetHTTPException);
end;

function THefestoRabbitMQAdapter.DoGet(const APath: string): string;
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

function THefestoRabbitMQAdapter.DoPost(const APath, ABody: string): string;
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

procedure THefestoRabbitMQAdapter.DoDelete(const APath: string);
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

function THefestoRabbitMQAdapter.Name: string;
begin
  Result := 'rabbitmq';
end;

function THefestoRabbitMQAdapter.Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
var
  LPath: string;
  LBody: string;
  LResponse: string;
  LJsonArray: TJSONArray;
  LJsonValue: TJSONValue;
  LJsonObj: TJSONObject;
  LEnvelope: THefestoJobEnvelope;
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

      LEnvelope := THefestoJobEnvelope.Create(
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

procedure THefestoRabbitMQAdapter.Ack(const AJob: IHefestoJobEnvelope);
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

procedure THefestoRabbitMQAdapter.Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
begin
  { RabbitMQ HTTP API does not support delayed requeue natively.
    The message was fetched with ack_requeue_true so it remains in the queue. }
end;

procedure THefestoRabbitMQAdapter.MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
var
  LRequest: THefestoPublishRequest;
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

procedure THefestoRabbitMQAdapter.Publish(const ARequest: THefestoPublishRequest);
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

function THefestoRabbitMQAdapter.ParseActionFromBody(const ABody: string): string;
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
