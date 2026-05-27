unit Hefesto.Queue.AzureServiceBus;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  System.JSON,
  System.NetEncoding,
  System.Net.HttpClient,
  System.Net.URLClient,
  System.DateUtils,
  System.Hash,
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  THefestoAzureServiceBusAdapter = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueuePublisher)
  private
    // Configuration fields: set once before first use. Thread-safe after init.
    FNamespace: string;
    FQueue: string;
    FSharedAccessKey: string;
    FSharedAccessKeyName: string;
    FDeadLetterQueue: string;

    function BaseUrl: string;
    function GenerateSasToken: string;
    function CreateHttpClient: THTTPClient;
    function ParseActionFromBody(const ABody: string): string;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoAzureServiceBusAdapter; static;

    function Namespace(const AValue: string): THefestoAzureServiceBusAdapter;
    function Queue(const AValue: string): THefestoAzureServiceBusAdapter;
    function SharedAccessKey(const AValue: string): THefestoAzureServiceBusAdapter;
    function SharedAccessKeyName(const AValue: string): THefestoAzureServiceBusAdapter;
    function DeadLetterQueue(const AValue: string): THefestoAzureServiceBusAdapter;

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

{ THefestoAzureServiceBusAdapter }

constructor THefestoAzureServiceBusAdapter.Create;
begin
  inherited Create;
  FNamespace := '';
  FQueue := 'jobs';
  FSharedAccessKey := '';
  FSharedAccessKeyName := 'RootManageSharedAccessKey';
  FDeadLetterQueue := '';
end;

destructor THefestoAzureServiceBusAdapter.Destroy;
begin
  inherited;
end;

class function THefestoAzureServiceBusAdapter.New: THefestoAzureServiceBusAdapter;
begin
  Result := THefestoAzureServiceBusAdapter.Create;
end;

function THefestoAzureServiceBusAdapter.Namespace(const AValue: string): THefestoAzureServiceBusAdapter;
begin
  Result := Self;
  FNamespace := AValue.Trim;
end;

function THefestoAzureServiceBusAdapter.Queue(const AValue: string): THefestoAzureServiceBusAdapter;
begin
  Result := Self;
  FQueue := AValue.Trim;
end;

function THefestoAzureServiceBusAdapter.SharedAccessKey(const AValue: string): THefestoAzureServiceBusAdapter;
begin
  Result := Self;
  FSharedAccessKey := AValue.Trim;
end;

function THefestoAzureServiceBusAdapter.SharedAccessKeyName(const AValue: string): THefestoAzureServiceBusAdapter;
begin
  Result := Self;
  FSharedAccessKeyName := AValue.Trim;
end;

function THefestoAzureServiceBusAdapter.DeadLetterQueue(const AValue: string): THefestoAzureServiceBusAdapter;
begin
  Result := Self;
  FDeadLetterQueue := AValue.Trim;
end;

function THefestoAzureServiceBusAdapter.BaseUrl: string;
begin
  Result := Format('https://%s.servicebus.windows.net/%s', [FNamespace, FQueue]);
end;

function THefestoAzureServiceBusAdapter.GenerateSasToken: string;
var
  LResourceUri: string;
  LExpiry: Int64;
  LStringToSign: string;
  LSignature: string;
  LKeyBytes: TBytes;
begin
  LResourceUri := TNetEncoding.URL.Encode(
    Format('https://%s.servicebus.windows.net/%s', [FNamespace, FQueue]));

  LExpiry := DateTimeToUnix(IncHour(TTimeZone.Local.ToUniversalTime(Now), 1), False);
  LStringToSign := LResourceUri + #10 + IntToStr(LExpiry);

  LKeyBytes := TEncoding.UTF8.GetBytes(FSharedAccessKey);
  LSignature := TNetEncoding.URL.Encode(
    TNetEncoding.Base64.EncodeBytesToString(
      THashSHA2.GetHMACAsBytes(LStringToSign, LKeyBytes, SHA256)));

  Result := Format('SharedAccessSignature sr=%s&sig=%s&se=%d&skn=%s',
    [LResourceUri, LSignature, LExpiry, FSharedAccessKeyName]);
end;

function THefestoAzureServiceBusAdapter.CreateHttpClient: THTTPClient;
begin
  Result := THTTPClient.Create;
  Result.ContentType := 'application/json';
  Result.CustomHeaders['Authorization'] := GenerateSasToken;
end;

function THefestoAzureServiceBusAdapter.Name: string;
begin
  Result := 'azure-servicebus';
end;

function THefestoAzureServiceBusAdapter.Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
var
  LClient: THTTPClient;
  LResponse: IHTTPResponse;
  LUrl: string;
  LBody: string;
  LMessageId: string;
  LLockToken: string;
  LAction: string;
  LEnvelope: THefestoJobEnvelope;
  LBrokerProps: TJSONObject;
  LBrokerPropsStr: string;
  LDeliveryCount: Integer;
  I: Integer;
  LBatchSize: Integer;
begin
  SetLength(Result, 0);
  LBatchSize := AOptions.BatchSize;
  if LBatchSize <= 0 then
    LBatchSize := 1;

  LClient := CreateHttpClient;
  try
    for I := 0 to LBatchSize - 1 do
    begin
      LUrl := BaseUrl + '/messages/head';
      if AOptions.WaitTimeSeconds > 0 then
        LUrl := LUrl + '?timeout=' + IntToStr(AOptions.WaitTimeSeconds);

      LResponse := LClient.Post(LUrl, nil, nil);

      if LResponse.StatusCode <> 201 then
        Break;

      LBody := LResponse.ContentAsString;
      LMessageId := '';
      LLockToken := '';
      LDeliveryCount := 0;

      LBrokerPropsStr := LResponse.HeaderValue['BrokerProperties'];
      if not LBrokerPropsStr.IsEmpty then
      begin
        LBrokerProps := TJSONObject(TJSONObject.ParseJSONValue(LBrokerPropsStr));
        try
          if Assigned(LBrokerProps) then
          begin
            LMessageId := LBrokerProps.GetValue<string>('MessageId', '');
            LLockToken := LBrokerProps.GetValue<string>('LockToken', '');
            LDeliveryCount := LBrokerProps.GetValue<Integer>('DeliveryCount', 0);
          end;
        finally
          LBrokerProps.Free;
        end;
      end;

      if LMessageId.IsEmpty then
        LMessageId := TGUID.NewGuid.ToString;

      LAction := ParseActionFromBody(LBody);

      LEnvelope := THefestoJobEnvelope.Create(
        LMessageId,
        FQueue,
        LMessageId + '/' + LLockToken,
        LBody,
        LAction,
        LDeliveryCount);

      SetLength(Result, Length(Result) + 1);
      Result[High(Result)] := LEnvelope;
    end;
  finally
    LClient.Free;
  end;
end;

procedure THefestoAzureServiceBusAdapter.Ack(const AJob: IHefestoJobEnvelope);
var
  LClient: THTTPClient;
  LParts: TArray<string>;
  LMessageId: string;
  LLockToken: string;
  LUrl: string;
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LParts := AJob.ReceiptHandle.Split(['/']);
  if Length(LParts) < 2 then
    Exit;

  LMessageId := LParts[0];
  LLockToken := LParts[1];

  LUrl := Format('%s/messages/%s/%s', [BaseUrl, LMessageId, LLockToken]);

  LClient := CreateHttpClient;
  try
    LClient.Delete(LUrl);
  finally
    LClient.Free;
  end;
end;

procedure THefestoAzureServiceBusAdapter.Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
var
  LClient: THTTPClient;
  LParts: TArray<string>;
  LMessageId: string;
  LLockToken: string;
  LUrl: string;
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LParts := AJob.ReceiptHandle.Split(['/']);
  if Length(LParts) < 2 then
    Exit;

  LMessageId := LParts[0];
  LLockToken := LParts[1];

  LUrl := Format('%s/messages/%s/%s', [BaseUrl, LMessageId, LLockToken]);

  LClient := CreateHttpClient;
  try
    LClient.Put(LUrl, nil, nil);
  finally
    LClient.Free;
  end;
end;

procedure THefestoAzureServiceBusAdapter.MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
var
  LRequest: THefestoPublishRequest;
  LTargetQueue: string;
begin
  LTargetQueue := FDeadLetterQueue;
  if LTargetQueue.IsEmpty then
    LTargetQueue := FQueue + '/$deadletterqueue';

  LRequest.QueueName := LTargetQueue;
  LRequest.Action := AJob.Action;
  LRequest.Body := AJob.Body;
  LRequest.DelaySeconds := 0;
  SetLength(LRequest.Attributes, 2);
  LRequest.Attributes[0] := TPair<string, string>.Create('x-sidekiq-reason', AReason);
  LRequest.Attributes[1] := TPair<string, string>.Create('x-sidekiq-source-queue', FQueue);

  Publish(LRequest);
  Ack(AJob);
end;

procedure THefestoAzureServiceBusAdapter.Publish(const ARequest: THefestoPublishRequest);
var
  LClient: THTTPClient;
  LContent: TStringStream;
  LUrl: string;
  LTargetQueue: string;
  I: Integer;
begin
  LTargetQueue := ARequest.QueueName;
  if LTargetQueue.IsEmpty then
    LTargetQueue := FQueue;

  LUrl := Format('https://%s.servicebus.windows.net/%s/messages',
    [FNamespace, LTargetQueue]);

  LClient := CreateHttpClient;
  try
    if not ARequest.Action.IsEmpty then
      LClient.CustomHeaders['action'] := ARequest.Action;

    for I := 0 to High(ARequest.Attributes) do
      LClient.CustomHeaders[ARequest.Attributes[I].Key] := ARequest.Attributes[I].Value;

    if ARequest.DelaySeconds > 0 then
      LClient.CustomHeaders['x-ms-scheduled-enqueue-time'] :=
        DateToISO8601(IncSecond(TTimeZone.Local.ToUniversalTime(Now), ARequest.DelaySeconds), False);

    LContent := TStringStream.Create(ARequest.Body, TEncoding.UTF8);
    try
      LClient.Post(LUrl, LContent, nil,
        [TNameValuePair.Create('Content-Type', 'application/json')]);
    finally
      LContent.Free;
    end;
  finally
    LClient.Free;
  end;
end;

function THefestoAzureServiceBusAdapter.ParseActionFromBody(const ABody: string): string;
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
