unit Hefesto.Queue.GooglePubSub;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  System.JSON,
  System.NetEncoding,
  System.Net.HttpClient,
  System.Net.URLClient,
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  THefestoGooglePubSubAdapter = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueuePublisher)
  private
    // Configuration fields: set once before first use. Thread-safe after init.
    FProject: string;
    FSubscription: string;
    FTopic: string;
    FBearerToken: string;
    FDeadLetterTopic: string;

    function SubscriptionUrl: string;
    function TopicUrl(const ATopic: string): string;
    function CreateHttpClient: THTTPClient;
    function DoPost(const AUrl, ABody: string): string;
    function ParseActionFromBody(const ABody: string): string;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoGooglePubSubAdapter; static;

    function Project(const AValue: string): THefestoGooglePubSubAdapter;
    function Subscription(const AValue: string): THefestoGooglePubSubAdapter;
    function Topic(const AValue: string): THefestoGooglePubSubAdapter;
    function BearerToken(const AValue: string): THefestoGooglePubSubAdapter;
    function DeadLetterTopic(const AValue: string): THefestoGooglePubSubAdapter;

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

{ THefestoGooglePubSubAdapter }

constructor THefestoGooglePubSubAdapter.Create;
begin
  inherited Create;
  FProject := '';
  FSubscription := '';
  FTopic := '';
  FBearerToken := '';
  FDeadLetterTopic := '';
end;

destructor THefestoGooglePubSubAdapter.Destroy;
begin
  inherited;
end;

class function THefestoGooglePubSubAdapter.New: THefestoGooglePubSubAdapter;
begin
  Result := THefestoGooglePubSubAdapter.Create;
end;

function THefestoGooglePubSubAdapter.Project(const AValue: string): THefestoGooglePubSubAdapter;
begin
  Result := Self;
  FProject := AValue.Trim;
end;

function THefestoGooglePubSubAdapter.Subscription(const AValue: string): THefestoGooglePubSubAdapter;
begin
  Result := Self;
  FSubscription := AValue.Trim;
end;

function THefestoGooglePubSubAdapter.Topic(const AValue: string): THefestoGooglePubSubAdapter;
begin
  Result := Self;
  FTopic := AValue.Trim;
end;

function THefestoGooglePubSubAdapter.BearerToken(const AValue: string): THefestoGooglePubSubAdapter;
begin
  Result := Self;
  FBearerToken := AValue.Trim;
end;

function THefestoGooglePubSubAdapter.DeadLetterTopic(const AValue: string): THefestoGooglePubSubAdapter;
begin
  Result := Self;
  FDeadLetterTopic := AValue.Trim;
end;

function THefestoGooglePubSubAdapter.SubscriptionUrl: string;
begin
  Result := Format('https://pubsub.googleapis.com/v1/projects/%s/subscriptions/%s',
    [FProject, FSubscription]);
end;

function THefestoGooglePubSubAdapter.TopicUrl(const ATopic: string): string;
begin
  Result := Format('https://pubsub.googleapis.com/v1/projects/%s/topics/%s',
    [FProject, ATopic]);
end;

function THefestoGooglePubSubAdapter.CreateHttpClient: THTTPClient;
begin
  Result := THTTPClient.Create;
  Result.ContentType := 'application/json';
  Result.CustomHeaders['Authorization'] := 'Bearer ' + FBearerToken;
end;

function THefestoGooglePubSubAdapter.DoPost(const AUrl, ABody: string): string;
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
        [TNameValuePair.Create('Content-Type', 'application/json')]);
      Result := LResponse.ContentAsString;
    finally
      LContent.Free;
    end;
  finally
    LClient.Free;
  end;
end;

function THefestoGooglePubSubAdapter.Name: string;
begin
  Result := 'google-pubsub';
end;

function THefestoGooglePubSubAdapter.Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
var
  LUrl: string;
  LRequestBody: string;
  LResponse: string;
  LJsonResponse: TJSONObject;
  LMessages: TJSONArray;
  LMessage: TJSONObject;
  LPubsubMessage: TJSONObject;
  LData: string;
  LDecodedData: string;
  LMessageId: string;
  LAckId: string;
  LAction: string;
  LEnvelope: THefestoJobEnvelope;
  LAttributes: TJSONObject;
  LAttrPair: TJSONPair;
  LDeliveryAttempt: Integer;
  LBatchSize: Integer;
  LCount: Integer;
  LJsonValue: TJSONValue;
begin
  SetLength(Result, 0);
  LBatchSize := AOptions.BatchSize;
  if LBatchSize <= 0 then
    LBatchSize := 1;

  LUrl := SubscriptionUrl + ':pull';
  LRequestBody := Format('{"maxMessages":%d}', [LBatchSize]);

  LResponse := DoPost(LUrl, LRequestBody);
  if LResponse.Trim.IsEmpty then
    Exit;

  LJsonValue := TJSONObject.ParseJSONValue(LResponse);
  try
    if not (LJsonValue is TJSONObject) then
      Exit;

    LJsonResponse := TJSONObject(LJsonValue);
    LMessages := LJsonResponse.GetValue<TJSONArray>('receivedMessages', nil);
    if (LMessages = nil) or (LMessages.Count = 0) then
      Exit;

    LCount := 0;
    SetLength(Result, LMessages.Count);

    for var I := 0 to LMessages.Count - 1 do
    begin
      LMessage := LMessages.Items[I] as TJSONObject;
      LAckId := LMessage.GetValue<string>('ackId', '');
      LDeliveryAttempt := LMessage.GetValue<Integer>('deliveryAttempt', 0);

      LPubsubMessage := LMessage.GetValue<TJSONObject>('message', nil);
      if LPubsubMessage = nil then
        Continue;

      LMessageId := LPubsubMessage.GetValue<string>('messageId', '');
      LData := LPubsubMessage.GetValue<string>('data', '');

      if not LData.IsEmpty then
        LDecodedData := TEncoding.UTF8.GetString(TNetEncoding.Base64.DecodeStringToBytes(LData))
      else
        LDecodedData := '';

      LAction := '';
      LAttributes := LPubsubMessage.GetValue<TJSONObject>('attributes', nil);
      if Assigned(LAttributes) then
      begin
        if Assigned(LAttributes.GetValue('action')) then
          LAction := LAttributes.GetValue<string>('action');
      end;

      if LAction.IsEmpty then
        LAction := ParseActionFromBody(LDecodedData);

      LEnvelope := THefestoJobEnvelope.Create(
        LMessageId,
        FSubscription,
        LAckId,
        LDecodedData,
        LAction,
        LDeliveryAttempt);

      if Assigned(LAttributes) then
      begin
        for LAttrPair in LAttributes do
          LEnvelope.AddAttribute(LAttrPair.JsonString.Value, LAttrPair.JsonValue.Value);
      end;

      Result[LCount] := LEnvelope;
      Inc(LCount);
    end;

    SetLength(Result, LCount);
  finally
    LJsonValue.Free;
  end;
end;

procedure THefestoGooglePubSubAdapter.Ack(const AJob: IHefestoJobEnvelope);
var
  LUrl: string;
  LBody: string;
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LUrl := SubscriptionUrl + ':acknowledge';
  LBody := Format('{"ackIds":["%s"]}', [AJob.ReceiptHandle]);
  DoPost(LUrl, LBody);
end;

procedure THefestoGooglePubSubAdapter.Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
var
  LUrl: string;
  LBody: string;
begin
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LUrl := SubscriptionUrl + ':modifyAckDeadline';
  LBody := Format('{"ackIds":["%s"],"ackDeadlineSeconds":%d}',
    [AJob.ReceiptHandle, ADelaySeconds]);
  DoPost(LUrl, LBody);
end;

procedure THefestoGooglePubSubAdapter.MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
var
  LRequest: THefestoPublishRequest;
  LTargetTopic: string;
begin
  LTargetTopic := FDeadLetterTopic;
  if LTargetTopic.IsEmpty then
    LTargetTopic := FTopic + '-dead-letter';

  LRequest.QueueName := LTargetTopic;
  LRequest.Action := AJob.Action;
  LRequest.Body := AJob.Body;
  LRequest.DelaySeconds := 0;
  SetLength(LRequest.Attributes, 2);
  LRequest.Attributes[0] := TPair<string, string>.Create('x-sidekiq-reason', AReason);
  LRequest.Attributes[1] := TPair<string, string>.Create('x-sidekiq-source-subscription', FSubscription);

  Publish(LRequest);
  Ack(AJob);
end;

procedure THefestoGooglePubSubAdapter.Publish(const ARequest: THefestoPublishRequest);
var
  LUrl: string;
  LTargetTopic: string;
  LEncodedData: string;
  LJson: TJSONObject;
  LMessages: TJSONArray;
  LMessage: TJSONObject;
  LAttributes: TJSONObject;
  I: Integer;
begin
  LTargetTopic := ARequest.QueueName;
  if LTargetTopic.IsEmpty then
    LTargetTopic := FTopic;

  LUrl := TopicUrl(LTargetTopic) + ':publish';
  LEncodedData := TNetEncoding.Base64.EncodeBytesToString(
    TEncoding.UTF8.GetBytes(ARequest.Body));

  LJson := TJSONObject.Create;
  try
    LMessages := TJSONArray.Create;
    LMessage := TJSONObject.Create;
    LMessage.AddPair('data', LEncodedData);

    LAttributes := TJSONObject.Create;
    if not ARequest.Action.IsEmpty then
      LAttributes.AddPair('action', ARequest.Action);

    for I := 0 to High(ARequest.Attributes) do
      LAttributes.AddPair(ARequest.Attributes[I].Key, ARequest.Attributes[I].Value);

    LMessage.AddPair('attributes', LAttributes);
    LMessages.AddElement(LMessage);
    LJson.AddPair('messages', LMessages);

    DoPost(LUrl, LJson.ToJSON);
  finally
    LJson.Free;
  end;
end;

function THefestoGooglePubSubAdapter.ParseActionFromBody(const ABody: string): string;
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
