unit Hefesto.Queue.SQS;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Math,
  System.SyncObjs,
  System.Generics.Collections,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.SQS.Client;

type
  THefestoSqsQueueAdapter = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueuePublisher, IHefestoQueueBatchPublisher, IHefestoQueueLeaseManager)
  private
    FName: string;
    FQueueUrl: string;
    FDeadLetterQueueUrl: string;
    FAccessKey: string;
    FSecretKey: string;
    FSessionToken: string;
    FRegion: string;
    FRequestedAttributes: TStringList;
    FRequestedMessageAttributes: TStringList;
    FLock: TCriticalSection;

    procedure EnsureConfigured;
    function BuildClient: THefestoSqsClient;
    procedure ApplyReceiveRequestDefaults;
    procedure SendToQueue(
      const AQueueUrl: string;
      const AJob: IHefestoJobEnvelope;
      const AExtraAttributes: TStrings;
      const ADelaySeconds: Integer);

    class function ClampBatchSize(const AValue: Integer): Integer; static;
    class function DeriveNameFromQueueUrl(const AQueueUrl: string): string; static;
    class function NormalizeQueueReference(
      const AQueueReference: string;
      const ARegion: string): string; static;
    class function ParseAttempts(const AAttributes: TStrings): Integer; static;
    class function ParseActionFromBody(const ABody: string): string; static;
    class procedure CopyAttributesToEnvelope(
      const AEnvelope: THefestoJobEnvelope;
      const AAttributes: TStrings); static;
  public
    constructor Create(const AQueueUrl: string = ''; const AName: string = '');
    destructor Destroy; override;

    class function New(
      const AQueueUrl: string = '';
      const AName: string = ''): THefestoSqsQueueAdapter;

    class function NormalizeQueueUrl(
      const AQueueReference: string;
      const ARegion: string = 'us-east-1'): string; static;

    function Name(const AValue: string): THefestoSqsQueueAdapter; overload;
    function Name: string; overload;
    function QueueUrl(const AValue: string): THefestoSqsQueueAdapter;
    function DeadLetterQueueUrl(const AValue: string): THefestoSqsQueueAdapter;
    function AccessKey(const AValue: string): THefestoSqsQueueAdapter;
    function SecretKey(const AValue: string): THefestoSqsQueueAdapter;
    function SessionToken(const AValue: string): THefestoSqsQueueAdapter;
    function Region(const AValue: string): THefestoSqsQueueAdapter;
    function AddAttribute(const AValue: string): THefestoSqsQueueAdapter;
    function AddMessageAttribute(const AValue: string): THefestoSqsQueueAdapter;

    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure Publish(const ARequest: THefestoPublishRequest);
    procedure PublishBatch(const ARequests: TArray<THefestoPublishRequest>);
    procedure RenewVisibility(
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer);
  end;

implementation

uses
  System.JSON;

{ THefestoSqsQueueAdapter }

procedure THefestoSqsQueueAdapter.Ack(const AJob: IHefestoJobEnvelope);
var
  LClient: THefestoSqsClient;
begin
  EnsureConfigured;
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LClient := BuildClient;
  try
    LClient.DeleteMessage(AJob.ReceiptHandle);
  finally
    LClient.Free;
  end;
end;

function THefestoSqsQueueAdapter.AccessKey(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  FAccessKey := AValue.Trim;
end;

function THefestoSqsQueueAdapter.AddAttribute(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  if AValue.Trim.IsEmpty then
    Exit;
  FLock.Acquire;
  try
    FRequestedAttributes.Add(AValue.Trim);
  finally
    FLock.Release;
  end;
end;

function THefestoSqsQueueAdapter.AddMessageAttribute(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  if AValue.Trim.IsEmpty then
    Exit;
  FLock.Acquire;
  try
    FRequestedMessageAttributes.Add(AValue.Trim);
  finally
    FLock.Release;
  end;
end;

procedure THefestoSqsQueueAdapter.ApplyReceiveRequestDefaults;
begin
  FLock.Acquire;
  try
    if FRequestedAttributes.Count = 0 then
    begin
      FRequestedAttributes.Add('ApproximateReceiveCount');
      FRequestedAttributes.Add('ApproximateFirstReceiveTimestamp');
      FRequestedAttributes.Add('SentTimestamp');
    end;

    if FRequestedMessageAttributes.Count = 0 then
      FRequestedMessageAttributes.Add('All');
  finally
    FLock.Release;
  end;
end;

function THefestoSqsQueueAdapter.BuildClient: THefestoSqsClient;
var
  LAttributes: TStringList;
  LMessageAttributes: TStringList;
begin
  FLock.Acquire;
  try
    LAttributes := TStringList.Create;
    LAttributes.Assign(FRequestedAttributes);
    LMessageAttributes := TStringList.Create;
    LMessageAttributes.Assign(FRequestedMessageAttributes);
  finally
    FLock.Release;
  end;
  try
    Result := THefestoSqsClient.New
      .QueueUrl(NormalizeQueueReference(FQueueUrl, FRegion))
      .AccessKey(FAccessKey)
      .SecretKey(FSecretKey)
      .SessionToken(FSessionToken)
      .Region(FRegion)
      .RequestedAttributes(LAttributes)
      .RequestedMessageAttributes(LMessageAttributes);
  finally
    LAttributes.Free;
    LMessageAttributes.Free;
  end;
end;

class function THefestoSqsQueueAdapter.ClampBatchSize(
  const AValue: Integer): Integer;
begin
  Result := AValue;
  if Result <= 0 then
    Result := 1;
  if Result > 10 then
    Result := 10;
end;

class procedure THefestoSqsQueueAdapter.CopyAttributesToEnvelope(
  const AEnvelope: THefestoJobEnvelope;
  const AAttributes: TStrings);
var
  LIndex: Integer;
  LName: string;
begin
  for LIndex := 0 to Pred(AAttributes.Count) do
  begin
    LName := AAttributes.Names[LIndex];
    if not LName.IsEmpty then
      AEnvelope.AddAttribute(LName, AAttributes.ValueFromIndex[LIndex]);
  end;
end;

constructor THefestoSqsQueueAdapter.Create(
  const AQueueUrl: string;
  const AName: string);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FRegion := 'us-east-1';
  FQueueUrl := AQueueUrl.Trim;
  FName := AName.Trim;
  if FName.IsEmpty then
    FName := DeriveNameFromQueueUrl(FQueueUrl);

  FRequestedAttributes := TStringList.Create;
  FRequestedMessageAttributes := TStringList.Create;
  FRequestedAttributes.CaseSensitive := False;
  FRequestedMessageAttributes.CaseSensitive := False;
  FRequestedAttributes.Duplicates := dupIgnore;
  FRequestedMessageAttributes.Duplicates := dupIgnore;
  FRequestedAttributes.StrictDelimiter := True;
  FRequestedMessageAttributes.StrictDelimiter := True;
end;

function THefestoSqsQueueAdapter.DeadLetterQueueUrl(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  FDeadLetterQueueUrl := AValue.Trim;
end;

class function THefestoSqsQueueAdapter.DeriveNameFromQueueUrl(
  const AQueueUrl: string): string;
var
  LPos: Integer;
begin
  Result := 'sqs';
  if AQueueUrl.Trim.IsEmpty then
    Exit;

  LPos := LastDelimiter('/\', AQueueUrl);
  if LPos > 0 then
    Result := Copy(AQueueUrl, LPos + 1, MaxInt)
  else
    Result := AQueueUrl;
end;

destructor THefestoSqsQueueAdapter.Destroy;
begin
  FLock.Free;
  FRequestedMessageAttributes.Free;
  FRequestedAttributes.Free;
  inherited;
end;

procedure THefestoSqsQueueAdapter.EnsureConfigured;
begin
  if NormalizeQueueReference(FQueueUrl, FRegion).IsEmpty then
    raise Exception.Create('SQS queue URL not configured.');
  if FAccessKey.IsEmpty then
    raise Exception.Create('SQS access key not configured.');
  if FSecretKey.IsEmpty then
    raise Exception.Create('SQS secret key not configured.');
end;

function THefestoSqsQueueAdapter.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
var
  LClient: THefestoSqsClient;
  LSqsOptions: THefestoSqsReceiveOptions;
  LMessages: TObjectList<THefestoSqsMessage>;
  LMessage: THefestoSqsMessage;
  LEnvelope: THefestoJobEnvelope;
  LBody: string;
  LAction: string;
  LAttempts: Integer;
  LCount: Integer;
begin
  EnsureConfigured;
  ApplyReceiveRequestDefaults;

  LSqsOptions.BatchSize := ClampBatchSize(AOptions.BatchSize);
  LSqsOptions.WaitTimeSeconds := AOptions.WaitTimeSeconds;
  LSqsOptions.VisibilityTimeout := AOptions.VisibilityTimeout;

  LClient := BuildClient;
  try
    LMessages := LClient.ReceiveMessages(LSqsOptions);
  finally
    LClient.Free;
  end;

  LCount := 0;
  SetLength(Result, 0);
  try
    for LMessage in LMessages do
    begin
      LBody := LMessage.Body.Replace(#13, EmptyStr).Replace(#10, EmptyStr);
      LAction := LMessage.MessageAttributes.Values['action'];
      if LAction.IsEmpty then
        LAction := ParseActionFromBody(LBody);

      LAttempts := ParseAttempts(LMessage.Attributes);
      LEnvelope := THefestoJobEnvelope.Create(
        LMessage.MessageId,
        Name,
        LMessage.ReceiptHandle,
        LBody,
        LAction,
        LAttempts);

      CopyAttributesToEnvelope(LEnvelope, LMessage.Attributes);
      CopyAttributesToEnvelope(LEnvelope, LMessage.MessageAttributes);

      SetLength(Result, LCount + 1);
      Result[LCount] := LEnvelope;
      Inc(LCount);
    end;
  finally
    LMessages.Free;
  end;
end;

procedure THefestoSqsQueueAdapter.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
var
  LAttributes: TStringList;
begin
  LAttributes := TStringList.Create;
  try
    LAttributes.Values['x-sidekiq-reason'] := AReason;
    LAttributes.Values['x-sidekiq-source-queue'] := Name;

    if not FDeadLetterQueueUrl.IsEmpty then
      SendToQueue(FDeadLetterQueueUrl, AJob, LAttributes, 0);

    Ack(AJob);
  finally
    LAttributes.Free;
  end;
end;

procedure THefestoSqsQueueAdapter.Nack(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
var
  LClient: THefestoSqsClient;
begin
  EnsureConfigured;
  if AJob.ReceiptHandle.IsEmpty then
    Exit;

  LClient := BuildClient;
  try
    LClient.ChangeMessageVisibility(AJob.ReceiptHandle, Max(0, ADelaySeconds));
  finally
    LClient.Free;
  end;
end;

procedure THefestoSqsQueueAdapter.Publish(
  const ARequest: THefestoPublishRequest);
var
  LClient: THefestoSqsClient;
  LAttributes: TStringList;
begin
  EnsureConfigured;
  LAttributes := TStringList.Create;
  try
    ApplyPublishAttributesToStrings(ARequest, LAttributes);
    if not ARequest.Action.IsEmpty then
      LAttributes.Values['action'] := ARequest.Action;

    LClient := THefestoSqsClient.New
      .QueueUrl(NormalizeQueueReference(FQueueUrl, FRegion))
      .AccessKey(FAccessKey)
      .SecretKey(FSecretKey)
      .SessionToken(FSessionToken)
      .Region(FRegion);
    try
      LClient.SendMessage(ARequest.Body, LAttributes, Max(0, ARequest.DelaySeconds));
    finally
      LClient.Free;
    end;
  finally
    LAttributes.Free;
  end;
end;

procedure THefestoSqsQueueAdapter.RenewVisibility(
  const AJob: IHefestoJobEnvelope;
  const AVisibilityTimeout: Integer);
var
  LClient: THefestoSqsClient;
begin
  EnsureConfigured;
  if (AVisibilityTimeout <= 0) or AJob.ReceiptHandle.IsEmpty then
    Exit;

  LClient := BuildClient;
  try
    LClient.ChangeMessageVisibility(AJob.ReceiptHandle, AVisibilityTimeout);
  finally
    LClient.Free;
  end;
end;

class function THefestoSqsQueueAdapter.New(
  const AQueueUrl: string;
  const AName: string): THefestoSqsQueueAdapter;
begin
  Result := THefestoSqsQueueAdapter.Create(AQueueUrl, AName);
end;

class function THefestoSqsQueueAdapter.NormalizeQueueReference(
  const AQueueReference: string;
  const ARegion: string): string;
var
  LQueueReference: string;
  LRegion: string;
begin
  LQueueReference := AQueueReference.Trim;
  if LQueueReference.IsEmpty then
    Exit(EmptyStr);

  if LQueueReference.StartsWith('https://', True) or
     LQueueReference.StartsWith('http://', True) then
    Exit(LQueueReference);

  LRegion := ARegion.Trim;
  if LRegion.IsEmpty then
    LRegion := 'us-east-1';

  while LQueueReference.StartsWith('/') do
    Delete(LQueueReference, 1, 1);

  Result := Format('https://sqs.%s.amazonaws.com/%s', [LRegion, LQueueReference]);
end;

class function THefestoSqsQueueAdapter.NormalizeQueueUrl(
  const AQueueReference: string;
  const ARegion: string): string;
begin
  Result := NormalizeQueueReference(AQueueReference, ARegion);
end;

function THefestoSqsQueueAdapter.Name: string;
begin
  Result := FName;
end;

function THefestoSqsQueueAdapter.Name(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  FName := AValue.Trim;
  if FName.IsEmpty then
    FName := DeriveNameFromQueueUrl(FQueueUrl);
end;

class function THefestoSqsQueueAdapter.ParseActionFromBody(
  const ABody: string): string;
var
  LJsonValue: TJSONValue;
  LJsonObject: TJSONObject;
begin
  Result := EmptyStr;
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

class function THefestoSqsQueueAdapter.ParseAttempts(
  const AAttributes: TStrings): Integer;
begin
  Result := StrToIntDef(AAttributes.Values['ApproximateReceiveCount'], 0);
end;

function THefestoSqsQueueAdapter.QueueUrl(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  FQueueUrl := AValue.Trim;
  if FName.IsEmpty or SameText(FName, 'sqs') then
    FName := DeriveNameFromQueueUrl(FQueueUrl);
end;

function THefestoSqsQueueAdapter.Region(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FRegion := AValue.Trim;
end;

function THefestoSqsQueueAdapter.SecretKey(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  FSecretKey := AValue.Trim;
end;

function THefestoSqsQueueAdapter.SessionToken(
  const AValue: string): THefestoSqsQueueAdapter;
begin
  Result := Self;
  FSessionToken := AValue.Trim;
end;

procedure THefestoSqsQueueAdapter.PublishBatch(
  const ARequests: TArray<THefestoPublishRequest>);
const
  SqsMaxBatch = 10;
var
  LClient: THefestoSqsClient;
  LBodies: TArray<string>;
  LDelays: TArray<Integer>;
  LAttrs: TArray<TStrings>;
  LOffset, LCount, LIndex: Integer;
  LAttributes: TStringList;
begin
  EnsureConfigured;
  LOffset := 0;
  while LOffset < Length(ARequests) do
  begin
    LCount := Min(SqsMaxBatch, Length(ARequests) - LOffset);
    SetLength(LBodies, LCount);
    SetLength(LDelays, LCount);
    SetLength(LAttrs, LCount);

    for LIndex := 0 to Pred(LCount) do
    begin
      LBodies[LIndex] := ARequests[LOffset + LIndex].Body;
      LDelays[LIndex] := Max(0, ARequests[LOffset + LIndex].DelaySeconds);
      LAttributes := TStringList.Create;
      ApplyPublishAttributesToStrings(ARequests[LOffset + LIndex], LAttributes);
      if not ARequests[LOffset + LIndex].Action.IsEmpty then
        LAttributes.Values['action'] := ARequests[LOffset + LIndex].Action;
      LAttrs[LIndex] := LAttributes;
    end;

    LClient := THefestoSqsClient.New
      .QueueUrl(NormalizeQueueReference(FQueueUrl, FRegion))
      .AccessKey(FAccessKey)
      .SecretKey(FSecretKey)
      .SessionToken(FSessionToken)
      .Region(FRegion);
    try
      LClient.SendMessageBatch(LBodies, LAttrs, LDelays);
    finally
      LClient.Free;
      for LIndex := 0 to Pred(LCount) do
        LAttrs[LIndex].Free;
    end;

    Inc(LOffset, LCount);
  end;
end;

procedure THefestoSqsQueueAdapter.SendToQueue(
  const AQueueUrl: string;
  const AJob: IHefestoJobEnvelope;
  const AExtraAttributes: TStrings;
  const ADelaySeconds: Integer);
var
  LClient: THefestoSqsClient;
  LAttributes: TStringList;
begin
  if AQueueUrl.IsEmpty then
    Exit;

  LAttributes := TStringList.Create;
  try
    LAttributes.Assign(AExtraAttributes);
    if not AJob.Action.IsEmpty then
      LAttributes.Values['action'] := AJob.Action;

    LClient := THefestoSqsClient.New
      .QueueUrl(NormalizeQueueReference(AQueueUrl, FRegion))
      .AccessKey(FAccessKey)
      .SecretKey(FSecretKey)
      .SessionToken(FSessionToken)
      .Region(FRegion);
    try
      LClient.SendMessage(AJob.Body, LAttributes, ADelaySeconds);
    finally
      LClient.Free;
    end;
  finally
    LAttributes.Free;
  end;
end;

end.
