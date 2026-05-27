unit Hefesto.SQS.Client;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  Xml.XMLIntf,
  Hefesto.HTTP;

type
  THefestoSqsMessage = class
  private
    FMessageId: string;
    FReceiptHandle: string;
    FBody: string;
    FAttributes: TStringList;
    FMessageAttributes: TStringList;
  public
    constructor Create;
    destructor Destroy; override;

    property MessageId: string read FMessageId write FMessageId;
    property ReceiptHandle: string read FReceiptHandle write FReceiptHandle;
    property Body: string read FBody write FBody;
    property Attributes: TStringList read FAttributes;
    property MessageAttributes: TStringList read FMessageAttributes;
  end;

  THefestoSqsReceiveOptions = record
    BatchSize: Integer;
    WaitTimeSeconds: Integer;
    VisibilityTimeout: Integer;
  end;

  THefestoSqsClient = class
  private
    FQueueUrl: string;
    FAccessKey: string;
    FSecretKey: string;
    FSessionToken: string;
    FRegion: string;
    FRequestedAttributes: TStringList;
    FRequestedMessageAttributes: TStringList;

    procedure EnsureConfigured;
    function CreateSignedRequest(const APayload: string): THefestoHttpRequest;
    class procedure AddIndexedValues(
      const AParams: TStrings;
      const APrefix: string;
      const AValues: TStrings); static;
    class procedure AddMessageAttributes(
      const AParams: TStrings;
      const APrefix: string;
      const AValues: TStrings); static;
    class function BuildFormBody(const AParams: TStrings): string; static;
    class function FindFirstChildByLocalName(
      const ANode: IXMLNode;
      const ALocalName: string): IXMLNode; static;
    class procedure FindChildrenByLocalName(
      const ANode: IXMLNode;
      const ALocalName: string;
      const ADestination: TInterfaceList); static;
    class function ParseText(
      const AParent: IXMLNode;
      const AChildLocalName: string): string; static;
    class function ExtractMessage(
      const ANode: IXMLNode): THefestoSqsMessage; static;
    class procedure RaiseSqsError(const AResponse: THefestoHttpResponse); static;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoSqsClient;

    function QueueUrl(const AValue: string): THefestoSqsClient;
    function AccessKey(const AValue: string): THefestoSqsClient;
    function SecretKey(const AValue: string): THefestoSqsClient;
    function SessionToken(const AValue: string): THefestoSqsClient;
    function Region(const AValue: string): THefestoSqsClient;
    function RequestedAttributes(const AValues: TStrings): THefestoSqsClient;
    function RequestedMessageAttributes(
      const AValues: TStrings): THefestoSqsClient;

    function ReceiveMessages(
      const AOptions: THefestoSqsReceiveOptions): TObjectList<THefestoSqsMessage>;
    procedure DeleteMessage(const AReceiptHandle: string);
    procedure ChangeMessageVisibility(
      const AReceiptHandle: string;
      const AVisibilityTimeout: Integer);
    procedure SendMessage(
      const ABody: string;
      const AAttributes: TStrings;
      const ADelaySeconds: Integer);
    // Sends up to 10 messages in a single SQS request (SendMessageBatch).
    // Each element: Body, Attributes, DelaySeconds. Silently skips empty items.
    procedure SendMessageBatch(
      const ABodies: TArray<string>;
      const AAttributes: TArray<TStrings>;
      const ADelaySeconds: TArray<Integer>);
  end;

implementation

uses
  System.Math,
  System.Net.URLClient,
  System.NetEncoding,
  Xml.XMLDoc,
  Hefesto.AWS.Signing;

{ THefestoSqsMessage }

constructor THefestoSqsMessage.Create;
begin
  inherited Create;
  FAttributes := TStringList.Create;
  FMessageAttributes := TStringList.Create;
end;

destructor THefestoSqsMessage.Destroy;
begin
  FMessageAttributes.Free;
  FAttributes.Free;
  inherited;
end;

{ THefestoSqsClient }

function THefestoSqsClient.AccessKey(const AValue: string): THefestoSqsClient;
begin
  Result := Self;
  FAccessKey := AValue.Trim;
end;

class procedure THefestoSqsClient.AddIndexedValues(
  const AParams: TStrings;
  const APrefix: string;
  const AValues: TStrings);
var
  LIndex: Integer;
begin
  for LIndex := 0 to Pred(AValues.Count) do
    AParams.Values[Format('%s.%d', [APrefix, LIndex + 1])] := AValues[LIndex];
end;

class procedure THefestoSqsClient.AddMessageAttributes(
  const AParams: TStrings;
  const APrefix: string;
  const AValues: TStrings);
var
  LIndex: Integer;
  LKey: string;
begin
  for LIndex := 0 to Pred(AValues.Count) do
  begin
    LKey := AValues.Names[LIndex];
    if LKey.IsEmpty then
      Continue;

    AParams.Values[Format('%s.%d.Name', [APrefix, LIndex + 1])] := LKey;
    AParams.Values[Format('%s.%d.Value.DataType', [APrefix, LIndex + 1])] := 'String';
    AParams.Values[Format('%s.%d.Value.StringValue', [APrefix, LIndex + 1])] :=
      AValues.ValueFromIndex[LIndex];
  end;
end;

class function THefestoSqsClient.BuildFormBody(const AParams: TStrings): string;
var
  LIndex: Integer;
  LName: string;
  LValue: string;
begin
  Result := EmptyStr;
  for LIndex := 0 to Pred(AParams.Count) do
  begin
    LName := AParams.Names[LIndex];
    LValue := AParams.ValueFromIndex[LIndex];
    if not Result.IsEmpty then
      Result := Result + '&';
    Result := Result + TNetEncoding.URL.Encode(LName) + '=' + TNetEncoding.URL.Encode(LValue);
  end;
end;

procedure THefestoSqsClient.ChangeMessageVisibility(
  const AReceiptHandle: string;
  const AVisibilityTimeout: Integer);
var
  LParams: TStringList;
  LRequest: THefestoHttpRequest;
  LResponse: THefestoHttpResponse;
begin
  if AReceiptHandle.Trim.IsEmpty then
    Exit;

  LParams := TStringList.Create;
  try
    LParams.Values['Action'] := 'ChangeMessageVisibility';
    LParams.Values['Version'] := '2012-11-05';
    LParams.Values['ReceiptHandle'] := AReceiptHandle;
    LParams.Values['VisibilityTimeout'] := AVisibilityTimeout.ToString;

    LRequest := CreateSignedRequest(BuildFormBody(LParams));
    try
      LResponse := THefestoHttpClient.Send(LRequest);
      try
        if LResponse.ResponseStatusCode >= 300 then
          RaiseSqsError(LResponse);
      finally
        LResponse.Free;
      end;
    finally
      LRequest.Free;
    end;
  finally
    LParams.Free;
  end;
end;

constructor THefestoSqsClient.Create;
begin
  inherited Create;
  FRegion := 'us-east-1';
  FRequestedAttributes := TStringList.Create;
  FRequestedMessageAttributes := TStringList.Create;
  FRequestedAttributes.Duplicates := dupIgnore;
  FRequestedMessageAttributes.Duplicates := dupIgnore;
end;

function THefestoSqsClient.CreateSignedRequest(
  const APayload: string): THefestoHttpRequest;
var
  LSigner: THefestoAwsV4Signer;
begin
  Result := THefestoHttpRequest.Create
    .Method('POST')
    .Url(FQueueUrl)
    .ContentType('application/x-www-form-urlencoded; charset=utf-8')
    .Body(APayload);

  LSigner := THefestoAwsV4Signer.New
    .AccessKey(FAccessKey)
    .SecretKey(FSecretKey)
    .SessionToken(FSessionToken)
    .Region(FRegion)
    .Service('sqs');
  try
    LSigner.Sign(Result);
  finally
    LSigner.Free;
  end;
end;

procedure THefestoSqsClient.DeleteMessage(const AReceiptHandle: string);
var
  LParams: TStringList;
  LRequest: THefestoHttpRequest;
  LResponse: THefestoHttpResponse;
begin
  if AReceiptHandle.Trim.IsEmpty then
    Exit;

  LParams := TStringList.Create;
  try
    LParams.Values['Action'] := 'DeleteMessage';
    LParams.Values['Version'] := '2012-11-05';
    LParams.Values['ReceiptHandle'] := AReceiptHandle;

    LRequest := CreateSignedRequest(BuildFormBody(LParams));
    try
      LResponse := THefestoHttpClient.Send(LRequest);
      try
        if LResponse.ResponseStatusCode >= 300 then
          RaiseSqsError(LResponse);
      finally
        LResponse.Free;
      end;
    finally
      LRequest.Free;
    end;
  finally
    LParams.Free;
  end;
end;

destructor THefestoSqsClient.Destroy;
begin
  FRequestedMessageAttributes.Free;
  FRequestedAttributes.Free;
  inherited;
end;

procedure THefestoSqsClient.EnsureConfigured;
begin
  if FQueueUrl.IsEmpty then
    raise Exception.Create('SQS queue URL not configured.');
  if FAccessKey.IsEmpty then
    raise Exception.Create('SQS access key not configured.');
  if FSecretKey.IsEmpty then
    raise Exception.Create('SQS secret key not configured.');
end;

class function THefestoSqsClient.ExtractMessage(
  const ANode: IXMLNode): THefestoSqsMessage;
var
  LNodeIndex: Integer;
  LChild: IXMLNode;
  LName: string;
  LValueNode: IXMLNode;
begin
  Result := THefestoSqsMessage.Create;
  Result.MessageId := ParseText(ANode, 'MessageId');
  Result.ReceiptHandle := ParseText(ANode, 'ReceiptHandle');
  Result.Body := ParseText(ANode, 'Body');

  for LNodeIndex := 0 to Pred(ANode.ChildNodes.Count) do
  begin
    LChild := ANode.ChildNodes[LNodeIndex];
    if SameText(LChild.LocalName, 'Attribute') then
      Result.Attributes.Values[ParseText(LChild, 'Name')] := ParseText(LChild, 'Value')
    else if SameText(LChild.LocalName, 'MessageAttribute') then
    begin
      LName := ParseText(LChild, 'Name');
      LValueNode := FindFirstChildByLocalName(LChild, 'Value');
      if Assigned(LValueNode) then
        Result.MessageAttributes.Values[LName] := ParseText(LValueNode, 'StringValue');
    end;
  end;
end;

class function THefestoSqsClient.FindFirstChildByLocalName(
  const ANode: IXMLNode;
  const ALocalName: string): IXMLNode;
var
  LIndex: Integer;
begin
  Result := nil;
  if not Assigned(ANode) then
    Exit;

  for LIndex := 0 to Pred(ANode.ChildNodes.Count) do
    if SameText(ANode.ChildNodes[LIndex].LocalName, ALocalName) then
      Exit(ANode.ChildNodes[LIndex]);
end;

class procedure THefestoSqsClient.FindChildrenByLocalName(
  const ANode: IXMLNode;
  const ALocalName: string;
  const ADestination: TInterfaceList);
var
  LIndex: Integer;
begin
  if not Assigned(ANode) then
    Exit;

  for LIndex := 0 to Pred(ANode.ChildNodes.Count) do
  begin
    if SameText(ANode.ChildNodes[LIndex].LocalName, ALocalName) then
      ADestination.Add(ANode.ChildNodes[LIndex]);

    FindChildrenByLocalName(ANode.ChildNodes[LIndex], ALocalName, ADestination);
  end;
end;

class function THefestoSqsClient.New: THefestoSqsClient;
begin
  Result := THefestoSqsClient.Create;
end;

class function THefestoSqsClient.ParseText(
  const AParent: IXMLNode;
  const AChildLocalName: string): string;
var
  LNode: IXMLNode;
begin
  Result := EmptyStr;
  LNode := FindFirstChildByLocalName(AParent, AChildLocalName);
  if Assigned(LNode) then
    Result := LNode.Text;
end;

function THefestoSqsClient.QueueUrl(const AValue: string): THefestoSqsClient;
begin
  Result := Self;
  FQueueUrl := AValue.Trim;
end;

class procedure THefestoSqsClient.RaiseSqsError(
  const AResponse: THefestoHttpResponse);
begin
  raise Exception.CreateFmt(
    'SQS request failed with HTTP %d. Body: %s',
    [AResponse.ResponseStatusCode, AResponse.ResponseBody]);
end;

function THefestoSqsClient.ReceiveMessages(
  const AOptions: THefestoSqsReceiveOptions): TObjectList<THefestoSqsMessage>;
var
  LParams: TStringList;
  LRequest: THefestoHttpRequest;
  LResponse: THefestoHttpResponse;
  LXml: IXMLDocument;
  LMessages: TInterfaceList;
  LIndex: Integer;
begin
  EnsureConfigured;

  LParams := TStringList.Create;
  try
    LParams.Values['Action'] := 'ReceiveMessage';
    LParams.Values['Version'] := '2012-11-05';
    LParams.Values['MaxNumberOfMessages'] := AOptions.BatchSize.ToString;
    if AOptions.WaitTimeSeconds > 0 then
      LParams.Values['WaitTimeSeconds'] := AOptions.WaitTimeSeconds.ToString;
    if AOptions.VisibilityTimeout > 0 then
      LParams.Values['VisibilityTimeout'] := AOptions.VisibilityTimeout.ToString;

    AddIndexedValues(LParams, 'AttributeName', FRequestedAttributes);
    AddIndexedValues(LParams, 'MessageAttributeName', FRequestedMessageAttributes);

    LRequest := CreateSignedRequest(BuildFormBody(LParams));
    try
      LResponse := THefestoHttpClient.Send(LRequest);
      try
        if LResponse.ResponseStatusCode >= 300 then
          RaiseSqsError(LResponse);

        Result := TObjectList<THefestoSqsMessage>.Create(True);
        if LResponse.ResponseBody.Trim.IsEmpty then
          Exit;

        LXml := TXMLDocument.Create(nil);
        LXml.LoadFromXML(LResponse.ResponseBody);
        LXml.Active := True;

        LMessages := TInterfaceList.Create;
        FindChildrenByLocalName(LXml.DocumentElement, 'Message', LMessages);
        for LIndex := 0 to Pred(LMessages.Count) do
          Result.Add(ExtractMessage(LMessages[LIndex] as IXMLNode));
      finally
        LResponse.Free;
      end;
    finally
      LRequest.Free;
    end;
  finally
    LParams.Free;
  end;
end;

function THefestoSqsClient.Region(const AValue: string): THefestoSqsClient;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FRegion := AValue.Trim;
end;

function THefestoSqsClient.RequestedAttributes(
  const AValues: TStrings): THefestoSqsClient;
begin
  Result := Self;
  FRequestedAttributes.Assign(AValues);
end;

function THefestoSqsClient.RequestedMessageAttributes(
  const AValues: TStrings): THefestoSqsClient;
begin
  Result := Self;
  FRequestedMessageAttributes.Assign(AValues);
end;

function THefestoSqsClient.SecretKey(const AValue: string): THefestoSqsClient;
begin
  Result := Self;
  FSecretKey := AValue.Trim;
end;

function THefestoSqsClient.SessionToken(
  const AValue: string): THefestoSqsClient;
begin
  Result := Self;
  FSessionToken := AValue.Trim;
end;

procedure THefestoSqsClient.SendMessage(
  const ABody: string;
  const AAttributes: TStrings;
  const ADelaySeconds: Integer);
var
  LParams: TStringList;
  LRequest: THefestoHttpRequest;
  LResponse: THefestoHttpResponse;
begin
  EnsureConfigured;

  LParams := TStringList.Create;
  try
    LParams.Values['Action'] := 'SendMessage';
    LParams.Values['Version'] := '2012-11-05';
    LParams.Values['MessageBody'] := ABody;
    if ADelaySeconds > 0 then
      LParams.Values['DelaySeconds'] := ADelaySeconds.ToString;

    AddMessageAttributes(LParams, 'MessageAttribute', AAttributes);

    LRequest := CreateSignedRequest(BuildFormBody(LParams));
    try
      LResponse := THefestoHttpClient.Send(LRequest);
      try
        if LResponse.ResponseStatusCode >= 300 then
          RaiseSqsError(LResponse);
      finally
        LResponse.Free;
      end;
    finally
      LRequest.Free;
    end;
  finally
    LParams.Free;
  end;
end;

procedure THefestoSqsClient.SendMessageBatch(
  const ABodies: TArray<string>;
  const AAttributes: TArray<TStrings>;
  const ADelaySeconds: TArray<Integer>);
var
  LParams: TStringList;
  LRequest: THefestoHttpRequest;
  LResponse: THefestoHttpResponse;
  LIndex: Integer;
  LPrefix: string;
  LCount: Integer;
begin
  EnsureConfigured;
  LCount := Min(10, Length(ABodies));
  if LCount = 0 then
    Exit;

  LParams := TStringList.Create;
  try
    LParams.Values['Action'] := 'SendMessageBatch';
    LParams.Values['Version'] := '2012-11-05';

    for LIndex := 0 to Pred(LCount) do
    begin
      LPrefix := Format('SendMessageBatchRequestEntry.%d', [LIndex + 1]);
      LParams.Values[LPrefix + '.Id'] := IntToStr(LIndex);
      LParams.Values[LPrefix + '.MessageBody'] := ABodies[LIndex];
      if (LIndex < Length(ADelaySeconds)) and (ADelaySeconds[LIndex] > 0) then
        LParams.Values[LPrefix + '.DelaySeconds'] := ADelaySeconds[LIndex].ToString;
      if LIndex < Length(AAttributes) then
        AddMessageAttributes(LParams, LPrefix + '.MessageAttribute', AAttributes[LIndex]);
    end;

    LRequest := CreateSignedRequest(BuildFormBody(LParams));
    try
      LResponse := THefestoHttpClient.Send(LRequest);
      try
        if LResponse.ResponseStatusCode >= 300 then
          RaiseSqsError(LResponse);
      finally
        LResponse.Free;
      end;
    finally
      LRequest.Free;
    end;
  finally
    LParams.Free;
  end;
end;

end.
