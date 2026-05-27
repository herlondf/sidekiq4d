unit Hefesto.Batch;

interface

uses
  System.SysUtils,
  System.Classes,
  Hefesto.Queue.Interfaces,
  Hefesto.Store.Interfaces;

type
  THefestoBatchCallbackKind = (bckComplete, bckSuccess);

  IHefestoBatch = interface
    ['{8778F675-386F-4DAE-B2D1-E457166F7A65}']
    function Id: string;
    function Description: string;
    function OnComplete(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function OnSuccess(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function Enqueue(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function EnqueueIn(
      const AQueueName, AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function EnqueueAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): IHefestoBatch;
  end;

  IHefestoBatchEnqueuer = interface
    ['{333DA5C0-11B2-4A20-9D5A-3E16A80AFD45}']
    procedure EnqueueNow(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil);
    procedure ScheduleAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil);
  end;

  IHefestoBatchService = interface
    ['{B0AE0E3E-1A15-4532-A708-37C3387D7283}']
    function CreateBatch(const ADescription: string = ''): string;
    procedure AddJob(const ABatchId: string);
    procedure RecordSuccess(const ABatchId: string);
    procedure RecordFinalFailure(const ABatchId: string);
    procedure RegisterCallback(
      const ABatchId: string;
      const AKind: THefestoBatchCallbackKind;
      const ARequest: THefestoPublishRequest);
    function PopReadyCallbacks(
      const ALimit: Integer = 0): TArray<THefestoPublishRequest>;
  end;

  THefestoBatchBuilder = class(TInterfacedObject, IHefestoBatch)
  private
    FBatchId: string;
    FDescription: string;
    FEnqueuer: IHefestoBatchEnqueuer;
    FService: IHefestoBatchService;

    function BuildJobAttributes(const AAttributes: TStrings): TStringList;
    function BuildCallbackRequest(
      const AKind: THefestoBatchCallbackKind;
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings): THefestoPublishRequest;
  public
    constructor Create(
      const ABatchId, ADescription: string;
      const AEnqueuer: IHefestoBatchEnqueuer;
      const AService: IHefestoBatchService);

    class function New(
      const ABatchId, ADescription: string;
      const AEnqueuer: IHefestoBatchEnqueuer;
      const AService: IHefestoBatchService): IHefestoBatch;

    function Id: string;
    function Description: string;
    function OnComplete(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function OnSuccess(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function Enqueue(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function EnqueueIn(
      const AQueueName, AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): IHefestoBatch;
    function EnqueueAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): IHefestoBatch;
  end;

  THefestoBatchStateStore = class(TInterfacedObject, IHefestoBatchService)
  private
    FStateStore: IHefestoStateStore;
    FLock: TObject;

    class function BatchKey(const ABatchId: string): string; static;
    class function CallbackKey(
      const ABatchId: string;
      const AKind: THefestoBatchCallbackKind): string; static;
    class function ReadyKey(
      const ABatchId: string;
      const AKind: THefestoBatchCallbackKind): string; static;
    class function KindName(const AKind: THefestoBatchCallbackKind): string; static;
    class function SerializeRequest(const ARequest: THefestoPublishRequest): string; static;
    class function DeserializeRequest(const AValue: string): THefestoPublishRequest; static;
    procedure EvaluateCallbacks(
      const ABatchId: string;
      var AStateJson: string);
    function LoadState(const ABatchId: string): string;
    procedure SaveState(const ABatchId, AStateJson: string);
  public
    constructor Create(const AStateStore: IHefestoStateStore);
    destructor Destroy; override;

    class function New(const AStateStore: IHefestoStateStore): IHefestoBatchService;

    function CreateBatch(const ADescription: string = ''): string;
    procedure AddJob(const ABatchId: string);
    procedure RecordSuccess(const ABatchId: string);
    procedure RecordFinalFailure(const ABatchId: string);
    procedure RegisterCallback(
      const ABatchId: string;
      const AKind: THefestoBatchCallbackKind;
      const ARequest: THefestoPublishRequest);
    function PopReadyCallbacks(
      const ALimit: Integer = 0): TArray<THefestoPublishRequest>;
  end;

implementation

uses
  System.DateUtils,
  System.Generics.Collections,
  System.JSON,
  Hefesto.Metadata;

type
  THefestoBatchState = record
    Description: string;
    Total: Integer;
    Pending: Integer;
    Failures: Integer;
    CompleteQueued: Boolean;
    SuccessQueued: Boolean;

    function ToJson: string;
    class function FromJson(const AValue: string): THefestoBatchState; static;
  end;

{ THefestoBatchState }

class function THefestoBatchState.FromJson(const AValue: string): THefestoBatchState;
var
  LValue: TJSONValue;
  LObject: TJSONObject;
begin
  Result.Description := '';
  Result.Total := 0;
  Result.Pending := 0;
  Result.Failures := 0;
  Result.CompleteQueued := False;
  Result.SuccessQueued := False;
  if AValue.Trim.IsEmpty then
    Exit;

  LValue := TJSONObject.ParseJSONValue(AValue);
  try
    if not (LValue is TJSONObject) then
      Exit;
    LObject := TJSONObject(LValue);
    Result.Description := LObject.GetValue<string>('description', '');
    Result.Total := LObject.GetValue<Integer>('total', 0);
    Result.Pending := LObject.GetValue<Integer>('pending', 0);
    Result.Failures := LObject.GetValue<Integer>('failures', 0);
    Result.CompleteQueued := LObject.GetValue<Boolean>('complete_queued', False);
    Result.SuccessQueued := LObject.GetValue<Boolean>('success_queued', False);
  finally
    LValue.Free;
  end;
end;

function THefestoBatchState.ToJson: string;
var
  LObject: TJSONObject;
begin
  LObject := TJSONObject.Create;
  try
    LObject.AddPair('description', Description);
    LObject.AddPair('total', TJSONNumber.Create(Total));
    LObject.AddPair('pending', TJSONNumber.Create(Pending));
    LObject.AddPair('failures', TJSONNumber.Create(Failures));
    LObject.AddPair('complete_queued', TJSONBool.Create(CompleteQueued));
    LObject.AddPair('success_queued', TJSONBool.Create(SuccessQueued));
    Result := LObject.ToJSON;
  finally
    LObject.Free;
  end;
end;

{ THefestoBatchBuilder }

function THefestoBatchBuilder.BuildCallbackRequest(
  const AKind: THefestoBatchCallbackKind;
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): THefestoPublishRequest;
var
  LAttributes: TStringList;
begin
  LAttributes := BuildJobAttributes(AAttributes);
  try
    LAttributes.Values[THefestoJobAttribute.BatchCallbackKind] :=
      THefestoBatchStateStore.KindName(AKind);
    LAttributes.Values[THefestoJobAttribute.BatchCallbackFor] := FBatchId;
    Result := MakePublishRequest(AQueueName, AAction, ABody, 0, LAttributes);
  finally
    LAttributes.Free;
  end;
end;

function THefestoBatchBuilder.BuildJobAttributes(
  const AAttributes: TStrings): TStringList;
begin
  Result := TStringList.Create;
  if Assigned(AAttributes) then
    Result.Assign(AAttributes);
  Result.Values[THefestoJobAttribute.BatchId] := FBatchId;
end;

constructor THefestoBatchBuilder.Create(
  const ABatchId, ADescription: string;
  const AEnqueuer: IHefestoBatchEnqueuer;
  const AService: IHefestoBatchService);
begin
  inherited Create;
  FBatchId := ABatchId;
  FDescription := ADescription;
  FEnqueuer := AEnqueuer;
  FService := AService;
end;

function THefestoBatchBuilder.Description: string;
begin
  Result := FDescription;
end;

function THefestoBatchBuilder.Enqueue(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): IHefestoBatch;
var
  LAttributes: TStringList;
begin
  Result := Self;
  LAttributes := BuildJobAttributes(AAttributes);
  try
    FService.AddJob(FBatchId);
    FEnqueuer.EnqueueNow(AQueueName, AAction, ABody, LAttributes);
  finally
    LAttributes.Free;
  end;
end;

function THefestoBatchBuilder.EnqueueAt(
  const AQueueName, AAction, ABody: string;
  const ADueAt: TDateTime;
  const AAttributes: TStrings): IHefestoBatch;
var
  LAttributes: TStringList;
begin
  Result := Self;
  LAttributes := BuildJobAttributes(AAttributes);
  try
    FService.AddJob(FBatchId);
    FEnqueuer.ScheduleAt(AQueueName, AAction, ABody, ADueAt, LAttributes);
  finally
    LAttributes.Free;
  end;
end;

function THefestoBatchBuilder.EnqueueIn(
  const AQueueName, AAction, ABody: string;
  const ADelaySeconds: Integer;
  const AAttributes: TStrings): IHefestoBatch;
begin
  if ADelaySeconds <= 0 then
    Result := Enqueue(AQueueName, AAction, ABody, AAttributes)
  else
    Result := EnqueueAt(
      AQueueName,
      AAction,
      ABody,
      IncSecond(Now, ADelaySeconds),
      AAttributes);
end;

function THefestoBatchBuilder.Id: string;
begin
  Result := FBatchId;
end;

class function THefestoBatchBuilder.New(
  const ABatchId, ADescription: string;
  const AEnqueuer: IHefestoBatchEnqueuer;
  const AService: IHefestoBatchService): IHefestoBatch;
begin
  Result := THefestoBatchBuilder.Create(ABatchId, ADescription, AEnqueuer, AService);
end;

function THefestoBatchBuilder.OnComplete(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): IHefestoBatch;
begin
  Result := Self;
  FService.RegisterCallback(
    FBatchId,
    bckComplete,
    BuildCallbackRequest(bckComplete, AQueueName, AAction, ABody, AAttributes));
end;

function THefestoBatchBuilder.OnSuccess(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): IHefestoBatch;
begin
  Result := Self;
  FService.RegisterCallback(
    FBatchId,
    bckSuccess,
    BuildCallbackRequest(bckSuccess, AQueueName, AAction, ABody, AAttributes));
end;

{ THefestoBatchStateStore }

procedure THefestoBatchStateStore.AddJob(const ABatchId: string);
var
  LState: THefestoBatchState;
begin
  TMonitor.Enter(FLock);
  try
    LState := THefestoBatchState.FromJson(LoadState(ABatchId));
    Inc(LState.Total);
    Inc(LState.Pending);
    SaveState(ABatchId, LState.ToJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

class function THefestoBatchStateStore.BatchKey(const ABatchId: string): string;
begin
  Result := 'batch:' + ABatchId + ':state';
end;

class function THefestoBatchStateStore.CallbackKey(
  const ABatchId: string;
  const AKind: THefestoBatchCallbackKind): string;
begin
  Result := 'batch:' + ABatchId + ':callback:' + KindName(AKind);
end;

constructor THefestoBatchStateStore.Create(const AStateStore: IHefestoStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
  FLock := TObject.Create;
end;

function THefestoBatchStateStore.CreateBatch(const ADescription: string): string;
var
  LState: THefestoBatchState;
begin
  Result := GuidToString(TGuid.NewGuid);
  TMonitor.Enter(FLock);
  try
    LState.Description := ADescription;
    LState.Total := 0;
    LState.Pending := 0;
    LState.Failures := 0;
    LState.CompleteQueued := False;
    LState.SuccessQueued := False;
    SaveState(Result, LState.ToJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

class function THefestoBatchStateStore.DeserializeRequest(
  const AValue: string): THefestoPublishRequest;
var
  LJsonValue: TJSONValue;
  LObject: TJSONObject;
  LAttributes: TJSONArray;
  LPair: TJSONValue;
  LIndex: Integer;
begin
  Result.QueueName := '';
  Result.Action := '';
  Result.Body := '';
  Result.DelaySeconds := 0;
  SetLength(Result.Attributes, 0);
  if AValue.Trim.IsEmpty then
    Exit;

  LJsonValue := TJSONObject.ParseJSONValue(AValue);
  try
    if not (LJsonValue is TJSONObject) then
      Exit;
    LObject := TJSONObject(LJsonValue);
    Result.QueueName := LObject.GetValue<string>('queue_name', '');
    Result.Action := LObject.GetValue<string>('action', '');
    Result.Body := LObject.GetValue<string>('body', '');
    Result.DelaySeconds := LObject.GetValue<Integer>('delay_seconds', 0);
    LAttributes := LObject.GetValue<TJSONArray>('attributes');
    if Assigned(LAttributes) then
    begin
      SetLength(Result.Attributes, LAttributes.Count);
      for LIndex := 0 to Pred(LAttributes.Count) do
      begin
        LPair := LAttributes.Items[LIndex];
        if LPair is TJSONObject then
        begin
          Result.Attributes[LIndex].Key := TJSONObject(LPair).GetValue<string>('key', '');
          Result.Attributes[LIndex].Value := TJSONObject(LPair).GetValue<string>('value', '');
        end;
      end;
    end;
  finally
    LJsonValue.Free;
  end;
end;

destructor THefestoBatchStateStore.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure THefestoBatchStateStore.EvaluateCallbacks(
  const ABatchId: string;
  var AStateJson: string);
var
  LState: THefestoBatchState;
  LCompleteRequest: string;
  LSuccessRequest: string;
begin
  LState := THefestoBatchState.FromJson(AStateJson);
  if (LState.Total <= 0) or (LState.Pending > 0) then
    Exit;

  LCompleteRequest := FStateStore.Get(CallbackKey(ABatchId, bckComplete));
  if (not LState.CompleteQueued) and not LCompleteRequest.Trim.IsEmpty then
  begin
    FStateStore.Put(ReadyKey(ABatchId, bckComplete), LCompleteRequest, 86400);
    LState.CompleteQueued := True;
  end;

  LSuccessRequest := FStateStore.Get(CallbackKey(ABatchId, bckSuccess));
  if (LState.Failures = 0)
    and (not LState.SuccessQueued)
    and not LSuccessRequest.Trim.IsEmpty then
  begin
    FStateStore.Put(ReadyKey(ABatchId, bckSuccess), LSuccessRequest, 86400);
    LState.SuccessQueued := True;
  end;

  AStateJson := LState.ToJson;
end;

function THefestoBatchStateStore.LoadState(const ABatchId: string): string;
begin
  Result := FStateStore.Get(BatchKey(ABatchId));
end;

class function THefestoBatchStateStore.KindName(
  const AKind: THefestoBatchCallbackKind): string;
begin
  case AKind of
    bckComplete:
      Result := 'complete';
    bckSuccess:
      Result := 'success';
  else
    Result := 'unknown';
  end;
end;

class function THefestoBatchStateStore.New(
  const AStateStore: IHefestoStateStore): IHefestoBatchService;
begin
  Result := THefestoBatchStateStore.Create(AStateStore);
end;

function THefestoBatchStateStore.PopReadyCallbacks(
  const ALimit: Integer): TArray<THefestoPublishRequest>;
var
  LKey: string;
  LCount: Integer;
  LLimit: Integer;
begin
  TMonitor.Enter(FLock);
  try
    LCount := 0;
    LLimit := ALimit;
    if LLimit <= 0 then
      LLimit := MaxInt;
    SetLength(Result, 0);
    for LKey in FStateStore.ListKeys('batch:') do
    begin
      if Pos(':ready:', LKey) = 0 then
        Continue;
      if LCount >= LLimit then
        Break;
      SetLength(Result, LCount + 1);
      Result[LCount] := DeserializeRequest(FStateStore.Get(LKey));
      FStateStore.Delete(LKey);
      Inc(LCount);
    end;
  finally
    TMonitor.Exit(FLock);
  end;
end;

procedure THefestoBatchStateStore.RecordFinalFailure(const ABatchId: string);
var
  LStateJson: string;
  LState: THefestoBatchState;
begin
  if ABatchId.Trim.IsEmpty then
    Exit;
  TMonitor.Enter(FLock);
  try
    LStateJson := LoadState(ABatchId);
    LState := THefestoBatchState.FromJson(LStateJson);
    if LState.Pending > 0 then
      Dec(LState.Pending);
    Inc(LState.Failures);
    LStateJson := LState.ToJson;
    EvaluateCallbacks(ABatchId, LStateJson);
    SaveState(ABatchId, LStateJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

procedure THefestoBatchStateStore.RecordSuccess(const ABatchId: string);
var
  LStateJson: string;
  LState: THefestoBatchState;
begin
  if ABatchId.Trim.IsEmpty then
    Exit;
  TMonitor.Enter(FLock);
  try
    LStateJson := LoadState(ABatchId);
    LState := THefestoBatchState.FromJson(LStateJson);
    if LState.Pending > 0 then
      Dec(LState.Pending);
    LStateJson := LState.ToJson;
    EvaluateCallbacks(ABatchId, LStateJson);
    SaveState(ABatchId, LStateJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

class function THefestoBatchStateStore.ReadyKey(
  const ABatchId: string;
  const AKind: THefestoBatchCallbackKind): string;
begin
  Result := 'batch:' + ABatchId + ':ready:' + KindName(AKind);
end;

procedure THefestoBatchStateStore.RegisterCallback(
  const ABatchId: string;
  const AKind: THefestoBatchCallbackKind;
  const ARequest: THefestoPublishRequest);
var
  LStateJson: string;
begin
  TMonitor.Enter(FLock);
  try
    FStateStore.Put(CallbackKey(ABatchId, AKind), SerializeRequest(ARequest), 86400);
    LStateJson := LoadState(ABatchId);
    EvaluateCallbacks(ABatchId, LStateJson);
    SaveState(ABatchId, LStateJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

procedure THefestoBatchStateStore.SaveState(
  const ABatchId, AStateJson: string);
begin
  FStateStore.Put(BatchKey(ABatchId), AStateJson, 86400);
end;

class function THefestoBatchStateStore.SerializeRequest(
  const ARequest: THefestoPublishRequest): string;
var
  LObject: TJSONObject;
  LArray: TJSONArray;
  LIndex: Integer;
  LItem: TJSONObject;
begin
  LObject := TJSONObject.Create;
  try
    LObject.AddPair('queue_name', ARequest.QueueName);
    LObject.AddPair('action', ARequest.Action);
    LObject.AddPair('body', ARequest.Body);
    LObject.AddPair('delay_seconds', TJSONNumber.Create(ARequest.DelaySeconds));
    LArray := TJSONArray.Create;
    for LIndex := 0 to Pred(Length(ARequest.Attributes)) do
    begin
      LItem := TJSONObject.Create;
      LItem.AddPair('key', ARequest.Attributes[LIndex].Key);
      LItem.AddPair('value', ARequest.Attributes[LIndex].Value);
      LArray.AddElement(LItem);
    end;
    LObject.AddPair('attributes', LArray);
    Result := LObject.ToJSON;
  finally
    LObject.Free;
  end;
end;

end.
