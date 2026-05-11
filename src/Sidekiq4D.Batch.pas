unit Sidekiq4D.Batch;

interface

uses
  System.SysUtils,
  System.Classes,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Store.Interfaces;

type
  TSidekiqBatchCallbackKind = (bckComplete, bckSuccess);

  ISidekiqBatch = interface
    ['{8778F675-386F-4DAE-B2D1-E457166F7A65}']
    function Id: string;
    function Description: string;
    function OnComplete(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function OnSuccess(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function Enqueue(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function EnqueueIn(
      const AQueueName, AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function EnqueueAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): ISidekiqBatch;
  end;

  ISidekiqBatchEnqueuer = interface
    ['{333DA5C0-11B2-4A20-9D5A-3E16A80AFD45}']
    procedure EnqueueNow(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil);
    procedure ScheduleAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil);
  end;

  ISidekiqBatchService = interface
    ['{B0AE0E3E-1A15-4532-A708-37C3387D7283}']
    function CreateBatch(const ADescription: string = ''): string;
    procedure AddJob(const ABatchId: string);
    procedure RecordSuccess(const ABatchId: string);
    procedure RecordFinalFailure(const ABatchId: string);
    procedure RegisterCallback(
      const ABatchId: string;
      const AKind: TSidekiqBatchCallbackKind;
      const ARequest: TSidekiqPublishRequest);
    function PopReadyCallbacks(
      const ALimit: Integer = 0): TArray<TSidekiqPublishRequest>;
  end;

  TSidekiqBatchBuilder = class(TInterfacedObject, ISidekiqBatch)
  private
    FBatchId: string;
    FDescription: string;
    FEnqueuer: ISidekiqBatchEnqueuer;
    FService: ISidekiqBatchService;

    function BuildJobAttributes(const AAttributes: TStrings): TStringList;
    function BuildCallbackRequest(
      const AKind: TSidekiqBatchCallbackKind;
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings): TSidekiqPublishRequest;
  public
    constructor Create(
      const ABatchId, ADescription: string;
      const AEnqueuer: ISidekiqBatchEnqueuer;
      const AService: ISidekiqBatchService);

    class function New(
      const ABatchId, ADescription: string;
      const AEnqueuer: ISidekiqBatchEnqueuer;
      const AService: ISidekiqBatchService): ISidekiqBatch;

    function Id: string;
    function Description: string;
    function OnComplete(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function OnSuccess(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function Enqueue(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function EnqueueIn(
      const AQueueName, AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): ISidekiqBatch;
    function EnqueueAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): ISidekiqBatch;
  end;

  TSidekiqBatchStateStore = class(TInterfacedObject, ISidekiqBatchService)
  private
    FStateStore: ISidekiqStateStore;
    FLock: TObject;

    class function BatchKey(const ABatchId: string): string; static;
    class function CallbackKey(
      const ABatchId: string;
      const AKind: TSidekiqBatchCallbackKind): string; static;
    class function ReadyKey(
      const ABatchId: string;
      const AKind: TSidekiqBatchCallbackKind): string; static;
    class function KindName(const AKind: TSidekiqBatchCallbackKind): string; static;
    class function SerializeRequest(const ARequest: TSidekiqPublishRequest): string; static;
    class function DeserializeRequest(const AValue: string): TSidekiqPublishRequest; static;
    procedure EvaluateCallbacks(
      const ABatchId: string;
      var AStateJson: string);
    function LoadState(const ABatchId: string): string;
    procedure SaveState(const ABatchId, AStateJson: string);
  public
    constructor Create(const AStateStore: ISidekiqStateStore);
    destructor Destroy; override;

    class function New(const AStateStore: ISidekiqStateStore): ISidekiqBatchService;

    function CreateBatch(const ADescription: string = ''): string;
    procedure AddJob(const ABatchId: string);
    procedure RecordSuccess(const ABatchId: string);
    procedure RecordFinalFailure(const ABatchId: string);
    procedure RegisterCallback(
      const ABatchId: string;
      const AKind: TSidekiqBatchCallbackKind;
      const ARequest: TSidekiqPublishRequest);
    function PopReadyCallbacks(
      const ALimit: Integer = 0): TArray<TSidekiqPublishRequest>;
  end;

implementation

uses
  System.DateUtils,
  System.Generics.Collections,
  System.JSON,
  Sidekiq4D.Metadata;

type
  TSidekiqBatchState = record
    Description: string;
    Total: Integer;
    Pending: Integer;
    Failures: Integer;
    CompleteQueued: Boolean;
    SuccessQueued: Boolean;

    function ToJson: string;
    class function FromJson(const AValue: string): TSidekiqBatchState; static;
  end;

{ TSidekiqBatchState }

class function TSidekiqBatchState.FromJson(const AValue: string): TSidekiqBatchState;
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

function TSidekiqBatchState.ToJson: string;
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

{ TSidekiqBatchBuilder }

function TSidekiqBatchBuilder.BuildCallbackRequest(
  const AKind: TSidekiqBatchCallbackKind;
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): TSidekiqPublishRequest;
var
  LAttributes: TStringList;
begin
  LAttributes := BuildJobAttributes(AAttributes);
  try
    LAttributes.Values[TSidekiqJobAttribute.BatchCallbackKind] :=
      TSidekiqBatchStateStore.KindName(AKind);
    LAttributes.Values[TSidekiqJobAttribute.BatchCallbackFor] := FBatchId;
    Result := MakePublishRequest(AQueueName, AAction, ABody, 0, LAttributes);
  finally
    LAttributes.Free;
  end;
end;

function TSidekiqBatchBuilder.BuildJobAttributes(
  const AAttributes: TStrings): TStringList;
begin
  Result := TStringList.Create;
  if Assigned(AAttributes) then
    Result.Assign(AAttributes);
  Result.Values[TSidekiqJobAttribute.BatchId] := FBatchId;
end;

constructor TSidekiqBatchBuilder.Create(
  const ABatchId, ADescription: string;
  const AEnqueuer: ISidekiqBatchEnqueuer;
  const AService: ISidekiqBatchService);
begin
  inherited Create;
  FBatchId := ABatchId;
  FDescription := ADescription;
  FEnqueuer := AEnqueuer;
  FService := AService;
end;

function TSidekiqBatchBuilder.Description: string;
begin
  Result := FDescription;
end;

function TSidekiqBatchBuilder.Enqueue(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqBatch;
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

function TSidekiqBatchBuilder.EnqueueAt(
  const AQueueName, AAction, ABody: string;
  const ADueAt: TDateTime;
  const AAttributes: TStrings): ISidekiqBatch;
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

function TSidekiqBatchBuilder.EnqueueIn(
  const AQueueName, AAction, ABody: string;
  const ADelaySeconds: Integer;
  const AAttributes: TStrings): ISidekiqBatch;
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

function TSidekiqBatchBuilder.Id: string;
begin
  Result := FBatchId;
end;

class function TSidekiqBatchBuilder.New(
  const ABatchId, ADescription: string;
  const AEnqueuer: ISidekiqBatchEnqueuer;
  const AService: ISidekiqBatchService): ISidekiqBatch;
begin
  Result := TSidekiqBatchBuilder.Create(ABatchId, ADescription, AEnqueuer, AService);
end;

function TSidekiqBatchBuilder.OnComplete(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqBatch;
begin
  Result := Self;
  FService.RegisterCallback(
    FBatchId,
    bckComplete,
    BuildCallbackRequest(bckComplete, AQueueName, AAction, ABody, AAttributes));
end;

function TSidekiqBatchBuilder.OnSuccess(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqBatch;
begin
  Result := Self;
  FService.RegisterCallback(
    FBatchId,
    bckSuccess,
    BuildCallbackRequest(bckSuccess, AQueueName, AAction, ABody, AAttributes));
end;

{ TSidekiqBatchStateStore }

procedure TSidekiqBatchStateStore.AddJob(const ABatchId: string);
var
  LState: TSidekiqBatchState;
begin
  TMonitor.Enter(FLock);
  try
    LState := TSidekiqBatchState.FromJson(LoadState(ABatchId));
    Inc(LState.Total);
    Inc(LState.Pending);
    SaveState(ABatchId, LState.ToJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

class function TSidekiqBatchStateStore.BatchKey(const ABatchId: string): string;
begin
  Result := 'batch:' + ABatchId + ':state';
end;

class function TSidekiqBatchStateStore.CallbackKey(
  const ABatchId: string;
  const AKind: TSidekiqBatchCallbackKind): string;
begin
  Result := 'batch:' + ABatchId + ':callback:' + KindName(AKind);
end;

constructor TSidekiqBatchStateStore.Create(const AStateStore: ISidekiqStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
  FLock := TObject.Create;
end;

function TSidekiqBatchStateStore.CreateBatch(const ADescription: string): string;
var
  LState: TSidekiqBatchState;
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

class function TSidekiqBatchStateStore.DeserializeRequest(
  const AValue: string): TSidekiqPublishRequest;
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

destructor TSidekiqBatchStateStore.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure TSidekiqBatchStateStore.EvaluateCallbacks(
  const ABatchId: string;
  var AStateJson: string);
var
  LState: TSidekiqBatchState;
  LCompleteRequest: string;
  LSuccessRequest: string;
begin
  LState := TSidekiqBatchState.FromJson(AStateJson);
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

function TSidekiqBatchStateStore.LoadState(const ABatchId: string): string;
begin
  Result := FStateStore.Get(BatchKey(ABatchId));
end;

class function TSidekiqBatchStateStore.KindName(
  const AKind: TSidekiqBatchCallbackKind): string;
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

class function TSidekiqBatchStateStore.New(
  const AStateStore: ISidekiqStateStore): ISidekiqBatchService;
begin
  Result := TSidekiqBatchStateStore.Create(AStateStore);
end;

function TSidekiqBatchStateStore.PopReadyCallbacks(
  const ALimit: Integer): TArray<TSidekiqPublishRequest>;
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

procedure TSidekiqBatchStateStore.RecordFinalFailure(const ABatchId: string);
var
  LStateJson: string;
  LState: TSidekiqBatchState;
begin
  if ABatchId.Trim.IsEmpty then
    Exit;
  TMonitor.Enter(FLock);
  try
    LStateJson := LoadState(ABatchId);
    LState := TSidekiqBatchState.FromJson(LStateJson);
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

procedure TSidekiqBatchStateStore.RecordSuccess(const ABatchId: string);
var
  LStateJson: string;
  LState: TSidekiqBatchState;
begin
  if ABatchId.Trim.IsEmpty then
    Exit;
  TMonitor.Enter(FLock);
  try
    LStateJson := LoadState(ABatchId);
    LState := TSidekiqBatchState.FromJson(LStateJson);
    if LState.Pending > 0 then
      Dec(LState.Pending);
    LStateJson := LState.ToJson;
    EvaluateCallbacks(ABatchId, LStateJson);
    SaveState(ABatchId, LStateJson);
  finally
    TMonitor.Exit(FLock);
  end;
end;

class function TSidekiqBatchStateStore.ReadyKey(
  const ABatchId: string;
  const AKind: TSidekiqBatchCallbackKind): string;
begin
  Result := 'batch:' + ABatchId + ':ready:' + KindName(AKind);
end;

procedure TSidekiqBatchStateStore.RegisterCallback(
  const ABatchId: string;
  const AKind: TSidekiqBatchCallbackKind;
  const ARequest: TSidekiqPublishRequest);
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

procedure TSidekiqBatchStateStore.SaveState(
  const ABatchId, AStateJson: string);
begin
  FStateStore.Put(BatchKey(ABatchId), AStateJson, 86400);
end;

class function TSidekiqBatchStateStore.SerializeRequest(
  const ARequest: TSidekiqPublishRequest): string;
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
