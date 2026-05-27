unit Hefesto.DeadLetter.Store;

{
  IHefestoDeadLetterQueue backed by IHefestoStateStore.

  Cada entrada é serializada como JSON e armazenada sob a chave
  'dlq:<job_id>'.  O método List() chama StateStore.ListKeys('dlq:')
  e desserializa cada entrada.  Retry() usa o callback FRetryPublish
  para re-publicar na fila original.

  Thread safety: IHefestoStateStore já é thread-safe em todas as
  implementações fornecidas; nenhum lock adicional é necessário aqui.
}

interface

uses
  System.SysUtils,
  System.JSON,
  System.DateUtils,
  Hefesto.DeadLetter,
  Hefesto.Store.Interfaces,
  Hefesto.Queue.Interfaces;

type
  { Callback para re-publicar o job.  Recebe o request já montado.
    O Server injeta uma lambda que chama DispatchPublish internamente. }
  THefestoRetryPublishProc = reference to procedure(
    const ARequest: THefestoPublishRequest);

  THefestoStateStoreDeadLetterQueue = class(TInterfacedObject,
    IHefestoDeadLetterQueue)
  private
    FStore       : IHefestoStateStore;
    FRetryPublish: THefestoRetryPublishProc;

    class function EntryKey(const AJobId: string): string; static;
    class function EntryToJson(
      const AEntry: THefestoDeadLetterEntry): string; static;
    class function JsonToEntry(
      const AJson: string): THefestoDeadLetterEntry; static;
  public
    constructor Create(
      const AStore       : IHefestoStateStore;
      const ARetryPublish: THefestoRetryPublishProc);

    class function New(
      const AStore       : IHefestoStateStore;
      const ARetryPublish: THefestoRetryPublishProc
      ): IHefestoDeadLetterQueue;

    { IHefestoDeadLetterQueue }
    procedure Push(const AEntry: THefestoDeadLetterEntry);
    function  Pop(const AJobId: string): THefestoDeadLetterEntry;
    function  List: TArray<THefestoDeadLetterEntry>;
    procedure Retry(const AJobId: string);
    procedure Delete(const AJobId: string);
    function  Count: Integer;
  end;

implementation

const
  DLQ_PREFIX = 'dlq:';

{ THefestoStateStoreDeadLetterQueue — private helpers }

class function THefestoStateStoreDeadLetterQueue.EntryKey(
  const AJobId: string): string;
begin
  Result := DLQ_PREFIX + AJobId;
end;

class function THefestoStateStoreDeadLetterQueue.EntryToJson(
  const AEntry: THefestoDeadLetterEntry): string;
var
  LObj: TJSONObject;
begin
  LObj := TJSONObject.Create;
  try
    LObj.AddPair('job_id',         AEntry.JobId);
    LObj.AddPair('job_json',       AEntry.JobJson);
    LObj.AddPair('action',         AEntry.Action);
    LObj.AddPair('original_queue', AEntry.OriginalQueue);
    LObj.AddPair('error_message',  AEntry.ErrorMessage);
    LObj.AddPair('retry_count',    TJSONNumber.Create(AEntry.RetryCount));
    LObj.AddPair('failed_at',
      TJSONNumber.Create(DateTimeToUnix(AEntry.FailedAt, False)));
    Result := LObj.ToJSON;
  finally
    LObj.Free;
  end;
end;

class function THefestoStateStoreDeadLetterQueue.JsonToEntry(
  const AJson: string): THefestoDeadLetterEntry;
var
  LObj: TJSONObject;
begin
  Result := Default(THefestoDeadLetterEntry);
  if AJson.IsEmpty then
    Exit;

  LObj := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  if not Assigned(LObj) then
    Exit;
  try
    Result.JobId         := LObj.GetValue<string>('job_id',         '');
    Result.JobJson       := LObj.GetValue<string>('job_json',       '');
    Result.Action        := LObj.GetValue<string>('action',         '');
    Result.OriginalQueue := LObj.GetValue<string>('original_queue', '');
    Result.ErrorMessage  := LObj.GetValue<string>('error_message',  '');
    Result.RetryCount    := LObj.GetValue<Integer>('retry_count',   0);
    Result.FailedAt      := UnixToDateTime(
      LObj.GetValue<Int64>('failed_at', 0), False);
  finally
    LObj.Free;
  end;
end;

{ THefestoStateStoreDeadLetterQueue — public }

constructor THefestoStateStoreDeadLetterQueue.Create(
  const AStore       : IHefestoStateStore;
  const ARetryPublish: THefestoRetryPublishProc);
begin
  inherited Create;
  FStore        := AStore;
  FRetryPublish := ARetryPublish;
end;

class function THefestoStateStoreDeadLetterQueue.New(
  const AStore       : IHefestoStateStore;
  const ARetryPublish: THefestoRetryPublishProc): IHefestoDeadLetterQueue;
begin
  Result := THefestoStateStoreDeadLetterQueue.Create(AStore, ARetryPublish);
end;

procedure THefestoStateStoreDeadLetterQueue.Push(
  const AEntry: THefestoDeadLetterEntry);
begin
  if AEntry.JobId.IsEmpty then
    Exit;
  FStore.Put(EntryKey(AEntry.JobId), EntryToJson(AEntry));
end;

function THefestoStateStoreDeadLetterQueue.Pop(
  const AJobId: string): THefestoDeadLetterEntry;
begin
  Result := JsonToEntry(FStore.Get(EntryKey(AJobId)));
  FStore.Delete(EntryKey(AJobId));
end;

function THefestoStateStoreDeadLetterQueue.List: TArray<THefestoDeadLetterEntry>;
var
  LKeys: TArray<string>;
  LKey : string;
  LJson: string;
  LEntry: THefestoDeadLetterEntry;
  LCount: Integer;
begin
  LKeys := FStore.ListKeys(DLQ_PREFIX);
  SetLength(Result, Length(LKeys));
  LCount := 0;
  for LKey in LKeys do
  begin
    LJson := FStore.Get(LKey);
    if LJson.IsEmpty then
      Continue;
    LEntry := JsonToEntry(LJson);
    if LEntry.JobId.IsEmpty then
      Continue;
    Result[LCount] := LEntry;
    Inc(LCount);
  end;
  SetLength(Result, LCount);
end;

procedure THefestoStateStoreDeadLetterQueue.Retry(const AJobId: string);
var
  LEntry : THefestoDeadLetterEntry;
  LRequest: THefestoPublishRequest;
begin
  LEntry := JsonToEntry(FStore.Get(EntryKey(AJobId)));
  if LEntry.JobId.IsEmpty then
    Exit;

  if Assigned(FRetryPublish) then
  begin
    LRequest.QueueName    := LEntry.OriginalQueue;
    LRequest.Action       := LEntry.Action;
    LRequest.Body         := LEntry.JobJson;
    LRequest.DelaySeconds := 0;
    SetLength(LRequest.Attributes, 0);
    FRetryPublish(LRequest);
  end;

  FStore.Delete(EntryKey(AJobId));
end;

procedure THefestoStateStoreDeadLetterQueue.Delete(const AJobId: string);
begin
  FStore.Delete(EntryKey(AJobId));
end;

function THefestoStateStoreDeadLetterQueue.Count: Integer;
begin
  Result := Length(FStore.ListKeys(DLQ_PREFIX));
end;

end.
