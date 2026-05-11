unit Sidekiq4D.DeadLetter.Store;

{
  ISidekiqDeadLetterQueue backed by ISidekiqStateStore.

  Cada entrada é serializada como JSON e armazenada sob a chave
  'dlq:<job_id>'.  O método List() chama StateStore.ListKeys('dlq:')
  e desserializa cada entrada.  Retry() usa o callback FRetryPublish
  para re-publicar na fila original.

  Thread safety: ISidekiqStateStore já é thread-safe em todas as
  implementações fornecidas; nenhum lock adicional é necessário aqui.
}

interface

uses
  System.SysUtils,
  System.JSON,
  System.DateUtils,
  Sidekiq4D.DeadLetter,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Queue.Interfaces;

type
  { Callback para re-publicar o job.  Recebe o request já montado.
    O Server injeta uma lambda que chama DispatchPublish internamente. }
  TSidekiqRetryPublishProc = reference to procedure(
    const ARequest: TSidekiqPublishRequest);

  TSidekiqStateStoreDeadLetterQueue = class(TInterfacedObject,
    ISidekiqDeadLetterQueue)
  private
    FStore       : ISidekiqStateStore;
    FRetryPublish: TSidekiqRetryPublishProc;

    class function EntryKey(const AJobId: string): string; static;
    class function EntryToJson(
      const AEntry: TSidekiqDeadLetterEntry): string; static;
    class function JsonToEntry(
      const AJson: string): TSidekiqDeadLetterEntry; static;
  public
    constructor Create(
      const AStore       : ISidekiqStateStore;
      const ARetryPublish: TSidekiqRetryPublishProc);

    class function New(
      const AStore       : ISidekiqStateStore;
      const ARetryPublish: TSidekiqRetryPublishProc
      ): ISidekiqDeadLetterQueue;

    { ISidekiqDeadLetterQueue }
    procedure Push(const AEntry: TSidekiqDeadLetterEntry);
    function  Pop(const AJobId: string): TSidekiqDeadLetterEntry;
    function  List: TArray<TSidekiqDeadLetterEntry>;
    procedure Retry(const AJobId: string);
    procedure Delete(const AJobId: string);
    function  Count: Integer;
  end;

implementation

const
  DLQ_PREFIX = 'dlq:';

{ TSidekiqStateStoreDeadLetterQueue — private helpers }

class function TSidekiqStateStoreDeadLetterQueue.EntryKey(
  const AJobId: string): string;
begin
  Result := DLQ_PREFIX + AJobId;
end;

class function TSidekiqStateStoreDeadLetterQueue.EntryToJson(
  const AEntry: TSidekiqDeadLetterEntry): string;
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

class function TSidekiqStateStoreDeadLetterQueue.JsonToEntry(
  const AJson: string): TSidekiqDeadLetterEntry;
var
  LObj: TJSONObject;
begin
  Result := Default(TSidekiqDeadLetterEntry);
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

{ TSidekiqStateStoreDeadLetterQueue — public }

constructor TSidekiqStateStoreDeadLetterQueue.Create(
  const AStore       : ISidekiqStateStore;
  const ARetryPublish: TSidekiqRetryPublishProc);
begin
  inherited Create;
  FStore        := AStore;
  FRetryPublish := ARetryPublish;
end;

class function TSidekiqStateStoreDeadLetterQueue.New(
  const AStore       : ISidekiqStateStore;
  const ARetryPublish: TSidekiqRetryPublishProc): ISidekiqDeadLetterQueue;
begin
  Result := TSidekiqStateStoreDeadLetterQueue.Create(AStore, ARetryPublish);
end;

procedure TSidekiqStateStoreDeadLetterQueue.Push(
  const AEntry: TSidekiqDeadLetterEntry);
begin
  if AEntry.JobId.IsEmpty then
    Exit;
  FStore.Put(EntryKey(AEntry.JobId), EntryToJson(AEntry));
end;

function TSidekiqStateStoreDeadLetterQueue.Pop(
  const AJobId: string): TSidekiqDeadLetterEntry;
begin
  Result := JsonToEntry(FStore.Get(EntryKey(AJobId)));
  FStore.Delete(EntryKey(AJobId));
end;

function TSidekiqStateStoreDeadLetterQueue.List: TArray<TSidekiqDeadLetterEntry>;
var
  LKeys: TArray<string>;
  LKey : string;
  LJson: string;
  LEntry: TSidekiqDeadLetterEntry;
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

procedure TSidekiqStateStoreDeadLetterQueue.Retry(const AJobId: string);
var
  LEntry : TSidekiqDeadLetterEntry;
  LRequest: TSidekiqPublishRequest;
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

procedure TSidekiqStateStoreDeadLetterQueue.Delete(const AJobId: string);
begin
  FStore.Delete(EntryKey(AJobId));
end;

function TSidekiqStateStoreDeadLetterQueue.Count: Integer;
begin
  Result := Length(FStore.ListKeys(DLQ_PREFIX));
end;

end.
