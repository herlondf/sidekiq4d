unit Sidekiq4D.Scheduled;

interface

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.Generics.Collections,
  System.Generics.Defaults,
  System.SyncObjs,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces;

type
  TSidekiqScheduledEntry = record
    QueueName: string;
    Action: string;
    Body: string;
    Attributes: TArray<TPair<string, string>>;
    DueAt: TDateTime;
  end;

  ISidekiqScheduledStore = interface
    ['{4A8F96D6-6412-4F35-8B78-59C4F6C4E4F3}']
    procedure Schedule(const AEntry: TSidekiqScheduledEntry);
    function PopDue(
      const ANow: TDateTime;
      const ALimit: Integer): TArray<TSidekiqScheduledEntry>;
    function Count: Integer;
    procedure Clear;
    { Retorna snapshot de todos os jobs agendados (sem removê-los). }
    function List: TArray<TSidekiqScheduledEntry>;
    { Remove o primeiro job que corresponda a queue+action+dueAt. }
    procedure Delete(const AQueueName, AAction: string; const ADueAt: TDateTime);
  end;

  TSidekiqInMemoryScheduledStore = class(TInterfacedObject, ISidekiqScheduledStore)
  private
    FEntries: TList<TSidekiqScheduledEntry>;
    FLock: TCriticalSection;

    function FindInsertIndex(const AEntry: TSidekiqScheduledEntry): Integer;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: ISidekiqScheduledStore;

    procedure Schedule(const AEntry: TSidekiqScheduledEntry);
    function PopDue(
      const ANow: TDateTime;
      const ALimit: Integer): TArray<TSidekiqScheduledEntry>;
    function Count: Integer;
    procedure Clear;
    function List: TArray<TSidekiqScheduledEntry>;
    procedure Delete(const AQueueName, AAction: string; const ADueAt: TDateTime);
  end;

function MakeScheduledEntry(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings;
  const ADueAt: TDateTime): TSidekiqScheduledEntry;

procedure ApplyAttributesToStrings(
  const AEntry: TSidekiqScheduledEntry;
  const ATarget: TStrings);

implementation

function MakeScheduledEntry(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings;
  const ADueAt: TDateTime): TSidekiqScheduledEntry;
var
  LIndex: Integer;
  LName: string;
  LCount: Integer;
begin
  Result.QueueName := AQueueName;
  Result.Action := AAction;
  Result.Body := ABody;
  Result.DueAt := ADueAt;
  LCount := 0;
  if Assigned(AAttributes) then
  begin
    SetLength(Result.Attributes, AAttributes.Count);
    for LIndex := 0 to Pred(AAttributes.Count) do
    begin
      LName := AAttributes.Names[LIndex];
      if LName.IsEmpty then
        Continue;
      Result.Attributes[LCount].Key := LName;
      Result.Attributes[LCount].Value := AAttributes.ValueFromIndex[LIndex];
      Inc(LCount);
    end;
    SetLength(Result.Attributes, LCount);
  end
  else
    SetLength(Result.Attributes, 0);
end;

procedure ApplyAttributesToStrings(
  const AEntry: TSidekiqScheduledEntry;
  const ATarget: TStrings);
var
  LIndex: Integer;
begin
  if not Assigned(ATarget) then
    Exit;
  for LIndex := 0 to Pred(Length(AEntry.Attributes)) do
    ATarget.Values[AEntry.Attributes[LIndex].Key] := AEntry.Attributes[LIndex].Value;
end;

{ TSidekiqInMemoryScheduledStore }

procedure TSidekiqInMemoryScheduledStore.Clear;
begin
  FLock.Acquire;
  try
    FEntries.Clear;
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryScheduledStore.Count: Integer;
begin
  FLock.Acquire;
  try
    Result := FEntries.Count;
  finally
    FLock.Release;
  end;
end;

constructor TSidekiqInMemoryScheduledStore.Create;
begin
  inherited Create;
  FEntries := TList<TSidekiqScheduledEntry>.Create;
  FLock := TCriticalSection.Create;
end;

destructor TSidekiqInMemoryScheduledStore.Destroy;
begin
  FLock.Free;
  FEntries.Free;
  inherited;
end;

class function TSidekiqInMemoryScheduledStore.New: ISidekiqScheduledStore;
begin
  Result := TSidekiqInMemoryScheduledStore.Create;
end;

function TSidekiqInMemoryScheduledStore.PopDue(
  const ANow: TDateTime;
  const ALimit: Integer): TArray<TSidekiqScheduledEntry>;
var
  LResultList: TList<TSidekiqScheduledEntry>;
  LIndex: Integer;
  LLimit: Integer;
begin
  FLock.Acquire;
  try
    LLimit := ALimit;
    if LLimit <= 0 then
      LLimit := MaxInt;

    LResultList := TList<TSidekiqScheduledEntry>.Create;
    try
      LIndex := 0;
      while (LIndex < FEntries.Count) and (LResultList.Count < LLimit) do
      begin
        if FEntries[LIndex].DueAt <= ANow then
        begin
          LResultList.Add(FEntries[LIndex]);
          FEntries.Delete(LIndex);
        end
        else
          Break;
      end;
      Result := LResultList.ToArray;
    finally
      LResultList.Free;
    end;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqInMemoryScheduledStore.Schedule(
  const AEntry: TSidekiqScheduledEntry);
var
  LIndex: Integer;
begin
  FLock.Acquire;
  try
    LIndex := FindInsertIndex(AEntry);
    if LIndex >= FEntries.Count then
      FEntries.Add(AEntry)
    else
      FEntries.Insert(LIndex, AEntry);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryScheduledStore.List: TArray<TSidekiqScheduledEntry>;
begin
  FLock.Acquire;
  try
    Result := FEntries.ToArray;
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryScheduledStore.FindInsertIndex(
  const AEntry: TSidekiqScheduledEntry): Integer;
var
  LLo, LHi, LMid: Integer;
begin
  LLo := 0;
  LHi := FEntries.Count;
  while LLo < LHi do
  begin
    LMid := (LLo + LHi) div 2;
    if FEntries[LMid].DueAt <= AEntry.DueAt then
      LLo := LMid + 1
    else
      LHi := LMid;
  end;
  Result := LLo;
end;

procedure TSidekiqInMemoryScheduledStore.Delete(
  const AQueueName, AAction: string; const ADueAt: TDateTime);
var
  LIndex: Integer;
  LEntry: TSidekiqScheduledEntry;
begin
  FLock.Acquire;
  try
    for LIndex := 0 to Pred(FEntries.Count) do
    begin
      LEntry := FEntries[LIndex];
      if (LEntry.QueueName = AQueueName) and
         (LEntry.Action = AAction) and
         (Abs(LEntry.DueAt - ADueAt) < (1 / 86400)) then
      begin
        FEntries.Delete(LIndex);
        Exit;
      end;
    end;
  finally
    FLock.Release;
  end;
end;

end.
