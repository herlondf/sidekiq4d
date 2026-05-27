unit Hefesto.Scheduled.Tests;

{
  Testes unitários para THefestoInMemoryScheduledStore.
  Cobre: Schedule, Count, Clear, List, PopDue (limite, ordem, vencimento).
  Usa THefestoInMemoryScheduledStore — sem broker externo.
}

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.DateUtils,
  System.Classes,
  Hefesto.Scheduled;

type
  [TestFixture('ScheduledStore')]
  TScheduledStoreTests = class
  private
    FStore: IHefestoScheduledStore;

    function MakeEntry(
      const AAction: string;
      const ADueAt: TDateTime): THefestoScheduledEntry;
  public
    [Setup]
    procedure Setup;

    [Test][Category('Unit')]
    procedure Count_EmptyStore_ReturnsZero;

    [Test][Category('Unit')]
    procedure Schedule_OneEntry_CountIsOne;

    [Test][Category('Unit')]
    procedure Schedule_MultipleEntries_CountIsCorrect;

    [Test][Category('Unit')]
    procedure Clear_AfterSchedule_CountIsZero;

    [Test][Category('Unit')]
    procedure List_AfterSchedule_ReturnsSnapshot;

    [Test][Category('Unit')]
    procedure List_DoesNotRemoveEntries;

    [Test][Category('Unit')]
    procedure PopDue_NoDueJobs_ReturnsEmpty;

    [Test][Category('Unit')]
    procedure PopDue_DueJob_ReturnsEntry;

    [Test][Category('Unit')]
    procedure PopDue_DueJob_RemovesFromStore;

    [Test][Category('Unit')]
    procedure PopDue_Limit_RespectsLimit;

    [Test][Category('Unit')]
    procedure PopDue_MixedDueFuture_ReturnsOnlyDue;

    [Test][Category('Unit')]
    procedure PopDue_OrderedByDueAt_EarliestFirst;

    [Test][Category('Unit')]
    procedure Schedule_MaintainsChronologicalOrder;

    [Test][Category('Unit')]
    procedure MakeScheduledEntry_WithAttributes_SetsAttributes;

    [Test][Category('Unit')]
    procedure ApplyAttributesToStrings_FillsTarget;
  end;

implementation

function TScheduledStoreTests.MakeEntry(
  const AAction: string;
  const ADueAt: TDateTime): THefestoScheduledEntry;
begin
  Result := Default(THefestoScheduledEntry);
  Result.QueueName := 'default';
  Result.Action    := AAction;
  Result.Body      := '{}';
  Result.DueAt     := ADueAt;
end;

procedure TScheduledStoreTests.Setup;
begin
  FStore := THefestoInMemoryScheduledStore.New;
end;

procedure TScheduledStoreTests.Count_EmptyStore_ReturnsZero;
begin
  Assert.AreEqual(0, FStore.Count, 'Store novo deve ter Count = 0');
end;

procedure TScheduledStoreTests.Schedule_OneEntry_CountIsOne;
begin
  FStore.Schedule(MakeEntry('SendEmail', Now + 1));
  Assert.AreEqual(1, FStore.Count);
end;

procedure TScheduledStoreTests.Schedule_MultipleEntries_CountIsCorrect;
begin
  FStore.Schedule(MakeEntry('A', Now + 1));
  FStore.Schedule(MakeEntry('B', Now + 2));
  FStore.Schedule(MakeEntry('C', Now + 3));
  Assert.AreEqual(3, FStore.Count);
end;

procedure TScheduledStoreTests.Clear_AfterSchedule_CountIsZero;
begin
  FStore.Schedule(MakeEntry('A', Now + 1));
  FStore.Schedule(MakeEntry('B', Now + 2));
  FStore.Clear;
  Assert.AreEqual(0, FStore.Count, 'Clear deve remover todas as entradas');
end;

procedure TScheduledStoreTests.List_AfterSchedule_ReturnsSnapshot;
begin
  FStore.Schedule(MakeEntry('A', Now + 1));
  FStore.Schedule(MakeEntry('B', Now + 2));
  Assert.AreEqual(2, Length(FStore.List));
end;

procedure TScheduledStoreTests.List_DoesNotRemoveEntries;
begin
  FStore.Schedule(MakeEntry('A', Now + 1));
  FStore.List;
  FStore.List;
  Assert.AreEqual(1, FStore.Count, 'List nao deve remover entradas');
end;

procedure TScheduledStoreTests.PopDue_NoDueJobs_ReturnsEmpty;
var
  LFuture: TDateTime;
begin
  LFuture := Now + 1; // 1 dia no futuro
  FStore.Schedule(MakeEntry('A', LFuture));
  Assert.AreEqual(0, Length(FStore.PopDue(Now, 100)),
    'Job futuro nao deve aparecer em PopDue');
end;

procedure TScheduledStoreTests.PopDue_DueJob_ReturnsEntry;
var
  LPast: TDateTime;
  LResult: TArray<THefestoScheduledEntry>;
begin
  LPast := Now - 1; // 1 dia no passado
  FStore.Schedule(MakeEntry('SendSMS', LPast));
  LResult := FStore.PopDue(Now, 100);
  Assert.AreEqual(1, Length(LResult));
  Assert.AreEqual('SendSMS', LResult[0].Action);
end;

procedure TScheduledStoreTests.PopDue_DueJob_RemovesFromStore;
begin
  FStore.Schedule(MakeEntry('A', Now - 1));
  FStore.PopDue(Now, 100);
  Assert.AreEqual(0, FStore.Count, 'PopDue deve remover os jobs vencidos');
end;

procedure TScheduledStoreTests.PopDue_Limit_RespectsLimit;
var
  LResult: TArray<THefestoScheduledEntry>;
  LI: Integer;
begin
  for LI := 1 to 5 do
    FStore.Schedule(MakeEntry('Job' + LI.ToString, Now - LI));
  LResult := FStore.PopDue(Now, 2);
  Assert.AreEqual(2, Length(LResult), 'PopDue com limit=2 deve retornar exatamente 2');
  Assert.AreEqual(3, FStore.Count, 'Os 3 restantes devem permanecer no store');
end;

procedure TScheduledStoreTests.PopDue_MixedDueFuture_ReturnsOnlyDue;
var
  LResult: TArray<THefestoScheduledEntry>;
begin
  FStore.Schedule(MakeEntry('past1', Now - 2));
  FStore.Schedule(MakeEntry('past2', Now - 1));
  FStore.Schedule(MakeEntry('future', Now + 1));
  LResult := FStore.PopDue(Now, 100);
  Assert.AreEqual(2, Length(LResult),
    'PopDue deve retornar apenas os jobs vencidos');
  Assert.AreEqual(1, FStore.Count, 'Job futuro deve permanecer no store');
end;

procedure TScheduledStoreTests.PopDue_OrderedByDueAt_EarliestFirst;
var
  LResult: TArray<THefestoScheduledEntry>;
begin
  FStore.Schedule(MakeEntry('later',   Now - 1));
  FStore.Schedule(MakeEntry('earlier', Now - 3));
  FStore.Schedule(MakeEntry('middle',  Now - 2));
  LResult := FStore.PopDue(Now, 100);
  Assert.AreEqual(3, Length(LResult));
  Assert.IsTrue(LResult[0].DueAt <= LResult[1].DueAt,
    'Primeiro resultado deve ter DueAt <= segundo');
  Assert.IsTrue(LResult[1].DueAt <= LResult[2].DueAt,
    'Segundo resultado deve ter DueAt <= terceiro');
end;

procedure TScheduledStoreTests.Schedule_MaintainsChronologicalOrder;
var
  LList: TArray<THefestoScheduledEntry>;
begin
  FStore.Schedule(MakeEntry('C', Now + 3));
  FStore.Schedule(MakeEntry('A', Now + 1));
  FStore.Schedule(MakeEntry('B', Now + 2));
  LList := FStore.List;
  Assert.AreEqual(3, Length(LList));
  Assert.IsTrue(LList[0].DueAt <= LList[1].DueAt,
    'Store deve manter ordem cronologica crescente');
  Assert.IsTrue(LList[1].DueAt <= LList[2].DueAt);
end;

procedure TScheduledStoreTests.MakeScheduledEntry_WithAttributes_SetsAttributes;
var
  LAttrs: TStringList;
  LEntry: THefestoScheduledEntry;
begin
  LAttrs := TStringList.Create;
  try
    LAttrs.Values['tenant'] := 'acme';
    LAttrs.Values['prio']   := 'high';
    LEntry := MakeScheduledEntry('emails', 'SendEmail', '{}', LAttrs, Now + 1);
    Assert.AreEqual('emails',    LEntry.QueueName);
    Assert.AreEqual('SendEmail', LEntry.Action);
    Assert.AreEqual(2,           Length(LEntry.Attributes));
  finally
    LAttrs.Free;
  end;
end;

procedure TScheduledStoreTests.ApplyAttributesToStrings_FillsTarget;
var
  LEntry  : THefestoScheduledEntry;
  LAttrs  : TStringList;
  LTarget : TStringList;
begin
  LAttrs := TStringList.Create;
  LTarget := TStringList.Create;
  try
    LAttrs.Values['tenant'] := 'acme';
    LEntry := MakeScheduledEntry('q', 'A', '{}', LAttrs, Now);
    ApplyAttributesToStrings(LEntry, LTarget);
    Assert.AreEqual('acme', LTarget.Values['tenant']);
  finally
    LAttrs.Free;
    LTarget.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TScheduledStoreTests);

end.
