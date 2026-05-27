unit Hefesto.ThreadSafety.Tests;

{
  Stress tests de thread-safety para Hefesto — DUnitX.

  Padrão de barreira: TCountdownEvent com Signal()/Wait() faz todas as threads
  dispararem simultaneamente, maximizando a probabilidade de colisão real.

  Cobertura dos fixes aplicados nas rodadas de auditoria 2-5:
    - THefestoStateStoreLockHandle.Release()   (Round 4 — FReleased: Integer)
    - THefestoStateStoreLockHandle.Renew()     (Round 4 — TInterlocked.Read)
    - THefestoInMemoryStateStore               (core infra — TryPutIfAbsent, Put/Get)
    - THefestoInMemoryLockProvider             (exclusão mútua via TryPutIfAbsent)
    - THefestoInMemoryQueueAdapter.Fetch()     (sem dupla entrega)
}

interface

uses
  DUnitX.TestFramework,
  System.Classes,
  System.SyncObjs,
  System.SysUtils,
  System.Generics.Collections,
  Hefesto.Store.Interfaces,
  Hefesto.Store.InMemory,
  Hefesto.Locking,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Job,
  Hefesto.Graph;

type
  // ---------------------------------------------------------------------------
  // Spy store: conta chamadas a Delete() atomicamente.
  // Permite verificar que Release() concorrente dispara Delete() exatamente 1x.
  // ---------------------------------------------------------------------------
  THefestoSpyStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    FInner: IHefestoStateStore;
    FDeleteCount: Integer;
  public
    constructor Create(const AInner: IHefestoStateStore);
    function DeleteCount: Integer;
    { IHefestoStateStore }
    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  [TestFixture('LockHandle')]
  TLockHandleThreadSafetyTests = class
  public
    [Test]
    [Category('ThreadSafety')]
    { Verifica que CompareExchange impede dupla deleção quando N threads
      chamam Release() ao mesmo tempo (fix Round 4). }
    procedure Release_ConcurrentCalls_DeletesKeyExactlyOnce;

    [Test]
    [Category('ThreadSafety')]
    { Verifica que Renew() e Release() concorrentes não levantam exceção
      (TInterlocked.Read no Renew — fix Round 4). }
    procedure Renew_ConcurrentWithRelease_NoExceptionRaised;
  end;

  [TestFixture('InMemoryStore')]
  TInMemoryStateStoreThreadSafetyTests = class
  public
    [Test]
    [Category('ThreadSafety')]
    { TryPutIfAbsent() na mesma chave deve ter sucesso exatamente 1x
      entre N threads concorrentes. }
    procedure TryPutIfAbsent_ConcurrentCalls_OnlyOneSucceeds;

    [Test]
    [Category('ThreadSafety')]
    { Put()/Get() concorrentes na mesma chave não devem levantar exceção. }
    procedure ConcurrentPutGet_SameKey_NoCrash;
  end;

  [TestFixture('InMemoryLock')]
  TInMemoryLockProviderThreadSafetyTests = class
  public
    [Test]
    [Category('ThreadSafety')]
    { TryAcquire() na mesma chave deve conceder exatamente 1 handle
      entre N threads concorrentes — exclusão mútua. }
    procedure TryAcquire_ConcurrentCallsSameKey_ExactlyOneHandleGranted;
  end;

  [TestFixture('InMemoryQueue')]
  TInMemoryQueueThreadSafetyTests = class
  public
    [Test]
    [Category('ThreadSafety')]
    { Fetch() concorrente não deve entregar o mesmo item a mais de um thread. }
    procedure ConcurrentFetch_NoItemDeliveredTwice;
  end;

  [TestFixture('JobGraphParallel')]
  TJobGraphParallelThreadSafetyTests = class
  public
    [Test]
    [Category('ThreadSafety')]
    { N instâncias independentes de THefestoJobGraph.Parallel rodando
      simultaneamente não devem provocar race conditions. }
    procedure ConcurrentParallelGraphs_AllNodesComplete;
  end;

implementation

const
  STRESS_THREADS = 50;
  STRESS_TIMEOUT = 10000; // ms — tempo máximo de espera na barreira por thread

// ---------------------------------------------------------------------------
// Helper: cria N threads que disparam simultaneamente via barreira
// ---------------------------------------------------------------------------
procedure RunConcurrent(AThreadCount: Integer; const ABody: TProc);
var
  LThreads: TArray<TThread>;
  LBarrier: TCountdownEvent;
  I: Integer;
begin
  SetLength(LThreads, AThreadCount);
  LBarrier := TCountdownEvent.Create(AThreadCount);
  try
    for I := 0 to AThreadCount - 1 do
    begin
      LThreads[I] := TThread.CreateAnonymousThread(
        procedure
        begin
          LBarrier.Signal;             // sinaliza "estou pronto"
          LBarrier.WaitFor(STRESS_TIMEOUT); // espera todas as outras threads
          ABody();                     // dispara junto
        end);
      LThreads[I].FreeOnTerminate := False;
    end;

    for I := 0 to AThreadCount - 1 do LThreads[I].Start;
    for I := 0 to AThreadCount - 1 do
    begin
      LThreads[I].WaitFor;
      LThreads[I].Free;
    end;
  finally
    LBarrier.Free;
  end;
end;

// ---------------------------------------------------------------------------
// THefestoSpyStateStore
// ---------------------------------------------------------------------------

constructor THefestoSpyStateStore.Create(const AInner: IHefestoStateStore);
begin
  inherited Create;
  FInner := AInner;
end;

function THefestoSpyStateStore.DeleteCount: Integer;
begin
  Result := TInterlocked.CompareExchange(FDeleteCount, 0, 0);
end;

procedure THefestoSpyStateStore.Delete(const AKey: string);
begin
  TInterlocked.Increment(FDeleteCount); // conta toda chamada, inclusive em chave já removida
  FInner.Delete(AKey);
end;

function THefestoSpyStateStore.Exists(const AKey: string): Boolean;
begin
  Result := FInner.Exists(AKey);
end;

function THefestoSpyStateStore.Get(const AKey: string): string;
begin
  Result := FInner.Get(AKey);
end;

function THefestoSpyStateStore.ListKeys(const APrefix: string): TArray<string>;
begin
  Result := FInner.ListKeys(APrefix);
end;

procedure THefestoSpyStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
begin
  FInner.Put(AKey, AValue, ATtlSeconds);
end;

function THefestoSpyStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
begin
  Result := FInner.TryPutIfAbsent(AKey, AValue, ATtlSeconds);
end;

// ---------------------------------------------------------------------------
// TLockHandleThreadSafetyTests
// ---------------------------------------------------------------------------

procedure TLockHandleThreadSafetyTests.Release_ConcurrentCalls_DeletesKeyExactlyOnce;
var
  LSpy: THefestoSpyStateStore;
  LStore: IHefestoStateStore;
  LHandle: IHefestoLockHandle;
begin
  // Arrange
  LSpy   := THefestoSpyStateStore.Create(THefestoInMemoryStateStore.New);
  LStore := LSpy;
  LStore.Put('lock:concurrent-release', 'tok-x', 60);
  LHandle := THefestoStateStoreLockHandle.Create(LStore, 'concurrent-release', 'tok-x');

  // Act — STRESS_THREADS threads chamam Release() simultaneamente
  RunConcurrent(STRESS_THREADS,
    procedure
    begin
      LHandle.Release;
    end);

  // Assert
  Assert.AreEqual(1, LSpy.DeleteCount,
    Format('Delete() deve ser chamado exatamente 1 vez '
      + '(%d threads chamaram Release() simultaneamente)', [STRESS_THREADS]));
  Assert.IsFalse(LStore.Exists('lock:concurrent-release'),
    'A chave do lock deve ter sido removida');
end;

procedure TLockHandleThreadSafetyTests.Renew_ConcurrentWithRelease_NoExceptionRaised;
var
  LStore: IHefestoStateStore;
  LHandle: IHefestoRenewableLockHandle;
  LExceptionCount: Integer;
  LCounter: Integer;
begin
  // Arrange
  LStore  := THefestoInMemoryStateStore.New;
  LStore.Put('lock:renew-race', 'tok-r', 60);
  LHandle := THefestoStateStoreLockHandle.Create(LStore, 'renew-race', 'tok-r');

  LExceptionCount := 0;
  LCounter := 0;

  // Act — metade das threads renova, metade libera (alternado via contador atômico)
  RunConcurrent(STRESS_THREADS,
    procedure
    begin
      try
        if TInterlocked.Increment(LCounter) mod 2 = 0 then
          LHandle.Renew(30)
        else
          LHandle.Release;
      except
        TInterlocked.Increment(LExceptionCount);
      end;
    end);

  // Assert
  Assert.AreEqual(0, LExceptionCount,
    'Renew() e Release() concorrentes não devem levantar exceções');
end;

// ---------------------------------------------------------------------------
// TInMemoryStateStoreThreadSafetyTests
// ---------------------------------------------------------------------------

procedure TInMemoryStateStoreThreadSafetyTests.TryPutIfAbsent_ConcurrentCalls_OnlyOneSucceeds;
var
  LStore: IHefestoStateStore;
  LSuccessCount: Integer;
begin
  // Arrange
  LStore := THefestoInMemoryStateStore.New;
  LSuccessCount := 0;

  // Act — STRESS_THREADS threads tentam inserir a mesma chave simultaneamente
  RunConcurrent(STRESS_THREADS,
    procedure
    begin
      if LStore.TryPutIfAbsent('contested-key', 'val', 60) then
        TInterlocked.Increment(LSuccessCount);
    end);

  // Assert
  Assert.AreEqual(1, LSuccessCount,
    Format('TryPutIfAbsent() na mesma chave deve ter sucesso exatamente '
      + '1 vez entre %d threads', [STRESS_THREADS]));
end;

procedure TInMemoryStateStoreThreadSafetyTests.ConcurrentPutGet_SameKey_NoCrash;
var
  LStore: IHefestoStateStore;
  LExceptionCount: Integer;
begin
  // Arrange
  LStore := THefestoInMemoryStateStore.New;
  LExceptionCount := 0;

  // Act — todas as threads fazem Put() + Get() na mesma chave simultaneamente
  RunConcurrent(STRESS_THREADS,
    procedure
    begin
      try
        LStore.Put('shared-key', 'value', 60);
        LStore.Get('shared-key');
      except
        TInterlocked.Increment(LExceptionCount);
      end;
    end);

  // Assert
  Assert.AreEqual(0, LExceptionCount,
    'Put()/Get() concorrentes na mesma chave não devem levantar exceções');
end;

// ---------------------------------------------------------------------------
// TInMemoryLockProviderThreadSafetyTests
// ---------------------------------------------------------------------------

procedure TInMemoryLockProviderThreadSafetyTests.TryAcquire_ConcurrentCallsSameKey_ExactlyOneHandleGranted;
var
  LStore: IHefestoStateStore;
  LProvider: IHefestoLockProvider;
  LGrantedCount: Integer;
begin
  // Arrange
  LStore    := THefestoInMemoryStateStore.New;
  LProvider := THefestoInMemoryLockProvider.New(LStore);
  LGrantedCount := 0;

  // Act — STRESS_THREADS threads tentam adquirir a mesma chave simultaneamente
  RunConcurrent(STRESS_THREADS,
    procedure
    begin
      var LHandle := LProvider.TryAcquire('exclusive-lock', 30);
      if Assigned(LHandle) then
        TInterlocked.Increment(LGrantedCount);
    end);

  // Assert
  Assert.AreEqual(1, LGrantedCount,
    Format('TryAcquire() na mesma chave deve conceder exatamente '
      + '1 handle entre %d threads simultâneos', [STRESS_THREADS]));
end;

// ---------------------------------------------------------------------------
// TInMemoryQueueThreadSafetyTests
// ---------------------------------------------------------------------------

procedure TInMemoryQueueThreadSafetyTests.ConcurrentFetch_NoItemDeliveredTwice;
const
  ITEM_COUNT    = 200;
  FETCH_THREADS = 10;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LFetchedIds: TDictionary<string, Integer>;
  LResultLock: TCriticalSection;
  LDuplicateCount: Integer;
  LOptions: THefestoFetchOptions;
  LThreads: TArray<TThread>;
  LBarrier: TCountdownEvent;
  I: Integer;
begin
  // Arrange — pré-popula com ITEM_COUNT jobs
  LQueue := THefestoInMemoryQueueAdapter.New('stress-queue');
  for I := 1 to ITEM_COUNT do
    LQueue.Enqueue('job.action', '{"n":' + I.ToString + '}');

  LFetchedIds := TDictionary<string, Integer>.Create;
  LResultLock := TCriticalSection.Create;
  LBarrier    := TCountdownEvent.Create(FETCH_THREADS);
  LDuplicateCount := 0;

  LOptions.BatchSize        := (ITEM_COUNT div FETCH_THREADS) + 5;
  LOptions.WaitTimeSeconds  := 0;
  LOptions.VisibilityTimeout := 0;

  SetLength(LThreads, FETCH_THREADS);
  try
    for I := 0 to FETCH_THREADS - 1 do
    begin
      LThreads[I] := TThread.CreateAnonymousThread(
        procedure
        var
          LBatch: TArray<IHefestoJobEnvelope>;
          LJob: IHefestoJobEnvelope;
        begin
          LBarrier.Signal;
          LBarrier.WaitFor(STRESS_TIMEOUT);

          LBatch := LQueue.Fetch(LOptions);

          // Registra IDs obtidos — protegido por lock local (o próprio dict não é thread-safe)
          LResultLock.Acquire;
          try
            for LJob in LBatch do
            begin
              if LFetchedIds.ContainsKey(LJob.Id) then
                TInterlocked.Increment(LDuplicateCount)
              else
                LFetchedIds.Add(LJob.Id, 1);
            end;
          finally
            LResultLock.Release;
          end;
        end);
      LThreads[I].FreeOnTerminate := False;
    end;

    for I := 0 to FETCH_THREADS - 1 do LThreads[I].Start;
    for I := 0 to FETCH_THREADS - 1 do
    begin
      LThreads[I].WaitFor;
      LThreads[I].Free;
    end;
  finally
    LBarrier.Free;
    LResultLock.Free;
    LFetchedIds.Free;
    LQueue.Free;
  end;

  // Assert
  Assert.AreEqual(0, LDuplicateCount,
    'Nenhum job deve ser entregue a mais de um thread em Fetch() concorrentes');
end;

procedure TJobGraphParallelThreadSafetyTests.ConcurrentParallelGraphs_AllNodesComplete;
const
  GRAPH_THREADS = 20;
  NODES_PER_GRAPH = 4; // diamante: root → left, right → merge
var
  LBarrier: TCountdownEvent;
  LTotalCompleted: Integer;
  LThreads: TArray<TThread>;
  I: Integer;
begin
  LBarrier        := TCountdownEvent.Create(GRAPH_THREADS);
  LTotalCompleted := 0;

  SetLength(LThreads, GRAPH_THREADS);
  for I := 0 to GRAPH_THREADS - 1 do
  begin
    LThreads[I] := TThread.CreateAnonymousThread(
      procedure
      var
        LLocalCount: Integer;
      begin
        LLocalCount := 0;
        LBarrier.Signal;
        LBarrier.WaitFor(STRESS_TIMEOUT);

        THefestoJobGraph.New
          .AddNode('root',  procedure begin TInterlocked.Increment(LLocalCount); end)
          .AddNode('left',  procedure begin TInterlocked.Increment(LLocalCount); end)
            .DependsOn('root')
          .AddNode('right', procedure begin TInterlocked.Increment(LLocalCount); end)
            .DependsOn('root')
          .AddNode('merge', procedure begin TInterlocked.Increment(LLocalCount); end)
            .DependsOn('left').DependsOn('right')
          .Parallel
          .Execute;

        if LLocalCount = NODES_PER_GRAPH then
          TInterlocked.Increment(LTotalCompleted);
      end);
    LThreads[I].FreeOnTerminate := False;
  end;

  for I := 0 to GRAPH_THREADS - 1 do LThreads[I].Start;
  for I := 0 to GRAPH_THREADS - 1 do
  begin
    LThreads[I].WaitFor;
    LThreads[I].Free;
  end;

  LBarrier.Free;

  Assert.AreEqual(GRAPH_THREADS, LTotalCompleted,
    Format('Todos os %d grafos paralelos devem completar %d nós sem race condition',
      [GRAPH_THREADS, NODES_PER_GRAPH]));
end;

initialization
  TDUnitX.RegisterTestFixture(TLockHandleThreadSafetyTests);
  TDUnitX.RegisterTestFixture(TInMemoryStateStoreThreadSafetyTests);
  TDUnitX.RegisterTestFixture(TInMemoryLockProviderThreadSafetyTests);
  TDUnitX.RegisterTestFixture(TInMemoryQueueThreadSafetyTests);
  TDUnitX.RegisterTestFixture(TJobGraphParallelThreadSafetyTests);

end.
