program Hefesto.Tests;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.SyncObjs,
  System.Threading,
  System.Generics.Collections,
  Hefesto.Job in '..\src\Hefesto.Job.pas',
  Hefesto.Metadata in '..\src\Hefesto.Metadata.pas',
  Hefesto.Context in '..\src\Hefesto.Context.pas',
  Hefesto.Handler in '..\src\Hefesto.Handler.pas',
  Hefesto.Options in '..\src\Hefesto.Options.pas',
  Hefesto.Store.Interfaces in '..\src\Hefesto.Store.Interfaces.pas',
  Hefesto.Store.InMemory in '..\src\Hefesto.Store.InMemory.pas',
  Hefesto.Store.Postgres in '..\src\adapters\Hefesto.Store.Postgres.pas',
  Hefesto.Locking in '..\src\Hefesto.Locking.pas',
  Hefesto.Idempotency in '..\src\Hefesto.Idempotency.pas',
  Hefesto.ClientReliability in '..\src\Hefesto.ClientReliability.pas',
  Hefesto.RateLimit in '..\src\Hefesto.RateLimit.pas',
  Hefesto.Unique in '..\src\Hefesto.Unique.pas',
  Hefesto.Ack in '..\src\Hefesto.Ack.pas',
  Hefesto.Queue.Interfaces in '..\src\Hefesto.Queue.Interfaces.pas',
  Hefesto.Queue.InMemory in '..\src\Hefesto.Queue.InMemory.pas',
  Hefesto.Queue.SQS in '..\src\adapters\Hefesto.Queue.SQS.pas',
  Hefesto.Dispatcher in '..\src\Hefesto.Dispatcher.pas',
  Hefesto.Retry in '..\src\Hefesto.Retry.pas',
  Hefesto.Telemetry in '..\src\Hefesto.Telemetry.pas',
  Hefesto.Middleware in '..\src\Hefesto.Middleware.pas',
  Hefesto.Scheduled in '..\src\Hefesto.Scheduled.pas',
  Hefesto.Periodic in '..\src\Hefesto.Periodic.pas',
  Hefesto.Batch in '..\src\Hefesto.Batch.pas',
  Hefesto.Executor in '..\src\Hefesto.Executor.pas',
  Hefesto.WorkerPool in '..\src\Hefesto.WorkerPool.pas',
  Hefesto.Server in '..\src\Hefesto.Server.pas';

type
  TCountingHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    Calls: Integer;
    LastBody: string;

    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TFailingHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TConcurrencyProbeHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FLock: TCriticalSection;
    FActiveCount: Integer;
    FMaxActiveCount: Integer;
  public
    constructor Create;
    destructor Destroy; override;

    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
    function MaxActiveCount: Integer;
  end;

  TFlakyPublishQueue = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueuePublisher)
  private
    FInner: THefestoInMemoryQueueAdapter;
    FFailuresRemaining: Integer;
  public
    constructor Create(const AName: string; const AFailuresRemaining: Integer);
    function Name: string;
    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure Publish(const ARequest: THefestoPublishRequest);
    function PendingCount: Integer;
    function AckedCount: Integer;
  end;

  TLeaseAwareQueue = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueueLeaseManager)
  private
    FInner: THefestoInMemoryQueueAdapter;
    FRenewals: Integer;
  public
    constructor Create(const AName: string);
    function Name: string;
    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure RenewVisibility(
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer);
    function EnqueueWithAttributes(
      const AAction, ABody: string;
      const AAttributes: TStrings;
      const AAttempts: Integer = 0): TLeaseAwareQueue;
    function PendingCount: Integer;
    function AckedCount: Integer;
    function Renewals: Integer;
  end;

  TSlowHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    Calls: Integer;
    DelayMs: Integer;
    constructor Create(const ADelayMs: Integer);
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TActionCountingHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FAction: string;
  public
    Calls: Integer;
    LastBody: string;
    constructor Create(const AAction: string);
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TFetchCountingQueue = class(TInterfacedObject, IHefestoQueueAdapter, IHefestoQueuePublisher)
  private
    FName: string;
    FFetchCount: Integer;
    FFetchSequence: TStrings;
  public
    constructor Create(const AName: string; const AFetchSequence: TStrings = nil);
    function Name: string;
    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure Publish(const ARequest: THefestoPublishRequest);
    function FetchCount: Integer;
  end;

  TInMemoryPostgresBackend = class(TInterfacedObject, IHefestoPostgresStateStoreBackend)
  private
    FEntries: TStrings;
    function EntryName(const ATableName, AKey: string): string;
    function SerializeEntry(
      const AEntry: THefestoPostgresStateEntry): string;
    function DeserializeEntry(
      const ATableName, AKey, AValue: string): THefestoPostgresStateEntry;
  public
    constructor Create;
    destructor Destroy; override;
    function GetEntry(
      const ATableName, AKey: string;
      out AEntry: THefestoPostgresStateEntry): Boolean;
    procedure PutEntry(
      const ATableName: string;
      const AEntry: THefestoPostgresStateEntry);
    procedure DeleteEntry(const ATableName, AKey: string);
    function ListEntries(
      const ATableName, APrefix: string): TArray<THefestoPostgresStateEntry>;
    function TryPutIfAbsentOrExpired(
      const ATableName: string;
      const AEntry: THefestoPostgresStateEntry;
      const AReferenceTime: TDateTime): Boolean;
  end;

procedure AssertTrue(const ACondition: Boolean; const AMessage: string);
begin
  if not ACondition then
    raise Exception.Create('Assertion failed: ' + AMessage);
end;

procedure TestSuccessAcksJob;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LQueue.Enqueue('email', '{"id":1}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 1, 'expected one processed job');
  AssertTrue(LHandler.Calls = 1, 'handler should be called once');
  AssertTrue(LHandler.LastBody = '{"id":1}', 'handler should receive body');
  AssertTrue(LQueue.AckedCount = 1, 'successful job should be acked');
  AssertTrue(LQueue.NackedCount = 0, 'successful job should not be nacked');
end;

procedure TestFailureNacksJob;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LQueue.Enqueue('fail', '{"id":2}');

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .RetryPolicy(THefestoSimpleRetryPolicy.New(3, 10))
    .RegisterHandler('fail', TFailingHandler.Create);

  AssertTrue(LServer.RunOnce = 1, 'expected one processed job');
  AssertTrue(LQueue.AckedCount = 0, 'failed job should not be acked');
  AssertTrue(LQueue.NackedCount = 1, 'failed job should be nacked for retry');
end;

procedure TestSqsAdapterNormalizesOperationalQueueReference;
var
  LQueueUrl: string;
begin
  LQueueUrl := THefestoSqsQueueAdapter.NormalizeQueueUrl(
    '159521004132/docfiscall-nfse-dev-debug',
    'sa-east-1');
  AssertTrue(
    LQueueUrl = 'https://sqs.sa-east-1.amazonaws.com/159521004132/docfiscall-nfse-dev-debug',
    'adapter should normalize account-id queue references');

  LQueueUrl := THefestoSqsQueueAdapter.NormalizeQueueUrl(
    'https://sqs.us-east-1.amazonaws.com/159521004132/docfiscall-report-dev',
    'sa-east-1');
  AssertTrue(
    LQueueUrl = 'https://sqs.us-east-1.amazonaws.com/159521004132/docfiscall-report-dev',
    'adapter should preserve explicit queue URLs');
end;

procedure TestRunProcessesBatchAndStopsWhenIdle;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LQueue.Enqueue('email', '{"id":1}');
  LQueue.Enqueue('email', '{"id":2}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(2)
    .IdleDelayMs(0)
    .StopWhenIdle
    .RegisterHandler('email', LHandler);

  LServer.Run;

  AssertTrue(LHandler.Calls = 2, 'run should process both jobs');
  AssertTrue(LQueue.AckedCount = 2, 'run should ack both jobs');
  AssertTrue(LQueue.PendingCount = 0, 'queue should be empty');
end;

procedure TestRunMaxCyclesStops;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LQueue.Enqueue('email', '{"id":1}');
  LQueue.Enqueue('email', '{"id":2}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(1)
    .MaxCycles(1)
    .RegisterHandler('email', LHandler);

  LServer.Run;

  AssertTrue(LHandler.Calls = 1, 'max cycles should stop after one cycle');
  AssertTrue(LQueue.AckedCount = 1, 'only one job should be acked');
  AssertTrue(LQueue.PendingCount = 1, 'one job should remain pending');
end;

procedure TestInMemoryStateStore;
var
  LStore: IHefestoStateStore;
begin
  LStore := THefestoInMemoryStateStore.New;
  AssertTrue(not LStore.Exists('job:1'), 'state store should start empty');

  LStore.Put('job:1', 'running');
  AssertTrue(LStore.Exists('job:1'), 'put should create the key');
  AssertTrue(LStore.Get('job:1') = 'running', 'get should return stored value');

  AssertTrue(not LStore.TryPutIfAbsent('job:1', 'other'), 'existing key should not be replaced');
  AssertTrue(LStore.TryPutIfAbsent('job:2', 'queued'), 'missing key should be inserted');
  AssertTrue(LStore.Get('job:2') = 'queued', 'new key should be persisted');

  LStore.Delete('job:1');
  AssertTrue(not LStore.Exists('job:1'), 'delete should remove the key');
end;

procedure TestPostgresStateStoreCrudContract;
var
  LStore: IHefestoStateStore;
begin
  LStore := THefestoPostgresStateStore.New
    .Backend(TInMemoryPostgresBackend.Create)
    .TableName('sidekiq_state_test');

  AssertTrue(not LStore.Exists('job:pg:1'), 'postgres store should start empty');
  LStore.Put('job:pg:1', 'running');
  AssertTrue(LStore.Exists('job:pg:1'), 'postgres store should persist keys');
  AssertTrue(LStore.Get('job:pg:1') = 'running', 'postgres store should return stored value');
  AssertTrue(LStore.TryPutIfAbsent('job:pg:2', 'queued'), 'postgres store should insert missing key');
  AssertTrue(not LStore.TryPutIfAbsent('job:pg:2', 'other'), 'postgres store should reject existing key');
  LStore.Delete('job:pg:1');
  AssertTrue(not LStore.Exists('job:pg:1'), 'postgres store should delete persisted keys');
end;

procedure TestPostgresStateStoreTreatsExpiredKeyAsMissing;
var
  LStore: IHefestoStateStore;
begin
  LStore := THefestoPostgresStateStore.New
    .Backend(TInMemoryPostgresBackend.Create);

  LStore.Put('job:pg:expired', '1', 1);
  TThread.Sleep(1200);

  AssertTrue(LStore.Get('job:pg:expired') = EmptyStr, 'expired postgres key should read as missing');
  AssertTrue(not LStore.Exists('job:pg:expired'), 'expired postgres key should not exist');
end;

procedure TestPostgresStateStoreListKeysIgnoresExpired;
var
  LStore: IHefestoStateStore;
  LKeys: TArray<string>;
begin
  LStore := THefestoPostgresStateStore.New
    .Backend(TInMemoryPostgresBackend.Create);

  LStore.Put('job:pg:alive', '1', 10);
  LStore.Put('job:pg:expired', '1', 1);
  TThread.Sleep(1200);

  LKeys := LStore.ListKeys('job:pg:');
  AssertTrue(Length(LKeys) = 1, 'postgres list keys should purge expired entries');
  AssertTrue(LKeys[0] = 'job:pg:alive', 'postgres list keys should keep only non-expired key');
end;

procedure TestPostgresStateStoreTryPutIfAbsentReplacesExpired;
var
  LStore: IHefestoStateStore;
begin
  LStore := THefestoPostgresStateStore.New
    .Backend(TInMemoryPostgresBackend.Create);

  LStore.Put('job:pg:claim', 'old', 1);
  TThread.Sleep(1200);

  AssertTrue(
    LStore.TryPutIfAbsent('job:pg:claim', 'new', 10),
    'postgres store should claim expired key');
  AssertTrue(
    LStore.Get('job:pg:claim') = 'new',
    'postgres store should overwrite expired key when claiming');
end;

procedure TestInMemoryStateStoreExpiredKeyDoesNotDeadlock;
var
  LStore: IHefestoStateStore;
  LEvent: TEvent;
  LExists: Boolean;
begin
  LStore := THefestoInMemoryStateStore.New;
  LStore.Put('job:expired', '1', 1);
  TThread.Sleep(1100);
  LEvent := TEvent.Create(nil, True, False, '');
  try
    TTask.Run(
      procedure
      begin
        LExists := LStore.Exists('job:expired');
        LEvent.SetEvent;
      end);
    AssertTrue(
      LEvent.WaitFor(1000) = wrSignaled,
      'expired key lookup should not deadlock the in-memory state store');
    AssertTrue(not LExists, 'expired key should be purged during exists lookup');
  finally
    LEvent.Free;
  end;
end;

procedure TestInMemoryStateStoreListKeysPurgesExpiredWithoutDeadlock;
var
  LStore: IHefestoStateStore;
  LEvent: TEvent;
  LKeys: TArray<string>;
begin
  LStore := THefestoInMemoryStateStore.New;
  LStore.Put('job:alive', '1', 10);
  LStore.Put('job:expired', '1', 1);
  TThread.Sleep(1100);
  LEvent := TEvent.Create(nil, True, False, '');
  try
    TTask.Run(
      procedure
      begin
        LKeys := LStore.ListKeys('job:');
        LEvent.SetEvent;
      end);
    AssertTrue(
      LEvent.WaitFor(1000) = wrSignaled,
      'ListKeys should not deadlock while purging expired entries');
    AssertTrue(Length(LKeys) = 1, 'ListKeys should only keep the non-expired key');
    AssertTrue(LKeys[0] = 'job:alive', 'ListKeys should purge expired keys before returning');
  finally
    LEvent.Free;
  end;
end;

procedure TestInMemoryLockProvider;
var
  LStore: IHefestoStateStore;
  LProvider: IHefestoLockProvider;
  LHandle1: IHefestoLockHandle;
  LHandle2: IHefestoLockHandle;
begin
  LStore := THefestoInMemoryStateStore.New;
  LProvider := THefestoInMemoryLockProvider.New(LStore);

  LHandle1 := LProvider.TryAcquire('nfse:123', 30);
  AssertTrue(Assigned(LHandle1), 'first lock acquire should succeed');
  AssertTrue(LStore.Exists('lock:nfse:123'), 'lock should be persisted in state store');

  LHandle2 := LProvider.TryAcquire('nfse:123', 30);
  AssertTrue(not Assigned(LHandle2), 'second lock acquire should fail while lock is held');

  LHandle1.Release;
  AssertTrue(not LStore.Exists('lock:nfse:123'), 'release should remove the lock key');

  LHandle2 := LProvider.TryAcquire('nfse:123', 30);
  AssertTrue(Assigned(LHandle2), 'lock should be acquirable again after release');
end;

procedure TestIdempotencySkipsRepeatedLogicalJob;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
  LAttributes: TStringList;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LHandler := TCountingHandler.Create;
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'nota:123';
    LAttributes.Values[THefestoJobAttribute.LockKey] := 'nota:123';
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .BatchSize(2)
      .RegisterHandler('email', LHandler);

    AssertTrue(LServer.RunOnce = 2, 'server should fetch both jobs');
    AssertTrue(LHandler.Calls = 1, 'idempotency should skip repeated logical job');
    AssertTrue(LQueue.AckedCount = 2, 'both jobs should be acked');
    AssertTrue(LQueue.NackedCount = 0, 'repeated completed job should not be nacked');
  finally
    LAttributes.Free;
  end;
end;

procedure TestRunProcessesMultipleQueues;
var
  LQueue1: THefestoInMemoryQueueAdapter;
  LQueue2: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue1 := THefestoInMemoryQueueAdapter.New('critical');
  LQueue2 := THefestoInMemoryQueueAdapter.New('default');
  LQueue1.Enqueue('email', '{"id":1}');
  LQueue2.Enqueue('email', '{"id":2}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue1)
    .UseQueue(LQueue2)
    .Concurrency(2)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 2, 'server should aggregate jobs from all configured queues');
  AssertTrue(LHandler.Calls = 2, 'handler should process jobs from both queues');
  AssertTrue(LQueue1.AckedCount = 1, 'first queue should ack its job');
  AssertTrue(LQueue2.AckedCount = 1, 'second queue should ack its job');
end;

procedure TestPausedQueueSkipsFetchUntilResume;
var
  LCriticalQueue: THefestoInMemoryQueueAdapter;
  LDefaultQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LCriticalQueue := THefestoInMemoryQueueAdapter.New('critical');
  LDefaultQueue := THefestoInMemoryQueueAdapter.New('default');
  LCriticalQueue.Enqueue('email', '{"id":801}');
  LDefaultQueue.Enqueue('email', '{"id":802}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LCriticalQueue)
    .UseQueue(LDefaultQueue)
    .PauseQueue(LCriticalQueue.Name)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.QueuePaused(LCriticalQueue.Name), 'critical queue should be marked as paused');
  AssertTrue(LServer.RunOnce = 1, 'paused queue should be skipped while active queues keep running');
  AssertTrue(LHandler.Calls = 1, 'only the unpaused queue job should execute');
  AssertTrue(LCriticalQueue.AckedCount = 0, 'paused queue job should not be acked yet');
  AssertTrue(LDefaultQueue.AckedCount = 1, 'unpaused queue job should be processed');

  LServer.ResumeQueue(LCriticalQueue.Name);
  AssertTrue(not LServer.QueuePaused(LCriticalQueue.Name), 'critical queue should be resumed');
  AssertTrue(LServer.RunOnce = 1, 'resumed queue should fetch pending jobs again');
  AssertTrue(LHandler.Calls = 2, 'resumed queue job should execute after resume');
  AssertTrue(LCriticalQueue.AckedCount = 1, 'resumed queue job should eventually be acked');
end;

procedure TestRunOnceSkipsFetchWhileDraining;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LQueue.Enqueue('email', '{"id":901}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .RegisterHandler('email', LHandler)
    .BeginRollingRestart;

  AssertTrue(LServer.IsDraining, 'server should expose draining state after rolling restart begins');
  AssertTrue(LServer.RunOnce = 0, 'draining server should skip fetches during rolling restart');
  AssertTrue(LHandler.Calls = 0, 'draining server should not execute handlers');
  AssertTrue(LQueue.PendingCount = 1, 'draining server should leave queued jobs untouched');
  AssertTrue(LServer.ActiveWorkers = 0, 'draining server should not report active workers without execution');

  LServer.ResumeAfterRollingRestart;
  AssertTrue(not LServer.IsDraining, 'server should allow resuming after rolling restart is cancelled');
  AssertTrue(LServer.RunOnce = 1, 'resumed server should fetch pending jobs again');
  AssertTrue(LHandler.Calls = 1, 'resumed server should execute the pending job');
end;

procedure TestRollingRestartStopsAfterCurrentBusyCycle;
var
  LStore: IHefestoStateStore;
  LQueue: TLeaseAwareQueue;
  LHandler: TSlowHandler;
  LServer: IHefestoServer;
  LAttributes: TStringList;
  LTask: ITask;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue := TLeaseAwareQueue.Create('critical');
  LHandler := TSlowHandler.Create(2500);
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'rolling:restart:1';
    LAttributes.Values[THefestoJobAttribute.LockKey] := 'rolling:restart:1';
    LAttributes.Values[THefestoJobAttribute.ServerLeaseTtlSeconds] := '2';
    LAttributes.Values[THefestoJobAttribute.HeartbeatIntervalSeconds] := '1';
    LQueue.EnqueueWithAttributes('email', '{"id":902}', LAttributes);
    LQueue.EnqueueWithAttributes('email', '{"id":903}', nil);

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .StateStore(LStore)
      .BatchSize(1)
      .IdleDelayMs(0)
      .VisibilityTimeout(2)
      .RegisterHandler('email', LHandler);

    LTask := TTask.Run(
      procedure
      begin
        LServer.Run;
      end);

    TThread.Sleep(300);

    AssertTrue(LServer.ActiveWorkers > 0, 'rolling restart should expose active workers while the current job is still running');
    LServer.BeginRollingRestart;
    AssertTrue(LServer.IsDraining, 'rolling restart should mark the server as draining');

    LTask.Wait;

    AssertTrue(LHandler.Calls = 1, 'rolling restart should finish the in-flight job but stop before the next fetch');
    AssertTrue(LQueue.AckedCount = 1, 'rolling restart should still ack the completed in-flight job');
    AssertTrue(LQueue.PendingCount = 1, 'rolling restart should leave the next queued job pending');
    AssertTrue(LQueue.Renewals > 0, 'rolling restart should preserve lease renewal while the in-flight job drains');
    AssertTrue(LServer.ActiveWorkers = 0, 'server should report zero active workers after draining completes');
  finally
    LAttributes.Free;
  end;
end;

procedure TestLeaderElectionFailoverBetweenServers;
var
  LStore: IHefestoStateStore;
  LQueue1: THefestoInMemoryQueueAdapter;
  LQueue2: THefestoInMemoryQueueAdapter;
  LServer1: IHefestoServer;
  LServer2: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue1 := THefestoInMemoryQueueAdapter.New('critical');
  LQueue2 := THefestoInMemoryQueueAdapter.New('critical');

  LServer1 := THefestoServer.New
    .UseQueue(LQueue1)
    .StateStore(LStore)
    .LeaderName('cluster-a')
    .LeaderLeaseTtlSeconds(1)
    .UseLeaderElection;

  LServer2 := THefestoServer.New
    .UseQueue(LQueue2)
    .StateStore(LStore)
    .LeaderName('cluster-a')
    .LeaderLeaseTtlSeconds(1)
    .UseLeaderElection;

  AssertTrue(LServer1.RunOnce = 0, 'first server should acquire leadership without jobs');
  AssertTrue(LServer1.IsLeader, 'first server should become leader');

  AssertTrue(LServer2.RunOnce = 0, 'second server should remain follower while lease is active');
  AssertTrue(not LServer2.IsLeader, 'second server should not steal leadership before lease expiry');

  TThread.Sleep(1100);

  AssertTrue(LServer2.RunOnce = 0, 'second server should be able to take over after lease expiry');
  AssertTrue(LServer2.IsLeader, 'second server should acquire leadership after failover window');
  AssertTrue(not LServer1.IsLeader, 'first server should observe leadership expiry after failover');
end;

procedure TestFollowerCannotPromoteScheduledEntries;
var
  LStore: IHefestoStateStore;
  LScheduledStore: IHefestoScheduledStore;
  LQueue: THefestoInMemoryQueueAdapter;
  LLeaderServer: IHefestoServer;
  LFollowerServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LScheduledStore := THefestoInMemoryScheduledStore.New;
  LQueue := THefestoInMemoryQueueAdapter.New('critical');

  LLeaderServer := THefestoServer.New
    .UseQueue(LQueue)
    .StateStore(LStore)
    .ScheduledStore(LScheduledStore)
    .LeaderName('cluster-b')
    .LeaderLeaseTtlSeconds(5)
    .UseLeaderElection;

  LFollowerServer := THefestoServer.New
    .UseQueue(LQueue)
    .StateStore(LStore)
    .ScheduledStore(LScheduledStore)
    .LeaderName('cluster-b')
    .LeaderLeaseTtlSeconds(5)
    .UseLeaderElection;

  AssertTrue(LLeaderServer.PromoteScheduled = 0, 'leader bootstrap should not fail on empty schedule');
  AssertTrue(LLeaderServer.IsLeader, 'leader bootstrap should acquire the leadership lease');

  LLeaderServer.EnqueueAt(
    LQueue.Name,
    'email',
    '{"id":804}',
    IncMilliSecond(Now, -1));

  AssertTrue(LFollowerServer.PromoteScheduled = 0, 'follower should not promote scheduled entries while lease is held');
  AssertTrue(LQueue.PendingCount = 0, 'follower promotion attempt should keep queue unchanged');

  AssertTrue(LLeaderServer.PromoteScheduled = 1, 'leader should promote the due scheduled entry');
  AssertTrue(LQueue.PendingCount = 1, 'leader promotion should materialize the scheduled job into the queue');
end;

procedure TestNoAckPolicySkipsQueueAck;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LQueue.Enqueue('email', '{"id":3}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .AckPolicy(THefestoNoAckPolicy.New)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 1, 'server should process the job with custom ack policy');
  AssertTrue(LHandler.Calls = 1, 'handler should still execute');
  AssertTrue(LQueue.AckedCount = 0, 'custom no-ack policy should suppress ack');
end;

procedure TestQueueConcurrencyLimitSerializesOneQueue;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TConcurrencyProbeHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LQueue.Enqueue('email', '{"id":1}');
  LQueue.Enqueue('email', '{"id":2}');
  LHandler := TConcurrencyProbeHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(2)
    .Concurrency(2)
    .QueueConcurrency('critical', 1)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 2, 'server should fetch both jobs from the same queue');
  AssertTrue(LHandler.MaxActiveCount = 1, 'queue concurrency limit should serialize jobs from the same queue');
end;

procedure TestUniqueUntilExpiredKeepsCompletedOnFailure;
var
  LStore: IHefestoStateStore;
  LQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
  LAttributes: TStringList;
  LIdem: IHefestoIdempotency;
begin
  LStore := THefestoInMemoryStateStore.New;
  LIdem := THefestoStateStoreIdempotency.New(LStore);
  LQueue := THefestoInMemoryQueueAdapter.New;
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'nfse:unique:1';
    LAttributes.Values[THefestoJobAttribute.UniqueStrategy] :=
      THefestoUniqueStrategy.ValueUntilExpired;
    LQueue.EnqueueWithAttributes('fail', '{"id":1}', LAttributes);

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .StateStore(LStore)
      .RetryPolicy(THefestoSimpleRetryPolicy.New(0, 0))
      .RegisterHandler('fail', TFailingHandler.Create);

    LServer.RunOnce;

    AssertTrue(
      LIdem.IsCompleted('nfse:unique:1'),
      'until_expired should keep idempotency key marked as completed after failure');
  finally
    LAttributes.Free;
  end;
end;

procedure TestScheduledEnqueueInPromotesOnRun;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(5)
    .RegisterHandler('email', LHandler);

  LServer.EnqueueAt(LQueue.Name, 'email', '{"id":99}', IncMilliSecond(Now, -1));

  AssertTrue(LQueue.PendingCount = 0, 'scheduled entry should not be in queue before promotion');

  AssertTrue(LServer.RunOnce = 1, 'RunOnce should promote and execute the due entry');
  AssertTrue(LHandler.Calls = 1, 'handler should have been invoked for promoted scheduled job');
end;

procedure TestScheduledEnqueueAtFuturePromotesOnlyWhenDue;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(5)
    .RegisterHandler('email', LHandler);

  LServer.EnqueueAt(LQueue.Name, 'email', '{"id":100}', IncSecond(Now, 60));

  AssertTrue(LServer.RunOnce = 0, 'future scheduled entry should not be promoted yet');
  AssertTrue(LHandler.Calls = 0, 'future scheduled entry should not run yet');
end;

procedure TestFlushClientOutboxRespectsBudget;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LOutbox: IHefestoClientOutbox;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('default');
  LOutbox := THefestoInMemoryClientOutbox.New;
  LOutbox.Save(MakePublishRequest(LQueue.Name, 'email', '{"id":1}', 0, nil));
  LOutbox.Save(MakePublishRequest(LQueue.Name, 'email', '{"id":2}', 0, nil));
  LOutbox.Save(MakePublishRequest(LQueue.Name, 'email', '{"id":3}', 0, nil));

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .ClientOutbox(LOutbox)
    .ClientOutboxBudget(1);

  AssertTrue(LServer.FlushClientOutbox = 1, 'outbox flush should respect configured budget');
  AssertTrue(LOutbox.Count = 2, 'outbox should keep pending entries for future cycles');
  AssertTrue(LQueue.PendingCount = 1, 'only one publish should be materialized into the queue');
end;

procedure TestActionConcurrencyLimitSerializesOneAction;
var
  LQueue1: THefestoInMemoryQueueAdapter;
  LQueue2: THefestoInMemoryQueueAdapter;
  LHandler: TConcurrencyProbeHandler;
  LServer: IHefestoServer;
begin
  LQueue1 := THefestoInMemoryQueueAdapter.New('critical');
  LQueue2 := THefestoInMemoryQueueAdapter.New('default');
  LQueue1.Enqueue('email', '{"id":1}');
  LQueue2.Enqueue('email', '{"id":2}');
  LHandler := TConcurrencyProbeHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue1)
    .UseQueue(LQueue2)
    .BatchSize(2)
    .Concurrency(2)
    .ActionConcurrency('email', 1)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 2, 'server should fetch both jobs with same action');
  AssertTrue(LHandler.MaxActiveCount = 1, 'action concurrency limit should serialize jobs of the same action');
end;

procedure TestRateLimitRetriesWhenBucketIsExhausted;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
  LAttributes: TStringList;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LHandler := TCountingHandler.Create;
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.RateLimitKey] := 'tenant:nfse:emissao';
    LAttributes.Values[THefestoJobAttribute.RateLimitCapacity] := '1';
    LAttributes.Values[THefestoJobAttribute.RateLimitIntervalSeconds] := '60';
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);
    LQueue.EnqueueWithAttributes('email', '{"id":2}', LAttributes);

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .BatchSize(2)
      .RegisterHandler('email', LHandler);

    AssertTrue(LServer.RunOnce = 2, 'server should fetch both jobs under rate limit test');
    AssertTrue(LHandler.Calls = 1, 'second job should be retried when token bucket is exhausted');
    AssertTrue(LQueue.AckedCount = 1, 'first job should be acked');
    AssertTrue(LQueue.NackedCount = 1, 'rate-limited job should be nacked for retry');
  finally
    LAttributes.Free;
  end;
end;

procedure TestClientReliabilityStoresFailedPublishAndFlushesLater;
var
  LQueue: TFlakyPublishQueue;
  LOutbox: IHefestoClientOutbox;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := TFlakyPublishQueue.Create('reliable', 1);
  LOutbox := THefestoInMemoryClientOutbox.New;
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .ClientOutbox(LOutbox)
    .RegisterHandler('email', LHandler);

  LServer.Enqueue(LQueue.Name, 'email', '{"id":200}');

  AssertTrue(LOutbox.Count = 1, 'failed publish should be stored in the client outbox');
  AssertTrue(LQueue.PendingCount = 0, 'failed publish should not reach the queue immediately');
  AssertTrue(LServer.FlushClientOutbox = 1, 'flush should replay one pending publish');
  AssertTrue(LOutbox.Count = 0, 'outbox should be empty after successful replay');
  AssertTrue(LQueue.PendingCount = 1, 'replayed publish should reach the queue');
  AssertTrue(LServer.RunOnce = 1, 'replayed publish should be processed normally');
  AssertTrue(LHandler.Calls = 1, 'handler should run for the replayed publish');
  AssertTrue(LQueue.AckedCount = 1, 'replayed publish should still ack after processing');
end;

procedure TestExpiredQueuedJobIsDiscardedBeforeExecution;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
  LAttributes: TStringList;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LHandler := TCountingHandler.Create;
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.ExpiresInSeconds] := '1';

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .RegisterHandler('email', LHandler);

    LServer.Enqueue(LQueue.Name, 'email', '{"id":803}', LAttributes);
    TThread.Sleep(1100);

    AssertTrue(LQueue.PendingCount = 1, 'expired job should still be waiting in the queue before fetch');
    AssertTrue(LServer.RunOnce = 1, 'expired job should be fetched and discarded in the same cycle');
    AssertTrue(LHandler.Calls = 0, 'expired job must not reach the handler');
    AssertTrue(LQueue.AckedCount = 1, 'expired job should be acked through discard');
    AssertTrue(LQueue.PendingCount = 0, 'expired job should leave the queue after discard');
  finally
    LAttributes.Free;
  end;
end;

procedure TestMetricsTelemetryEmitsStatsDPayloads;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LSuccessHandler: TCountingHandler;
  LTransport: THefestoInMemoryMetricTransport;
  LTelemetry: IHefestoTelemetry;
  LServer: IHefestoServer;
  LPayload: string;
  LHasStarted: Boolean;
  LHasSuccess: Boolean;
  LHasDuration: Boolean;
  LHasFetch: Boolean;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LQueue.Enqueue('email', '{"id":901}');
  LSuccessHandler := TCountingHandler.Create;
  LTransport := THefestoInMemoryMetricTransport.New;
  LTelemetry := THefestoMetricsTelemetry.New(
    THefestoStatsDMetricsSink.New(LTransport, 'sidekiq4delphi'));

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Telemetry(LTelemetry)
    .RegisterHandler('email', LSuccessHandler);

  AssertTrue(LServer.RunOnce = 1, 'metrics telemetry scenario should process one job');

  LHasStarted := False;
  LHasSuccess := False;
  LHasDuration := False;
  LHasFetch := False;
  for LPayload in LTransport.Payloads do
  begin
    if Pos('sidekiq4delphi.jobs.started:1|c', LPayload) > 0 then
      LHasStarted := True;
    if Pos('sidekiq4delphi.jobs.succeeded:1|c', LPayload) > 0 then
      LHasSuccess := True;
    if Pos('sidekiq4delphi.jobs.duration_ms:', LPayload) > 0 then
      LHasDuration := True;
    if Pos('sidekiq4delphi.fetch.operations:1|c', LPayload) > 0 then
      LHasFetch := True;
  end;

  AssertTrue(LHasStarted, 'metrics telemetry should emit jobs.started counter');
  AssertTrue(LHasSuccess, 'metrics telemetry should emit jobs.succeeded counter');
  AssertTrue(LHasDuration, 'metrics telemetry should emit jobs.duration_ms timing');
  AssertTrue(LHasFetch, 'metrics telemetry should emit fetch.operations counter');
end;

procedure TestHistoricalMetricsTelemetryAggregatesExecution;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LSuccessHandler: TCountingHandler;
  LHistoryTelemetry: IHefestoTelemetry;
  LHistoricalMetrics: IHefestoHistoricalMetrics;
  LServer: IHefestoServer;
  LSnapshot: TArray<THefestoHistoricalMetricSnapshot>;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LQueue.Enqueue('email', '{"id":902}');
  LSuccessHandler := TCountingHandler.Create;
  LHistoryTelemetry := THefestoHistoricalMetricsTelemetry.New(60, 5);
  AssertTrue(
    Supports(LHistoryTelemetry, IHefestoHistoricalMetrics, LHistoricalMetrics),
    'historical telemetry should expose snapshot interface');

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Telemetry(LHistoryTelemetry)
    .RegisterHandler('email', LSuccessHandler);

  AssertTrue(LServer.RunOnce = 1, 'historical metrics scenario should process one job');

  LSnapshot := LHistoricalMetrics.Snapshot;
  AssertTrue(Length(LSnapshot) = 1, 'historical metrics should keep one current bucket');
  AssertTrue(LSnapshot[0].FetchOperations = 1, 'historical metrics should count fetch operations');
  AssertTrue(LSnapshot[0].FetchedJobs = 1, 'historical metrics should count fetched jobs');
  AssertTrue(LSnapshot[0].StartedJobs = 1, 'historical metrics should count started jobs');
  AssertTrue(LSnapshot[0].SucceededJobs = 1, 'historical metrics should count successful jobs');
  AssertTrue(LSnapshot[0].AckedJobs = 1, 'historical metrics should count acked jobs');
  AssertTrue(LSnapshot[0].AverageDurationMs >= 0, 'historical metrics should expose average duration');
end;

procedure TestServerReliabilityRenewsVisibilityForLongJob;
var
  LStore: IHefestoStateStore;
  LQueue: TLeaseAwareQueue;
  LHandler: TSlowHandler;
  LServer: IHefestoServer;
  LAttributes: TStringList;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue := TLeaseAwareQueue.Create('critical');
  LHandler := TSlowHandler.Create(2500);
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'lease:renew:1';
    LAttributes.Values[THefestoJobAttribute.LockKey] := 'lease:renew:1';
    LAttributes.Values[THefestoJobAttribute.ServerLeaseTtlSeconds] := '2';
    LAttributes.Values[THefestoJobAttribute.HeartbeatIntervalSeconds] := '1';
    LQueue.EnqueueWithAttributes('email', '{"id":501}', LAttributes);

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .StateStore(LStore)
      .VisibilityTimeout(2)
      .RegisterHandler('email', LHandler);

    AssertTrue(LServer.RunOnce = 1, 'long-running job should still be processed');
    AssertTrue(LHandler.Calls = 1, 'long-running handler should execute once');
    AssertTrue(LQueue.Renewals > 0, 'server reliability should renew visibility during long job');
    AssertTrue(
      SameText(LStore.Get('idempotency:lease:renew:1'), 'completed'),
      'successful long job should still mark idempotency as completed');
  finally
    LAttributes.Free;
  end;
end;

procedure TestServerReliabilityRecoversStaleExecution;
var
  LStore: IHefestoStateStore;
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
  LAttributes: TStringList;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LHandler := TCountingHandler.Create;
  LAttributes := TStringList.Create;
  try
    LAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'stale:execution:1';
    LAttributes.Values[THefestoJobAttribute.LockKey] := 'stale:execution:1';
    LQueue.EnqueueWithAttributes('email', '{"id":601}', LAttributes);

    LStore.Put('idempotency:stale:execution:1', 'processing', 300);
    LStore.Put('lock:stale:execution:1', 'token', 300);
    LStore.Put(
      'server_reliability:registry:stale-exec-1',
      '{"execution_id":"stale-exec-1","job_id":"job-1","queue_name":"critical","action":"email","idempotency_key":"stale:execution:1","lock_key":"stale:execution:1","lease_ttl_seconds":2,"heartbeat_interval_seconds":1,"visibility_timeout":2}',
      300);

    LServer := THefestoServer.New
      .UseQueue(LQueue)
      .StateStore(LStore)
      .RegisterHandler('email', LHandler);

    AssertTrue(LServer.RunOnce = 1, 'recovered stale execution should allow the job to be processed');
    AssertTrue(LHandler.Calls = 1, 'stale execution recovery should clear transient lock/idempotency state');
    AssertTrue(not LStore.Exists('server_reliability:registry:stale-exec-1'), 'stale lease registry entry should be removed');
  finally
    LAttributes.Free;
  end;
end;

procedure TestBatchCallbacksRunOnSuccessfulBatch;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LWorkerHandler: TCountingHandler;
  LCompleteHandler: TActionCountingHandler;
  LSuccessHandler: TActionCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LWorkerHandler := TCountingHandler.Create;
  LCompleteHandler := TActionCountingHandler.Create('batch_complete');
  LSuccessHandler := TActionCountingHandler.Create('batch_success');

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(10)
    .RegisterHandler('email', LWorkerHandler)
    .RegisterHandler('batch_complete', LCompleteHandler)
    .RegisterHandler('batch_success', LSuccessHandler);

  LServer.Batch('success-batch')
    .OnComplete(LQueue.Name, 'batch_complete', '{"kind":"complete"}')
    .OnSuccess(LQueue.Name, 'batch_success', '{"kind":"success"}')
    .Enqueue(LQueue.Name, 'email', '{"id":701}');

  AssertTrue(LQueue.PendingCount = 1, 'batch enqueue should materialize one worker job before the first cycle');
  AssertTrue(LServer.RunOnce = 1, 'batch worker should run on first cycle');
  AssertTrue(LWorkerHandler.Calls = 1, 'batch worker should execute once');
  AssertTrue(LCompleteHandler.Calls = 0, 'complete callback should wait for the next cycle');
  AssertTrue(LSuccessHandler.Calls = 0, 'success callback should wait for the next cycle');

  AssertTrue(LServer.RunOnce = 2, 'ready batch callbacks should run on the second cycle');
  AssertTrue(LCompleteHandler.Calls = 1, 'complete callback should run after batch finishes');
  AssertTrue(LSuccessHandler.Calls = 1, 'success callback should run when the batch has no failures');
  AssertTrue(LQueue.AckedCount = 3, 'worker and both callbacks should be acked');
end;

procedure TestBatchCallbacksSkipSuccessOnFinalFailure;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LCompleteHandler: TActionCountingHandler;
  LSuccessHandler: TActionCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LCompleteHandler := TActionCountingHandler.Create('batch_complete');
  LSuccessHandler := TActionCountingHandler.Create('batch_success');

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(10)
    .RetryPolicy(THefestoSimpleRetryPolicy.New(0, 0))
    .RegisterHandler('fail', TFailingHandler.Create)
    .RegisterHandler('batch_complete', LCompleteHandler)
    .RegisterHandler('batch_success', LSuccessHandler);

  LServer.Batch('failed-batch')
    .OnComplete(LQueue.Name, 'batch_complete', '{"kind":"complete"}')
    .OnSuccess(LQueue.Name, 'batch_success', '{"kind":"success"}')
    .Enqueue(LQueue.Name, 'fail', '{"id":702}');

  AssertTrue(LServer.RunOnce = 1, 'failing batch worker should still run on first cycle');
  AssertTrue(LQueue.DeadLetteredCount = 1, 'failed batch worker should be dead-lettered on final failure');

  AssertTrue(LServer.RunOnce = 1, 'only complete callback should be promoted for a failed batch');
  AssertTrue(LCompleteHandler.Calls = 1, 'complete callback should run even when the batch fails');
  AssertTrue(LSuccessHandler.Calls = 0, 'success callback should be skipped when the batch has failures');
  AssertTrue(LQueue.AckedCount = 1, 'only the complete callback should be acked after failure');
end;

type
  // Routes a specific action to a fixed target queue name.
  TActionToQueueRouter = class(TInterfacedObject, IHefestoJobRouter)
  private
    FAction: string;
    FTargetQueue: string;
  public
    constructor Create(const AAction, ATargetQueue: string);
    function Route(
      const AQueueName: string;
      const AAction: string;
      const AAttributes: TArray<TPair<string, string>>): string;
  end;

  // Routes based on a 'target_queue' attribute set by the publisher.
  TAttributeQueueRouter = class(TInterfacedObject, IHefestoJobRouter)
  public
    function Route(
      const AQueueName: string;
      const AAction: string;
      const AAttributes: TArray<TPair<string, string>>): string;
  end;

  TCountingServerMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
  public
    Before: Integer;
    After: Integer;
    procedure Call(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ANext: THefestoNextProc);
  end;

  TCountingClientMiddleware = class(TInterfacedObject, IHefestoClientMiddleware)
  public
    Invocations: Integer;
    procedure Call(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const ANext: THefestoNextProc);
  end;

procedure TCountingServerMiddleware.Call(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ANext: THefestoNextProc);
begin
  Inc(Before);
  ANext();
  Inc(After);
end;

procedure TCountingClientMiddleware.Call(
  const AQueueName: string;
  const AAction: string;
  const ABody: string;
  const AAttributes: TStrings;
  const ANext: THefestoNextProc);
begin
  Inc(Invocations);
  ANext();
end;

procedure TestJobRouterPassThroughLeavesQueueUnchanged;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('nfs:geral');
  LQueue.Enqueue('email', '{"id":1}');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 1, 'pass-through router should not affect job routing');
  AssertTrue(LHandler.Calls = 1, 'handler should receive the job on the original queue');
  AssertTrue(LQueue.AckedCount = 1, 'job should be acked on the original queue');
end;

procedure TestJobRouterRedirectsJobToTargetQueue;
var
  LSourceQueue: THefestoInMemoryQueueAdapter;
  LTargetQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LSourceQueue := THefestoInMemoryQueueAdapter.New('nfs:geral');
  LTargetQueue := THefestoInMemoryQueueAdapter.New('nfs:seq:fortaleza');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LSourceQueue)
    .UseQueue(LTargetQueue)
    .UseJobRouter(TActionToQueueRouter.Create('emit_nfs', 'nfs:seq:fortaleza'))
    .RegisterHandler('emit_nfs', LHandler);

  LServer.Enqueue('nfs:geral', 'emit_nfs', '{"nota":5}');

  AssertTrue(LSourceQueue.PendingCount = 0, 'router should redirect job away from source queue');
  AssertTrue(LTargetQueue.PendingCount = 1, 'router should place job in sequential queue');

  AssertTrue(LServer.RunOnce = 1, 'redirected job should be processed normally');
  AssertTrue(LHandler.Calls = 1, 'handler should run for the job in the target queue');
  AssertTrue(LTargetQueue.AckedCount = 1, 'job in sequential queue should be acked after processing');
end;

procedure TestJobRouterRoutesViaJobAttribute;
var
  LGeneralQueue: THefestoInMemoryQueueAdapter;
  LSeqQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
  LAttrs: TStringList;
begin
  LGeneralQueue := THefestoInMemoryQueueAdapter.New('nfs:geral');
  LSeqQueue     := THefestoInMemoryQueueAdapter.New('nfs:seq:campinas');
  LHandler      := TCountingHandler.Create;
  LAttrs        := TStringList.Create;
  try
    LServer := THefestoServer.New
      .UseQueue(LGeneralQueue)
      .UseQueue(LSeqQueue)
      .UseJobRouter(TAttributeQueueRouter.Create)
      .BatchSize(2)
      .RegisterHandler('emit_nfs', LHandler);

    // Job sem atributo → fica na fila geral
    LServer.Enqueue('nfs:geral', 'emit_nfs', '{"nota":1}');
    AssertTrue(LGeneralQueue.PendingCount = 1, 'job without target_queue should stay on general queue');
    AssertTrue(LSeqQueue.PendingCount = 0, 'sequential queue should be empty for untagged job');

    // Job com atributo → roteado para fila sequencial
    LAttrs.Values['target_queue'] := 'nfs:seq:campinas';
    LServer.Enqueue('nfs:geral', 'emit_nfs', '{"nota":2}', LAttrs);
    AssertTrue(LSeqQueue.PendingCount = 1, 'job with target_queue attribute should be routed to sequential queue');
    AssertTrue(LGeneralQueue.PendingCount = 1, 'general queue should not receive the routed job');

    AssertTrue(LServer.RunOnce = 2, 'both jobs should be processed');
    AssertTrue(LHandler.Calls = 2, 'handler should run for jobs from both queues');
  finally
    LAttrs.Free;
  end;
end;

procedure TestServerMiddlewareWrapsHandler;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LMiddleware: TCountingServerMiddleware;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LQueue.Enqueue('email', '{"id":1}');
  LHandler := TCountingHandler.Create;
  LMiddleware := TCountingServerMiddleware.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .UseServerMiddleware(LMiddleware)
    .RegisterHandler('email', LHandler);

  AssertTrue(LServer.RunOnce = 1, 'server should process one job');
  AssertTrue(LHandler.Calls = 1, 'handler should run through middleware chain');
  AssertTrue(LMiddleware.Before = 1, 'middleware should record one before-invocation');
  AssertTrue(LMiddleware.After = 1, 'middleware should record one after-invocation');
end;

procedure TestClientMiddlewareInvokedOnEnqueueIn;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LMiddleware: TCountingClientMiddleware;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;
  LHandler := TCountingHandler.Create;
  LMiddleware := TCountingClientMiddleware.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .UseClientMiddleware(LMiddleware)
    .RegisterHandler('email', LHandler);

  LServer.EnqueueIn(LQueue.Name, 'email', '{"id":1}', 0);

  AssertTrue(LMiddleware.Invocations = 1, 'client middleware should be invoked once for EnqueueIn');
  AssertTrue(LServer.RunOnce = 1, 'promoted job should run');
end;

procedure TestPromoteScheduledRespectsBudget;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
  LPastDue: TDateTime;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('default');
  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .ScheduledPromotionBudget(1);

  LPastDue := IncSecond(Now, -1);
  LServer.EnqueueAt(LQueue.Name, 'email', '{"id":1}', LPastDue);
  LServer.EnqueueAt(LQueue.Name, 'email', '{"id":2}', LPastDue);
  LServer.EnqueueAt(LQueue.Name, 'email', '{"id":3}', LPastDue);

  AssertTrue(LServer.PromoteScheduled = 1, 'scheduled promotion should respect budget');
  AssertTrue(LQueue.PendingCount = 1, 'only one due entry should be promoted in the first cycle');
  AssertTrue(LServer.PromoteScheduled = 1, 'remaining due entries should stay for later cycles');
  AssertTrue(LQueue.PendingCount = 2, 'second promotion should add only one more job');
end;

procedure TestPromoteBatchCallbacksRespectsBudget;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LWorkerHandler: TCountingHandler;
  LCompleteHandler: TActionCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  LWorkerHandler := TCountingHandler.Create;
  LCompleteHandler := TActionCountingHandler.Create('batch_complete');

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(10)
    .BatchCallbackPromotionBudget(1)
    .RegisterHandler('email', LWorkerHandler)
    .RegisterHandler('batch_complete', LCompleteHandler);

  LServer.Batch('batch-1')
    .OnComplete(LQueue.Name, 'batch_complete', '{"batch":1}')
    .Enqueue(LQueue.Name, 'email', '{"id":1}');
  LServer.Batch('batch-2')
    .OnComplete(LQueue.Name, 'batch_complete', '{"batch":2}')
    .Enqueue(LQueue.Name, 'email', '{"id":2}');
  LServer.Batch('batch-3')
    .OnComplete(LQueue.Name, 'batch_complete', '{"batch":3}')
    .Enqueue(LQueue.Name, 'email', '{"id":3}');

  AssertTrue(LServer.RunOnce = 3, 'first cycle should execute all worker jobs');
  AssertTrue(LCompleteHandler.Calls = 0, 'callbacks should wait for later promotion');

  AssertTrue(LServer.RunOnce = 1, 'second cycle should promote only one batch callback');
  AssertTrue(LCompleteHandler.Calls = 1, 'only one callback should run with budget 1');
  AssertTrue(LServer.RunOnce = 1, 'third cycle should promote the next callback');
  AssertTrue(LCompleteHandler.Calls = 2, 'second callback should remain for later cycle');
  AssertTrue(LServer.RunOnce = 1, 'fourth cycle should promote the last callback');
  AssertTrue(LCompleteHandler.Calls = 3, 'all callbacks should eventually run');
end;

procedure TestPeriodicJobRunsOncePerWindow;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('periodic');
  LHandler := TCountingHandler.Create;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(5)
    .RegisterHandler('email', LHandler)
    .RegisterPeriodic('heartbeat', '* * * * *', LQueue.Name, 'email', '{"kind":"heartbeat"}');

  AssertTrue(LServer.RunOnce = 1, 'periodic job should materialize and run on first cycle');
  AssertTrue(LHandler.Calls = 1, 'periodic handler should run once');
  AssertTrue(LServer.RunOnce = 0, 'same periodic window should not enqueue duplicates');
  AssertTrue(LHandler.Calls = 1, 'periodic deduplication should keep a single execution per minute');
end;

procedure TestPeriodicJobRespectsCronMismatch;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LHandler: TCountingHandler;
  LServer: IHefestoServer;
  LOtherHour: Integer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('periodic');
  LHandler := TCountingHandler.Create;
  LOtherHour := (HourOf(Now) + 1) mod 24;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .BatchSize(5)
    .RegisterHandler('email', LHandler)
    .RegisterPeriodic(
      'nightly',
      Format('* %d * * *', [LOtherHour]),
      LQueue.Name,
      'email',
      '{"kind":"nightly"}');

  AssertTrue(LServer.RunOnce = 0, 'periodic cron mismatch should not enqueue work');
  AssertTrue(LHandler.Calls = 0, 'handler should not run when cron does not match current window');
end;

procedure TestPromotePeriodicRespectsBudget;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('periodic');
  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .PeriodicPromotionBudget(1)
    .RegisterPeriodic('periodic-1', '* * * * *', LQueue.Name, 'email', '{"id":1}')
    .RegisterPeriodic('periodic-2', '* * * * *', LQueue.Name, 'email', '{"id":2}')
    .RegisterPeriodic('periodic-3', '* * * * *', LQueue.Name, 'email', '{"id":3}');

  AssertTrue(LServer.PromotePeriodic = 1, 'periodic promotion should respect budget');
  AssertTrue(LServer.PromoteScheduled = 1, 'promoted periodic job should materialize into scheduled queue');
  AssertTrue(LQueue.PendingCount = 1, 'first periodic budget cycle should materialize only one job');
  AssertTrue(LServer.PromotePeriodic = 1, 'later cycles should continue scheduling pending definitions');
  AssertTrue(LServer.PromoteScheduled = 1, 'second periodic materialization should also respect scheduled promotion path');
  AssertTrue(LQueue.PendingCount = 2, 'second periodic cycle should add one more pending job');
end;

procedure TestScheduledStoreMaintainsDueOrder;
var
  LStore: IHefestoScheduledStore;
  LAttributes: TStringList;
  LEntries: TArray<THefestoScheduledEntry>;
  LNow: TDateTime;
begin
  LStore := THefestoInMemoryScheduledStore.New;
  LAttributes := TStringList.Create;
  try
    LNow := Now;
    LStore.Schedule(MakeScheduledEntry('default', 'third', '{"id":3}', LAttributes, IncSecond(LNow, 3)));
    LStore.Schedule(MakeScheduledEntry('default', 'first', '{"id":1}', LAttributes, IncSecond(LNow, 1)));
    LStore.Schedule(MakeScheduledEntry('default', 'second', '{"id":2}', LAttributes, IncSecond(LNow, 2)));

    LEntries := LStore.PopDue(IncSecond(LNow, 10), 10);
    AssertTrue(Length(LEntries) = 3, 'scheduled store should return all due entries');
    AssertTrue(LEntries[0].Action = 'first', 'scheduled store should keep earliest due entry first');
    AssertTrue(LEntries[1].Action = 'second', 'scheduled store should keep second due entry in order');
    AssertTrue(LEntries[2].Action = 'third', 'scheduled store should keep latest due entry last');
  finally
    LAttributes.Free;
  end;
end;

procedure TestQueueFairnessRotatesEqualWeights;
var
  LFetchSequence: TStringList;
  LQueue1: TFetchCountingQueue;
  LQueue2: TFetchCountingQueue;
  LServer: IHefestoServer;
begin
  LFetchSequence := TStringList.Create;
  try
    LQueue1 := TFetchCountingQueue.Create('critical', LFetchSequence);
    LQueue2 := TFetchCountingQueue.Create('default', LFetchSequence);
    LServer := THefestoServer.New
      .UseQueue(LQueue1)
      .UseQueue(LQueue2);

    LServer.RunOnce;
    LServer.RunOnce;

    AssertTrue(LQueue1.FetchCount = 2, 'equal-weight queue should be fetched once per cycle');
    AssertTrue(LQueue2.FetchCount = 2, 'equal-weight queue should be fetched once per cycle');
    AssertTrue(LFetchSequence.Count = 4, 'fairness probe should capture four fetches across two cycles');
    AssertTrue(LFetchSequence[0] = 'critical', 'first cycle should start with the first registered queue');
    AssertTrue(LFetchSequence[1] = 'default', 'first cycle should then visit the second queue');
    AssertTrue(LFetchSequence[2] = 'default', 'second cycle should rotate the starting queue');
    AssertTrue(LFetchSequence[3] = 'critical', 'rotation should keep fairness across cycles');
  finally
    LFetchSequence.Free;
  end;
end;

procedure TestQueueWeightIncreasesFetchFrequency;
var
  LFetchSequence: TStringList;
  LQueue1: TFetchCountingQueue;
  LQueue2: TFetchCountingQueue;
  LServer: IHefestoServer;
begin
  LFetchSequence := TStringList.Create;
  try
    LQueue1 := TFetchCountingQueue.Create('critical', LFetchSequence);
    LQueue2 := TFetchCountingQueue.Create('default', LFetchSequence);
    LServer := THefestoServer.New
      .UseQueue(LQueue1)
      .UseQueue(LQueue2)
      .QueueWeight('critical', 3)
      .QueueWeight('default', 1);

    LServer.RunOnce;

    AssertTrue(LQueue1.FetchCount = 3, 'weighted queue should be fetched according to configured weight');
    AssertTrue(LQueue2.FetchCount = 1, 'lower-weight queue should be fetched fewer times in the same cycle');
    AssertTrue(LFetchSequence.Count = 4, 'weighted fetch order should contain one slot per configured weight');
  finally
    LFetchSequence.Free;
  end;
end;

{ TFlakyPublishQueue }

procedure TFlakyPublishQueue.Ack(const AJob: IHefestoJobEnvelope);
begin
  FInner.Ack(AJob);
end;

function TFlakyPublishQueue.AckedCount: Integer;
begin
  Result := FInner.AckedCount;
end;

constructor TFlakyPublishQueue.Create(
  const AName: string;
  const AFailuresRemaining: Integer);
begin
  inherited Create;
  FInner := THefestoInMemoryQueueAdapter.New(AName);
  FFailuresRemaining := AFailuresRemaining;
end;

function TFlakyPublishQueue.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
begin
  Result := FInner.Fetch(AOptions);
end;

procedure TFlakyPublishQueue.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
  FInner.MoveToDeadLetter(AJob, AReason);
end;

procedure TFlakyPublishQueue.Nack(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
  FInner.Nack(AJob, ADelaySeconds);
end;

function TFlakyPublishQueue.Name: string;
begin
  Result := FInner.Name;
end;

function TFlakyPublishQueue.PendingCount: Integer;
begin
  Result := FInner.PendingCount;
end;

procedure TFlakyPublishQueue.Publish(const ARequest: THefestoPublishRequest);
begin
  if FFailuresRemaining > 0 then
  begin
    Dec(FFailuresRemaining);
    raise Exception.Create('planned publish failure');
  end;

  FInner.Publish(ARequest);
end;

{ TConcurrencyProbeHandler }

function TConcurrencyProbeHandler.CanHandle(
  const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'email';
end;

constructor TConcurrencyProbeHandler.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor TConcurrencyProbeHandler.Destroy;
begin
  FLock.Free;
  inherited;
end;

function TConcurrencyProbeHandler.MaxActiveCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FMaxActiveCount;
  finally
    FLock.Release;
  end;
end;

procedure TConcurrencyProbeHandler.Perform(const AContext: IHefestoJobContext);
begin
  FLock.Acquire;
  try
    Inc(FActiveCount);
    if FActiveCount > FMaxActiveCount then
      FMaxActiveCount := FActiveCount;
  finally
    FLock.Release;
  end;

  TThread.Sleep(100);

  FLock.Acquire;
  try
    Dec(FActiveCount);
  finally
    FLock.Release;
  end;
end;

{ TLeaseAwareQueue }

procedure TLeaseAwareQueue.Ack(const AJob: IHefestoJobEnvelope);
begin
  FInner.Ack(AJob);
end;

constructor TLeaseAwareQueue.Create(const AName: string);
begin
  inherited Create;
  FInner := THefestoInMemoryQueueAdapter.New(AName);
end;

function TLeaseAwareQueue.EnqueueWithAttributes(
  const AAction, ABody: string;
  const AAttributes: TStrings;
  const AAttempts: Integer): TLeaseAwareQueue;
begin
  Result := Self;
  FInner.EnqueueWithAttributes(AAction, ABody, AAttributes, AAttempts);
end;

function TLeaseAwareQueue.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
begin
  Result := FInner.Fetch(AOptions);
end;

procedure TLeaseAwareQueue.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
  FInner.MoveToDeadLetter(AJob, AReason);
end;

function TLeaseAwareQueue.Name: string;
begin
  Result := FInner.Name;
end;

function TLeaseAwareQueue.PendingCount: Integer;
begin
  Result := FInner.PendingCount;
end;

function TLeaseAwareQueue.AckedCount: Integer;
begin
  Result := FInner.AckedCount;
end;

procedure TLeaseAwareQueue.Nack(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
  FInner.Nack(AJob, ADelaySeconds);
end;

procedure TLeaseAwareQueue.RenewVisibility(
  const AJob: IHefestoJobEnvelope;
  const AVisibilityTimeout: Integer);
begin
  Inc(FRenewals);
end;

function TLeaseAwareQueue.Renewals: Integer;
begin
  Result := FRenewals;
end;

{ TSlowHandler }

function TSlowHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'email';
end;

constructor TSlowHandler.Create(const ADelayMs: Integer);
begin
  inherited Create;
  DelayMs := ADelayMs;
end;

procedure TSlowHandler.Perform(const AContext: IHefestoJobContext);
begin
  Inc(Calls);
  TThread.Sleep(DelayMs);
end;

{ TActionCountingHandler }

function TActionCountingHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, FAction);
end;

constructor TActionCountingHandler.Create(const AAction: string);
begin
  inherited Create;
  FAction := AAction;
end;

procedure TActionCountingHandler.Perform(const AContext: IHefestoJobContext);
begin
  Inc(Calls);
  LastBody := AContext.Job.Body;
end;

{ TFetchCountingQueue }

procedure TFetchCountingQueue.Ack(const AJob: IHefestoJobEnvelope);
begin
end;

constructor TFetchCountingQueue.Create(
  const AName: string;
  const AFetchSequence: TStrings);
begin
  inherited Create;
  FName := AName;
  FFetchSequence := AFetchSequence;
end;

function TFetchCountingQueue.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
begin
  Inc(FFetchCount);
  if Assigned(FFetchSequence) then
    FFetchSequence.Add(FName);
  SetLength(Result, 0);
end;

function TFetchCountingQueue.FetchCount: Integer;
begin
  Result := FFetchCount;
end;

procedure TFetchCountingQueue.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
end;

function TFetchCountingQueue.Name: string;
begin
  Result := FName;
end;

procedure TFetchCountingQueue.Nack(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
end;

procedure TFetchCountingQueue.Publish(const ARequest: THefestoPublishRequest);
begin
end;

{ TInMemoryPostgresBackend }

constructor TInMemoryPostgresBackend.Create;
begin
  inherited Create;
  FEntries := TStringList.Create;
end;

procedure TInMemoryPostgresBackend.DeleteEntry(
  const ATableName, AKey: string);
var
  LIndex: Integer;
begin
  LIndex := FEntries.IndexOfName(EntryName(ATableName, AKey));
  if LIndex >= 0 then
    FEntries.Delete(LIndex);
end;

function TInMemoryPostgresBackend.DeserializeEntry(
  const ATableName, AKey, AValue: string): THefestoPostgresStateEntry;
var
  LParts: TArray<string>;
  LHasExpiration: Boolean;
  LExpiresAt: TDateTime;
begin
  LParts := AValue.Split(['|']);
  LHasExpiration := (Length(LParts) > 0) and SameText(LParts[0], '1');
  if LHasExpiration and (Length(LParts) > 1) then
    LExpiresAt := ISO8601ToDate(LParts[1], False)
  else
    LExpiresAt := 0;
  if Length(LParts) > 2 then
    Result := THefestoPostgresStateEntry.Create(AKey, LParts[2], LHasExpiration, LExpiresAt)
  else
    Result := THefestoPostgresStateEntry.Create(AKey, EmptyStr, LHasExpiration, LExpiresAt);
end;

destructor TInMemoryPostgresBackend.Destroy;
begin
  FEntries.Free;
  inherited;
end;

function TInMemoryPostgresBackend.EntryName(
  const ATableName, AKey: string): string;
begin
  Result := ATableName + ':' + AKey;
end;

function TInMemoryPostgresBackend.GetEntry(
  const ATableName, AKey: string;
  out AEntry: THefestoPostgresStateEntry): Boolean;
var
  LIndex: Integer;
begin
  LIndex := FEntries.IndexOfName(EntryName(ATableName, AKey));
  Result := LIndex >= 0;
  if Result then
    AEntry := DeserializeEntry(
      ATableName,
      AKey,
      FEntries.ValueFromIndex[LIndex]);
end;

function TInMemoryPostgresBackend.ListEntries(
  const ATableName, APrefix: string): TArray<THefestoPostgresStateEntry>;
var
  LIndex: Integer;
  LName: string;
  LKey: string;
  LCount: Integer;
begin
  SetLength(Result, FEntries.Count);
  LCount := 0;
  for LIndex := 0 to FEntries.Count - 1 do
  begin
    LName := FEntries.Names[LIndex];
    if not LName.StartsWith(ATableName + ':') then
      Continue;
    LKey := Copy(LName, Length(ATableName) + 2, MaxInt);
    if (not APrefix.IsEmpty) and (not LKey.StartsWith(APrefix)) then
      Continue;
    Result[LCount] := DeserializeEntry(
      ATableName,
      LKey,
      FEntries.ValueFromIndex[LIndex]);
    Inc(LCount);
  end;
  SetLength(Result, LCount);
end;

procedure TInMemoryPostgresBackend.PutEntry(
  const ATableName: string;
  const AEntry: THefestoPostgresStateEntry);
begin
  FEntries.Values[EntryName(ATableName, AEntry.Key)] := SerializeEntry(AEntry);
end;

function TInMemoryPostgresBackend.SerializeEntry(
  const AEntry: THefestoPostgresStateEntry): string;
var
  LExpirationFlag: string;
  LExpiresAt: string;
begin
  if AEntry.HasExpiration then
    LExpirationFlag := '1'
  else
    LExpirationFlag := '0';
  if AEntry.HasExpiration then
    LExpiresAt := DateToISO8601(AEntry.ExpiresAt, False)
  else
    LExpiresAt := '';
  Result := LExpirationFlag + '|' + LExpiresAt + '|' + AEntry.Value;
end;

function TInMemoryPostgresBackend.TryPutIfAbsentOrExpired(
  const ATableName: string;
  const AEntry: THefestoPostgresStateEntry;
  const AReferenceTime: TDateTime): Boolean;
var
  LCurrent: THefestoPostgresStateEntry;
begin
  Result := not GetEntry(ATableName, AEntry.Key, LCurrent)
    or LCurrent.IsExpired(AReferenceTime);
  if Result then
    PutEntry(ATableName, AEntry);
end;

{ TActionToQueueRouter }

constructor TActionToQueueRouter.Create(const AAction, ATargetQueue: string);
begin
  inherited Create;
  FAction := AAction;
  FTargetQueue := ATargetQueue;
end;

function TActionToQueueRouter.Route(
  const AQueueName: string;
  const AAction: string;
  const AAttributes: TArray<TPair<string, string>>): string;
begin
  if SameText(AAction, FAction) then
    Result := FTargetQueue
  else
    Result := AQueueName;
end;

{ TAttributeQueueRouter }

function TAttributeQueueRouter.Route(
  const AQueueName: string;
  const AAction: string;
  const AAttributes: TArray<TPair<string, string>>): string;
var
  LIndex: Integer;
begin
  Result := AQueueName;
  for LIndex := 0 to High(AAttributes) do
    if SameText(AAttributes[LIndex].Key, 'target_queue') then
    begin
      Result := AAttributes[LIndex].Value;
      Exit;
    end;
end;

{ TCountingHandler }

function TCountingHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'email';
end;

procedure TCountingHandler.Perform(const AContext: IHefestoJobContext);
begin
  Inc(Calls);
  LastBody := AContext.Job.Body;
end;

{ TFailingHandler }

function TFailingHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'fail';
end;

procedure TFailingHandler.Perform(const AContext: IHefestoJobContext);
begin
  raise Exception.Create('planned failure');
end;

begin
  try
    TestSuccessAcksJob;
    TestFailureNacksJob;
    TestSqsAdapterNormalizesOperationalQueueReference;
    TestRunProcessesBatchAndStopsWhenIdle;
    TestRunMaxCyclesStops;
    TestInMemoryStateStore;
    TestPostgresStateStoreCrudContract;
    TestPostgresStateStoreTreatsExpiredKeyAsMissing;
    TestPostgresStateStoreListKeysIgnoresExpired;
    TestPostgresStateStoreTryPutIfAbsentReplacesExpired;
    TestInMemoryStateStoreExpiredKeyDoesNotDeadlock;
    TestInMemoryStateStoreListKeysPurgesExpiredWithoutDeadlock;
    TestInMemoryLockProvider;
     TestIdempotencySkipsRepeatedLogicalJob;
     TestRunProcessesMultipleQueues;
     TestPausedQueueSkipsFetchUntilResume;
     TestRunOnceSkipsFetchWhileDraining;
     TestRollingRestartStopsAfterCurrentBusyCycle;
     TestLeaderElectionFailoverBetweenServers;
     TestFollowerCannotPromoteScheduledEntries;
     TestNoAckPolicySkipsQueueAck;
     TestQueueConcurrencyLimitSerializesOneQueue;
     TestActionConcurrencyLimitSerializesOneAction;
     TestRateLimitRetriesWhenBucketIsExhausted;
     TestClientReliabilityStoresFailedPublishAndFlushesLater;
     TestExpiredQueuedJobIsDiscardedBeforeExecution;
     TestMetricsTelemetryEmitsStatsDPayloads;
     TestHistoricalMetricsTelemetryAggregatesExecution;
     TestServerReliabilityRenewsVisibilityForLongJob;
     TestServerReliabilityRecoversStaleExecution;
     TestBatchCallbacksRunOnSuccessfulBatch;
     TestBatchCallbacksSkipSuccessOnFinalFailure;
     TestUniqueUntilExpiredKeepsCompletedOnFailure;
     TestScheduledEnqueueInPromotesOnRun;
     TestScheduledEnqueueAtFuturePromotesOnlyWhenDue;
     TestFlushClientOutboxRespectsBudget;
     TestJobRouterPassThroughLeavesQueueUnchanged;
     TestJobRouterRedirectsJobToTargetQueue;
     TestJobRouterRoutesViaJobAttribute;
     TestServerMiddlewareWrapsHandler;
     TestClientMiddlewareInvokedOnEnqueueIn;
     TestPromoteScheduledRespectsBudget;
     TestPromoteBatchCallbacksRespectsBudget;
     TestPeriodicJobRunsOncePerWindow;
     TestPeriodicJobRespectsCronMismatch;
     TestPromotePeriodicRespectsBudget;
     TestScheduledStoreMaintainsDueOrder;
     TestQueueFairnessRotatesEqualWeights;
     TestQueueWeightIncreasesFetchFrequency;
     Writeln('Hefesto.Tests OK');
  except
    on E: Exception do
    begin
      Writeln(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
