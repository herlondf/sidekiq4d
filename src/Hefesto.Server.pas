unit Hefesto.Server;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Diagnostics,
  System.DateUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Hefesto.Job,
  Hefesto.Metadata,
  Hefesto.Batch,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Dispatcher,
  Hefesto.Ack,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Options,
  Hefesto.Store.Interfaces,
  Hefesto.Locking,
  Hefesto.Leader,
  Hefesto.Idempotency,
  Hefesto.ClientReliability,
  Hefesto.Middleware,
  Hefesto.Scheduled,
  Hefesto.Periodic,
  Hefesto.Executor,
  Hefesto.ServerReliability,
  Hefesto.RateLimit,
  Hefesto.WorkerPool,
  Hefesto.DeadLetter,
  Hefesto.Web;

type
  IHefestoServer = interface(IHefestoBatchEnqueuer)
    ['{BDB5E447-44F6-40FE-99B6-B13773E63DF4}']
    function UseQueue(const AQueue: IHefestoQueueAdapter): IHefestoServer;
    function BatchSize(const AValue: Integer): IHefestoServer;
    function WaitTimeSeconds(const AValue: Integer): IHefestoServer;
    function VisibilityTimeout(const AValue: Integer): IHefestoServer;
    function IdleDelayMs(const AValue: Integer): IHefestoServer;
    function Concurrency(const AValue: Integer): IHefestoServer;
    function ClientOutboxBudget(const AValue: Integer): IHefestoServer;
    function BatchCallbackPromotionBudget(const AValue: Integer): IHefestoServer;
    function PeriodicPromotionBudget(const AValue: Integer): IHefestoServer;
    function ScheduledPromotionBudget(const AValue: Integer): IHefestoServer;
    function QueueConcurrency(
      const AQueueName: string;
      const AValue: Integer): IHefestoServer;
    function QueueWeight(
      const AQueueName: string;
      const AValue: Integer): IHefestoServer;
    function ActionConcurrency(
      const AAction: string;
      const AValue: Integer): IHefestoServer;
    function MaxCycles(const AValue: Integer): IHefestoServer;
    function StopWhenIdle(const AValue: Boolean = True): IHefestoServer;
    function BeginRollingRestart: IHefestoServer;
    function ResumeAfterRollingRestart: IHefestoServer;
    function UseLeaderElection(const AValue: Boolean = True): IHefestoServer;
    function LeaderName(const AValue: string): IHefestoServer;
    function LeaderLeaseTtlSeconds(const AValue: Integer): IHefestoServer;
    function IsLeader: Boolean;
    function IsDraining: Boolean;
    function ActiveWorkers: Integer;
    function PauseQueue(const AQueueName: string): IHefestoServer;
    function ResumeQueue(const AQueueName: string): IHefestoServer;
    function QueuePaused(const AQueueName: string): Boolean;
    function AckPolicy(const AValue: IHefestoAckPolicy): IHefestoServer;
    function RetryPolicy(const AValue: IHefestoRetryPolicy): IHefestoServer;
    function Telemetry(const AValue: IHefestoTelemetry): IHefestoServer;
    function StateStore(const AValue: IHefestoStateStore): IHefestoServer;
    function LockProvider(const AValue: IHefestoLockProvider): IHefestoServer;
    function Idempotency(const AValue: IHefestoIdempotency): IHefestoServer;
    function ClientOutbox(const AValue: IHefestoClientOutbox): IHefestoServer;
    function RateLimiter(const AValue: IHefestoRateLimiter): IHefestoServer;
    function ScheduledStore(const AValue: IHefestoScheduledStore): IHefestoServer;
    function RegisterPeriodic(
      const AName, ACronExpression, AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoServer;
    function UseJobRouter(const ARouter: IHefestoJobRouter): IHefestoServer;
    function UseClientMiddleware(
      const AValue: IHefestoClientMiddleware): IHefestoServer;
    function UseServerMiddleware(
      const AValue: IHefestoServerMiddleware): IHefestoServer;
    function RegisterHandler(
      const AAction: string;
      const AHandler: IHefestoJobHandler): IHefestoServer;
    function Batch(const ADescription: string = ''): IHefestoBatch;
    function Enqueue(
      const AQueueName: string;
      const AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoServer;
    function EnqueueIn(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): IHefestoServer;
    function FlushClientOutbox: Integer;
    function EnqueueAt(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): IHefestoServer;
    function PromotePeriodic: Integer;
    function PromoteScheduled: Integer;
    function RunOnce: Integer;
    procedure Run;
    procedure Stop;
    // ── Novas features ────────────────────────────────────────────────
    { Dead Letter Queue: jobs que esgotam retries são registrados aqui. }
    function DeadLetterQueue(
      const AValue: IHefestoDeadLetterQueue): IHefestoServer;
    { Define várias filas com peso em uma única chamada. }
    function UseQueues(
      const AConfigs: TArray<THefestoQueueConfig>): IHefestoServer;
    { Dashboard web embutido. Chame Start() antes de Run(). }
    function UseWebDashboard(
      const ADashboard: IHefestoWebDashboard): IHefestoServer;
    { Retorna snapshot dos jobs periódicos registrados. }
    function GetPeriodicJobs: TArray<THefestoPeriodicDefinition>;
  end;

  THefestoServer = class(TInterfacedObject, IHefestoServer, IHefestoBatchEnqueuer, IHefestoDashboardServer)
  private
    FQueues: TArray<IHefestoQueueAdapter>;
    FQueueConcurrencyLimits: TDictionary<string, Integer>;
    FActionConcurrencyLimits: TDictionary<string, Integer>;
    FQueueWeights: TDictionary<string, Integer>;
    FDispatcher: IHefestoDispatcher;
    FFetchOptions: THefestoFetchOptions;
    FServerOptions: THefestoServerOptions;
    FAckPolicy: IHefestoAckPolicy;
    FRetryPolicy: IHefestoRetryPolicy;
    FTelemetry: IHefestoTelemetry;
    FStateStore: IHefestoStateStore;
    FLockProvider: IHefestoLockProvider;
    FIdempotency: IHefestoIdempotency;
    FClientOutbox: IHefestoClientOutbox;
    FRateLimiter: IHefestoRateLimiter;
    FHasCustomRateLimiter: Boolean;
    FScheduledStore: IHefestoScheduledStore;
    FBatchService: IHefestoBatchService;
    FServerReliability: IHefestoServerReliability;
    FLeaderElection: IHefestoLeaderElection;
    FPeriodicJobs: TArray<THefestoPeriodicDefinition>;
    FPeriodicJobsLock: TCriticalSection;
    FJobRouter: IHefestoJobRouter;
    FClientMiddleware: TArray<IHefestoClientMiddleware>;
    FServerMiddleware: TArray<IHefestoServerMiddleware>;
    FDeadLetterQueue: IHefestoDeadLetterQueue;
    FWebDashboard: IHefestoWebDashboard;
    // Serialises all mutations to FQueues, FClientMiddleware, FServerMiddleware,
    // FQueueConcurrencyLimits, FActionConcurrencyLimits, and FQueueWeights.
    // These are written during server setup (possibly from different threads)
    // and read by the run-loop and worker threads concurrently.
    FConfigLock: TCriticalSection;
    FLifecycleLock: TObject;
    FStopRequested: Boolean;
    FDrainRequested: Boolean;
    FWorkerActivity: IHefestoWorkerActivity;
    FFetchRotation: Integer;

    procedure EnsureConfigured;
    function BuildExecutor: IHefestoJobExecutor;
    function BuildWorkerPool: IHefestoWorkerPool;
    function BuildFetchQueueOrder: TArray<IHefestoQueueAdapter>;
    procedure RebuildLeaderElection;
    function CanRunLeaderDuties: Boolean;
    class function NormalizePositiveBudget(
      const AValue, ADefault: Integer): Integer; static;
    function StopRequested: Boolean;
    procedure SetStopRequested(const AValue: Boolean);
    function DrainRequested: Boolean;
    procedure SetDrainRequested(const AValue: Boolean);
    class function QueuePauseKey(const AQueueName: string): string; static;
    class function TryResolveExpiration(
      const AValue: string;
      out AExpiresAt: TDateTime): Boolean; static;
    function CreateNormalizedAttributes(
      const AAttributes: TStrings;
      const AReferenceTime: TDateTime): TStringList;
    procedure AppendFetchedJobs(
      const AQueue: IHefestoQueueAdapter;
      const AJobs: TArray<IHefestoJobEnvelope>;
      var ATarget: TArray<THefestoQueuedJob>);
    function IsQueuePausedInternal(const AQueueName: string): Boolean;
    function IsJobExpired(const AJob: IHefestoJobEnvelope): Boolean;
    procedure HandleExpiredJob(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope);
    function QueueWeightFor(const AQueueName: string): Integer;
    procedure AppendRunnableJobs(
      const ASource: TArray<THefestoQueuedJob>;
      var ATarget: TArray<THefestoQueuedJob>);
    function FindQueueByName(const AName: string): IHefestoQueueAdapter;
    procedure DispatchPublish(const ARequest: THefestoPublishRequest);
    // Dispatch N requests to a single queue — uses IHefestoQueueBatchPublisher
    // when available (one round-trip), falls back to individual Publish otherwise.
    procedure DispatchPublishBatch(
      const AQueueName: string;
      const ARequests: TArray<THefestoPublishRequest>);
    function PromoteBatchCallbacks: Integer;
    function PeriodicWindowKey(
      const ADefinition: THefestoPeriodicDefinition;
      const AWindow: TDateTime): string;
    procedure SchedulePeriodicDefinition(
      const ADefinition: THefestoPeriodicDefinition;
      const AWindow: TDateTime);
    procedure InvokeClientEnqueue(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings;
      const AFinalAction: THefestoNextProc);
  public
    constructor Create;
    destructor Destroy; override;
    class function New: IHefestoServer;

    function UseQueue(const AQueue: IHefestoQueueAdapter): IHefestoServer;
    function BatchSize(const AValue: Integer): IHefestoServer;
    function WaitTimeSeconds(const AValue: Integer): IHefestoServer;
    function VisibilityTimeout(const AValue: Integer): IHefestoServer;
    function IdleDelayMs(const AValue: Integer): IHefestoServer;
    function Concurrency(const AValue: Integer): IHefestoServer;
    function ClientOutboxBudget(const AValue: Integer): IHefestoServer;
    function BatchCallbackPromotionBudget(const AValue: Integer): IHefestoServer;
    function PeriodicPromotionBudget(const AValue: Integer): IHefestoServer;
    function ScheduledPromotionBudget(const AValue: Integer): IHefestoServer;
    function QueueConcurrency(
      const AQueueName: string;
      const AValue: Integer): IHefestoServer;
    function QueueWeight(
      const AQueueName: string;
      const AValue: Integer): IHefestoServer;
    function ActionConcurrency(
      const AAction: string;
      const AValue: Integer): IHefestoServer;
    function MaxCycles(const AValue: Integer): IHefestoServer;
    function StopWhenIdle(const AValue: Boolean = True): IHefestoServer;
    function BeginRollingRestart: IHefestoServer;
    function ResumeAfterRollingRestart: IHefestoServer;
    function UseLeaderElection(const AValue: Boolean = True): IHefestoServer;
    function LeaderName(const AValue: string): IHefestoServer;
    function LeaderLeaseTtlSeconds(const AValue: Integer): IHefestoServer;
    function IsLeader: Boolean;
    function IsDraining: Boolean;
    function ActiveWorkers: Integer;
    function PauseQueue(const AQueueName: string): IHefestoServer;
    function ResumeQueue(const AQueueName: string): IHefestoServer;
    function QueuePaused(const AQueueName: string): Boolean;
    function AckPolicy(const AValue: IHefestoAckPolicy): IHefestoServer;
    function RetryPolicy(const AValue: IHefestoRetryPolicy): IHefestoServer;
    function Telemetry(const AValue: IHefestoTelemetry): IHefestoServer;
    function StateStore(const AValue: IHefestoStateStore): IHefestoServer;
    function LockProvider(const AValue: IHefestoLockProvider): IHefestoServer;
    function Idempotency(const AValue: IHefestoIdempotency): IHefestoServer;
    function ClientOutbox(const AValue: IHefestoClientOutbox): IHefestoServer;
    function RateLimiter(const AValue: IHefestoRateLimiter): IHefestoServer;
    function ScheduledStore(const AValue: IHefestoScheduledStore): IHefestoServer;
    function RegisterPeriodic(
      const AName, ACronExpression, AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoServer;
    function UseJobRouter(const ARouter: IHefestoJobRouter): IHefestoServer;
    function UseClientMiddleware(
      const AValue: IHefestoClientMiddleware): IHefestoServer;
    function UseServerMiddleware(
      const AValue: IHefestoServerMiddleware): IHefestoServer;
    function RegisterHandler(
      const AAction: string;
      const AHandler: IHefestoJobHandler): IHefestoServer;
    function Batch(const ADescription: string = ''): IHefestoBatch;
    function Enqueue(
      const AQueueName: string;
      const AAction, ABody: string;
      const AAttributes: TStrings = nil): IHefestoServer;
    procedure EnqueueNow(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil);
    procedure ScheduleAt(
      const AQueueName, AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil);
    function EnqueueIn(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): IHefestoServer;
    function FlushClientOutbox: Integer;
    function EnqueueAt(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): IHefestoServer;
    function PromotePeriodic: Integer;
    function PromoteScheduled: Integer;
    function RunOnce: Integer;
    procedure Run;
    procedure Stop;
    function DeadLetterQueue(
      const AValue: IHefestoDeadLetterQueue): IHefestoServer;
    function UseQueues(
      const AConfigs: TArray<THefestoQueueConfig>): IHefestoServer;
    function UseWebDashboard(
      const ADashboard: IHefestoWebDashboard): IHefestoServer;
    function GetPeriodicJobs: TArray<THefestoPeriodicDefinition>;
    // IHefestoDashboardServer
    function ActiveWorkerCount: Integer;
    function MaxConcurrencyCount: Integer;
    function IsServerLeader: Boolean;
    function IsServerDraining: Boolean;
    procedure PauseQueueByName(const AQueueName: string);
    procedure ResumeQueueByName(const AQueueName: string);
    function IsQueuePausedByName(const AQueueName: string): Boolean;
    function ListPeriodicJobs: TArray<THefestoPeriodicDefinition>;
  end;

implementation

uses
  Hefesto.Store.InMemory;

{ THefestoServer }

function THefestoServer.BatchSize(const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FFetchOptions.BatchSize := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.BatchCallbackPromotionBudget(
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.BatchCallbackPromotionBudget := NormalizePositiveBudget(
      AValue,
      THefestoServerOptions.Default.BatchCallbackPromotionBudget);
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.BuildExecutor: IHefestoJobExecutor;
var
  LMiddlewareSnapshot: TArray<IHefestoServerMiddleware>;
begin
  FConfigLock.Acquire;
  try
    LMiddlewareSnapshot := Copy(FServerMiddleware);
  finally
    FConfigLock.Release;
  end;
  Result := THefestoJobExecutor.New(
    FDispatcher,
    FAckPolicy,
    FRetryPolicy,
    FTelemetry,
    FStateStore,
    FLockProvider,
    FIdempotency,
    FRateLimiter,
    FBatchService,
    LMiddlewareSnapshot,
    FServerReliability,
    FDeadLetterQueue);
end;

function THefestoServer.AckPolicy(
  const AValue: IHefestoAckPolicy): IHefestoServer;
begin
  Result := Self;
  FAckPolicy := AValue;
end;

function THefestoServer.BuildWorkerPool: IHefestoWorkerPool;
var
  LQueueLimits: TDictionary<string, Integer>;
  LActionLimits: TDictionary<string, Integer>;
begin
  // Take a snapshot under the lock; BuildSemaphores only iterates the dicts —
  // the pool does not take ownership — so we free the copies afterwards.
  FConfigLock.Acquire;
  try
    LQueueLimits  := TDictionary<string, Integer>.Create(FQueueConcurrencyLimits);
    LActionLimits := TDictionary<string, Integer>.Create(FActionConcurrencyLimits);
  finally
    FConfigLock.Release;
  end;
  try
    Result := THefestoSequentialWorkerPool.New(
      FServerOptions.Concurrency,
      LQueueLimits,
      LActionLimits,
      FWorkerActivity);
  finally
    LQueueLimits.Free;
    LActionLimits.Free;
  end;
end;

function THefestoServer.BuildFetchQueueOrder: TArray<IHefestoQueueAdapter>;
var
  LQueue: IHefestoQueueAdapter;
  LWeight: Integer;
  LIndex: Integer;
  LSourceIndex: Integer;
  LLength: Integer;
  LOffset: Integer;
  LOrdered: TArray<IHefestoQueueAdapter>;
  LQueueSnapshot: TArray<IHefestoQueueAdapter>;
begin
  FConfigLock.Acquire;
  try
    LQueueSnapshot := Copy(FQueues);
  finally
    FConfigLock.Release;
  end;

  SetLength(LOrdered, 0);
  for LQueue in LQueueSnapshot do
  begin
    LWeight := QueueWeightFor(LQueue.Name);
    for LIndex := 1 to LWeight do
    begin
      SetLength(LOrdered, Length(LOrdered) + 1);
      LOrdered[High(LOrdered)] := LQueue;
    end;
  end;

  LLength := Length(LOrdered);
  if LLength = 0 then
    Exit(LOrdered);

  SetLength(Result, LLength);
  // Use pre-increment value so rotation starts at 0 on first call.
  // Subtract 1 from Increment's return (which is post-increment) to get
  // a unique, monotonically increasing slot per caller thread.
  LOffset := (TInterlocked.Increment(FFetchRotation) - 1) mod LLength;
  for LIndex := 0 to Pred(LLength) do
  begin
    LSourceIndex := (LOffset + LIndex) mod LLength;
    Result[LIndex] := LOrdered[LSourceIndex];
  end;
end;

function THefestoServer.CanRunLeaderDuties: Boolean;
begin
  if not FServerOptions.LeaderElectionEnabled then
    Exit(True);
  Result := Assigned(FLeaderElection) and FLeaderElection.EnsureLeadership;
end;

function THefestoServer.ClientOutboxBudget(
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.ClientOutboxBudget := NormalizePositiveBudget(
      AValue,
      THefestoServerOptions.Default.ClientOutboxBudget);
  finally
    FConfigLock.Release;
  end;
end;

procedure THefestoServer.AppendRunnableJobs(
  const ASource: TArray<THefestoQueuedJob>;
  var ATarget: TArray<THefestoQueuedJob>);
var
  LQueuedJob: THefestoQueuedJob;
  LBaseIndex: Integer;
begin
  for LQueuedJob in ASource do
  begin
    if IsJobExpired(LQueuedJob.Job) then
    begin
      HandleExpiredJob(LQueuedJob.Queue, LQueuedJob.Job);
      Continue;
    end;

    LBaseIndex := Length(ATarget);
    SetLength(ATarget, LBaseIndex + 1);
    ATarget[LBaseIndex] := LQueuedJob;
  end;
end;

procedure THefestoServer.AppendFetchedJobs(
  const AQueue: IHefestoQueueAdapter;
  const AJobs: TArray<IHefestoJobEnvelope>;
  var ATarget: TArray<THefestoQueuedJob>);
var
  LBaseIndex: Integer;
  LIndex: Integer;
  LVisibilityTimeout: Integer;
begin
  FConfigLock.Acquire;
  try
    LVisibilityTimeout := FFetchOptions.VisibilityTimeout;
  finally
    FConfigLock.Release;
  end;
  LBaseIndex := Length(ATarget);
  SetLength(ATarget, LBaseIndex + Length(AJobs));
  for LIndex := 0 to Pred(Length(AJobs)) do
    begin
      ATarget[LBaseIndex + LIndex].Queue := AQueue;
      ATarget[LBaseIndex + LIndex].Job := AJobs[LIndex];
      ATarget[LBaseIndex + LIndex].VisibilityTimeout := LVisibilityTimeout;
    end;
end;

constructor THefestoServer.Create;
begin
  inherited Create;
  FLifecycleLock := TObject.Create;
  FQueueConcurrencyLimits := TDictionary<string, Integer>.Create;
  FActionConcurrencyLimits := TDictionary<string, Integer>.Create;
  FQueueWeights := TDictionary<string, Integer>.Create;
  FDispatcher := THefestoDispatcher.New;
  FFetchOptions := THefestoFetchOptions.Default;
  FServerOptions := THefestoServerOptions.Default;
  FAckPolicy := THefestoDefaultAckPolicy.New;
  FRetryPolicy := THefestoSimpleRetryPolicy.New;
  FTelemetry := THefestoNoopTelemetry.New;
  FStateStore := THefestoInMemoryStateStore.New;
  FLockProvider := THefestoInMemoryLockProvider.New(FStateStore);
  FIdempotency := THefestoStateStoreIdempotency.New(FStateStore);
  FClientOutbox := THefestoFileClientOutbox.NewDefault;
  FRateLimiter := THefestoTokenBucketRateLimiter.New(FStateStore);
  FHasCustomRateLimiter := False;
  FScheduledStore := THefestoInMemoryScheduledStore.New;
  FBatchService := THefestoBatchStateStore.New(FStateStore);
  FServerReliability := THefestoServerReliability.New(
    FStateStore,
    FLockProvider,
    FIdempotency);
  FJobRouter := THefestoPassThroughRouter.Create;
  FWorkerActivity := THefestoWorkerActivity.New;
  FConfigLock := TCriticalSection.Create;
  FPeriodicJobsLock := TCriticalSection.Create;
  RebuildLeaderElection;
  SetLength(FPeriodicJobs, 0);
  SetLength(FClientMiddleware, 0);
  SetLength(FServerMiddleware, 0);
end;

function THefestoServer.CreateNormalizedAttributes(
  const AAttributes: TStrings;
  const AReferenceTime: TDateTime): TStringList;
var
  LReferenceTime: TDateTime;
  LExpiresInSeconds: Integer;
begin
  Result := TStringList.Create;
  if Assigned(AAttributes) then
    Result.Assign(AAttributes);

  if not Result.Values[THefestoJobAttribute.ExpiresAt].Trim.IsEmpty then
    Exit;

  LExpiresInSeconds := StrToIntDef(
    Result.Values[THefestoJobAttribute.ExpiresInSeconds],
    0);
  if LExpiresInSeconds <= 0 then
    Exit;

  LReferenceTime := AReferenceTime;
  if LReferenceTime <= 0 then
    LReferenceTime := Now;
  Result.Values[THefestoJobAttribute.ExpiresAt] :=
    IntToStr(DateTimeToUnix(IncSecond(LReferenceTime, LExpiresInSeconds)));
end;

destructor THefestoServer.Destroy;
begin
  if Assigned(FLeaderElection) then
    FLeaderElection.Release;
  FConfigLock.Free;
  FPeriodicJobsLock.Free;
  FLifecycleLock.Free;
  FActionConcurrencyLimits.Free;
  FQueueWeights.Free;
  FQueueConcurrencyLimits.Free;
  inherited;
end;

function THefestoServer.ActiveWorkers: Integer;
begin
  if Assigned(FWorkerActivity) then
    Result := FWorkerActivity.ActiveWorkers
  else
    Result := 0;
end;

function THefestoServer.ActionConcurrency(
  const AAction: string;
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  if AAction.Trim.IsEmpty then
    Exit;

  FConfigLock.Acquire;
  try
    if AValue > 0 then
      FActionConcurrencyLimits.AddOrSetValue(AAction, AValue)
    else
      FActionConcurrencyLimits.Remove(AAction);
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.Concurrency(const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    if AValue > 0 then
      FServerOptions.Concurrency := AValue
    else
      FServerOptions.Concurrency := 1;
  finally
    FConfigLock.Release;
  end;
end;

class function THefestoServer.NormalizePositiveBudget(
  const AValue, ADefault: Integer): Integer;
begin
  if AValue > 0 then
    Result := AValue
  else
    Result := ADefault;
end;

procedure THefestoServer.EnsureConfigured;
var
  LLen: Integer;
begin
  FConfigLock.Acquire;
  try
    LLen := Length(FQueues);
  finally
    FConfigLock.Release;
  end;
  if LLen = 0 then
    raise Exception.Create('Queue adapter not configured.');
end;

procedure THefestoServer.HandleExpiredJob(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope);
begin
  FAckPolicy.OnDiscard(AQueue, AJob, 'job expired before execution', FTelemetry);
  if Assigned(FBatchService) then
    FBatchService.RecordFinalFailure(AJob.Attribute(THefestoJobAttribute.BatchId));
end;

function THefestoServer.Idempotency(
  const AValue: IHefestoIdempotency): IHefestoServer;
begin
  Result := Self;
  FIdempotency := AValue;
end;

function THefestoServer.IdleDelayMs(const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.IdleDelayMs := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.IsLeader: Boolean;
begin
  if not FServerOptions.LeaderElectionEnabled then
    Exit(True);
  Result := Assigned(FLeaderElection) and FLeaderElection.IsLeader;
end;

function THefestoServer.IsDraining: Boolean;
begin
  Result := DrainRequested;
end;

function THefestoServer.IsJobExpired(const AJob: IHefestoJobEnvelope): Boolean;
var
  LExpiresAt: TDateTime;
begin
  Result := TryResolveExpiration(
    AJob.Attribute(THefestoJobAttribute.ExpiresAt),
    LExpiresAt)
    and (Now >= LExpiresAt);
end;

function THefestoServer.IsQueuePausedInternal(const AQueueName: string): Boolean;
begin
  Result := Assigned(FStateStore)
    and not AQueueName.Trim.IsEmpty
    and FStateStore.Exists(QueuePauseKey(AQueueName));
end;

function THefestoServer.ClientOutbox(
  const AValue: IHefestoClientOutbox): IHefestoServer;
begin
  Result := Self;
  if Assigned(AValue) then
    FClientOutbox := AValue
  else
    FClientOutbox := THefestoFileClientOutbox.NewDefault;
end;

function THefestoServer.RateLimiter(
  const AValue: IHefestoRateLimiter): IHefestoServer;
begin
  Result := Self;
  if Assigned(AValue) then
  begin
    FRateLimiter := AValue;
    FHasCustomRateLimiter := True;
  end
  else
  begin
    FRateLimiter := THefestoTokenBucketRateLimiter.New(FStateStore);
    FHasCustomRateLimiter := False;
  end;
end;

function THefestoServer.MaxCycles(const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.MaxCycles := AValue;
  finally
    FConfigLock.Release;
  end;
end;

class function THefestoServer.New: IHefestoServer;
begin
  Result := THefestoServer.Create;
end;

function THefestoServer.LeaderLeaseTtlSeconds(
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    if AValue > 0 then
      FServerOptions.LeaderLeaseTtlSeconds := AValue
    else
      FServerOptions.LeaderLeaseTtlSeconds := 30;
  finally
    FConfigLock.Release;
  end;
  RebuildLeaderElection;
end;

function THefestoServer.LeaderName(const AValue: string): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    if not AValue.Trim.IsEmpty then
      FServerOptions.LeaderName := AValue.Trim
    else
      FServerOptions.LeaderName := 'default';
  finally
    FConfigLock.Release;
  end;
  RebuildLeaderElection;
end;

function THefestoServer.PauseQueue(const AQueueName: string): IHefestoServer;
begin
  Result := Self;
  if AQueueName.Trim.IsEmpty then
    Exit;
  FStateStore.Put(QueuePauseKey(AQueueName), '1');
end;

function THefestoServer.QueuePaused(const AQueueName: string): Boolean;
begin
  Result := IsQueuePausedInternal(AQueueName);
end;

function THefestoServer.QueueConcurrency(
  const AQueueName: string;
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  if AQueueName.Trim.IsEmpty then
    Exit;

  FConfigLock.Acquire;
  try
    if AValue > 0 then
      FQueueConcurrencyLimits.AddOrSetValue(AQueueName, AValue)
    else
      FQueueConcurrencyLimits.Remove(AQueueName);
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.QueueWeight(
  const AQueueName: string;
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  if AQueueName.Trim.IsEmpty then
    Exit;

  FConfigLock.Acquire;
  try
    if AValue > 0 then
      FQueueWeights.AddOrSetValue(AQueueName, AValue)
    else
      FQueueWeights.AddOrSetValue(AQueueName, 1);
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.QueueWeightFor(const AQueueName: string): Integer;
begin
  FConfigLock.Acquire;
  try
    if not FQueueWeights.TryGetValue(AQueueName, Result) or (Result <= 0) then
      Result := 1;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.RegisterHandler(
  const AAction: string;
  const AHandler: IHefestoJobHandler): IHefestoServer;
begin
  Result := Self;
  FDispatcher.RegisterHandler(AAction, AHandler);
end;

function THefestoServer.ResumeQueue(const AQueueName: string): IHefestoServer;
begin
  Result := Self;
  if AQueueName.Trim.IsEmpty then
    Exit;
  FStateStore.Delete(QueuePauseKey(AQueueName));
end;

function THefestoServer.ScheduledPromotionBudget(
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.ScheduledPromotionBudget := NormalizePositiveBudget(
      AValue,
      THefestoServerOptions.Default.ScheduledPromotionBudget);
  finally
    FConfigLock.Release;
  end;
end;

procedure THefestoServer.RebuildLeaderElection;
begin
  if Assigned(FLeaderElection) then
    FLeaderElection.Release;

  if FServerOptions.LeaderElectionEnabled then
    FLeaderElection := THefestoLeaderElection.New(
      FLockProvider,
      FServerOptions.LeaderName,
      FServerOptions.LeaderLeaseTtlSeconds)
  else
    FLeaderElection := nil;
end;

function THefestoServer.Batch(const ADescription: string): IHefestoBatch;
begin
  Result := THefestoBatchBuilder.New(
    FBatchService.CreateBatch(ADescription),
    ADescription,
    Self as IHefestoBatchEnqueuer,
    FBatchService);
end;

function THefestoServer.RetryPolicy(
  const AValue: IHefestoRetryPolicy): IHefestoServer;
begin
  Result := Self;
  FRetryPolicy := AValue;
end;

procedure THefestoServer.Run;
var
  LProcessed: Integer;
  LCycle: Integer;
  LOpts: THefestoServerOptions;
begin
  EnsureConfigured;

  // Snapshot options once — prevents torn reads from concurrent setter calls.
  FConfigLock.Acquire;
  try
    LOpts := FServerOptions;
  finally
    FConfigLock.Release;
  end;

  SetStopRequested(False);
  LCycle := 0;
  if Assigned(FWebDashboard) and not FWebDashboard.IsRunning then
    FWebDashboard.Start(9999);
  FTelemetry.PollingStarted;
  try
    while not StopRequested do
    begin
      Inc(LCycle);
      LProcessed := RunOnce;

      if (LOpts.MaxCycles > 0) and (LCycle >= LOpts.MaxCycles) then
        Break;

      if LProcessed = 0 then
      begin
        if LOpts.StopWhenIdle then
          Break;

        if LOpts.IdleDelayMs > 0 then
        begin
          FTelemetry.Idle(LOpts.IdleDelayMs);
          TThread.Sleep(LOpts.IdleDelayMs);
        end;
      end;
    end;
  finally
    if Assigned(FLeaderElection) then
      FLeaderElection.Release;
    FTelemetry.PollingStopped;
  end;
end;

function THefestoServer.RunOnce: Integer;
var
  LFetchedJobs: TArray<IHefestoJobEnvelope>;
  LQueuedJobs: TArray<THefestoQueuedJob>;
  LRunnableJobs: TArray<THefestoQueuedJob>;
  LQueue: IHefestoQueueAdapter;
  LFetchQueue: IHefestoQueueAdapter;
  LFetchOrder: TArray<IHefestoQueueAdapter>;
  LExecutor: IHefestoJobExecutor;
  LWorkerPool: IHefestoWorkerPool;
  LFetchOpts: THefestoFetchOptions;
begin
  EnsureConfigured;

  // Snapshot fetch options — prevents torn reads from concurrent setter calls.
  FConfigLock.Acquire;
  try
    LFetchOpts := FFetchOptions;
  finally
    FConfigLock.Release;
  end;

  FTelemetry.ServerStarted;
  try
    if DrainRequested then
      Exit(0);
    FlushClientOutbox;
    PromoteBatchCallbacks;
    PromotePeriodic;
    PromoteScheduled;
    if CanRunLeaderDuties and Assigned(FServerReliability) then
      FServerReliability.RecoverStaleExecutions;
    LFetchOrder := BuildFetchQueueOrder;
    for LFetchQueue in LFetchOrder do
    begin
      LQueue := LFetchQueue;
      if IsQueuePausedInternal(LQueue.Name) then
      begin
        FTelemetry.FetchFinished(LQueue.Name, 0);
        Continue;
      end;
      LFetchedJobs := LQueue.Fetch(LFetchOpts);
      FTelemetry.FetchFinished(LQueue.Name, Length(LFetchedJobs));
      AppendFetchedJobs(LQueue, LFetchedJobs, LQueuedJobs);
    end;

    Result := Length(LQueuedJobs);
    if Result > 0 then
    begin
      AppendRunnableJobs(LQueuedJobs, LRunnableJobs);
      if Length(LRunnableJobs) = 0 then
        Exit;
      LExecutor := BuildExecutor;
      LWorkerPool := BuildWorkerPool;
      LWorkerPool.Execute(LRunnableJobs, LExecutor);
    end;
  finally
    FTelemetry.ServerStopped;
  end;
end;

procedure THefestoServer.Stop;
begin
  SetStopRequested(True);
  if Assigned(FWebDashboard) then
    FWebDashboard.Stop;
end;

function THefestoServer.DeadLetterQueue(
  const AValue: IHefestoDeadLetterQueue): IHefestoServer;
begin
  Result := Self;
  FDeadLetterQueue := AValue;
end;

function THefestoServer.UseQueues(
  const AConfigs: TArray<THefestoQueueConfig>): IHefestoServer;
var
  LConfig: THefestoQueueConfig;
begin
  Result := Self;
  for LConfig in AConfigs do
    QueueWeight(LConfig.Name, LConfig.Weight);
end;

function THefestoServer.UseWebDashboard(
  const ADashboard: IHefestoWebDashboard): IHefestoServer;
begin
  Result := Self;
  FWebDashboard := ADashboard;
end;

function THefestoServer.GetPeriodicJobs: TArray<THefestoPeriodicDefinition>;
begin
  FPeriodicJobsLock.Acquire;
  try
    Result := Copy(FPeriodicJobs);
  finally
    FPeriodicJobsLock.Release;
  end;
end;

function THefestoServer.StopRequested: Boolean;
begin
  TMonitor.Enter(FLifecycleLock);
  try
    Result := FStopRequested;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

procedure THefestoServer.SetStopRequested(const AValue: Boolean);
begin
  TMonitor.Enter(FLifecycleLock);
  try
    FStopRequested := AValue;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

function THefestoServer.DrainRequested: Boolean;
begin
  TMonitor.Enter(FLifecycleLock);
  try
    Result := FDrainRequested;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

procedure THefestoServer.SetDrainRequested(const AValue: Boolean);
begin
  TMonitor.Enter(FLifecycleLock);
  try
    FDrainRequested := AValue;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

function THefestoServer.StateStore(
  const AValue: IHefestoStateStore): IHefestoServer;
begin
  Result := Self;
  FStateStore := AValue;
  FLockProvider := THefestoInMemoryLockProvider.New(FStateStore);
  FIdempotency := THefestoStateStoreIdempotency.New(FStateStore);
  if not FHasCustomRateLimiter then
    FRateLimiter := THefestoTokenBucketRateLimiter.New(FStateStore);
  FBatchService := THefestoBatchStateStore.New(FStateStore);
  FServerReliability := THefestoServerReliability.New(
    FStateStore,
    FLockProvider,
    FIdempotency);
  RebuildLeaderElection;
end;

function THefestoServer.LockProvider(
  const AValue: IHefestoLockProvider): IHefestoServer;
begin
  Result := Self;
  FLockProvider := AValue;
  FServerReliability := THefestoServerReliability.New(
    FStateStore,
    FLockProvider,
    FIdempotency);
  RebuildLeaderElection;
end;

procedure THefestoServer.DispatchPublish(const ARequest: THefestoPublishRequest);
var
  LQueue: IHefestoQueueAdapter;
  LPublisher: IHefestoQueuePublisher;
  LRequest: THefestoPublishRequest;
  LRoutedQueue: string;
begin
  LRequest := ARequest;
  LRoutedQueue := FJobRouter.Route(LRequest.QueueName, LRequest.Action, LRequest.Attributes);
  if not LRoutedQueue.IsEmpty then
    LRequest.QueueName := LRoutedQueue;

  LQueue := FindQueueByName(LRequest.QueueName);
  if not Assigned(LQueue) then
    raise Exception.CreateFmt(
      'Queue adapter not configured for publish: %s',
      [LRequest.QueueName]);

  if not Supports(LQueue, IHefestoQueuePublisher, LPublisher) then
    raise Exception.CreateFmt(
      'Queue adapter "%s" does not support client publishing.',
      [LQueue.Name]);

  LPublisher.Publish(LRequest);
end;

procedure THefestoServer.DispatchPublishBatch(
  const AQueueName: string;
  const ARequests: TArray<THefestoPublishRequest>);
var
  LQueue: IHefestoQueueAdapter;
  LBatchPublisher: IHefestoQueueBatchPublisher;
  LPublisher: IHefestoQueuePublisher;
  LRequest: THefestoPublishRequest;
begin
  if Length(ARequests) = 0 then
    Exit;

  LQueue := FindQueueByName(AQueueName);
  if not Assigned(LQueue) then
    raise Exception.CreateFmt(
      'Queue adapter not configured for publish: %s', [AQueueName]);

  // Prefer batch (one round-trip) when the adapter supports it
  if Supports(LQueue, IHefestoQueueBatchPublisher, LBatchPublisher) then
  begin
    LBatchPublisher.PublishBatch(ARequests);
    Exit;
  end;

  // Fallback: individual publish for adapters without batch support
  if not Supports(LQueue, IHefestoQueuePublisher, LPublisher) then
    raise Exception.CreateFmt(
      'Queue adapter "%s" does not support client publishing.', [LQueue.Name]);

  for LRequest in ARequests do
    LPublisher.Publish(LRequest);
end;

function THefestoServer.StopWhenIdle(const AValue: Boolean): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.StopWhenIdle := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.BeginRollingRestart: IHefestoServer;
begin
  Result := Self;
  SetDrainRequested(True);
  SetStopRequested(True);
end;

function THefestoServer.ResumeAfterRollingRestart: IHefestoServer;
begin
  Result := Self;
  SetDrainRequested(False);
  SetStopRequested(False);
end;

function THefestoServer.UseLeaderElection(
  const AValue: Boolean): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.LeaderElectionEnabled := AValue;
  finally
    FConfigLock.Release;
  end;
  RebuildLeaderElection;
end;

function THefestoServer.Telemetry(
  const AValue: IHefestoTelemetry): IHefestoServer;
begin
  Result := Self;
  FTelemetry := AValue;
end;

function THefestoServer.UseQueue(
  const AQueue: IHefestoQueueAdapter): IHefestoServer;
var
  LLength: Integer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    LLength := Length(FQueues);
    SetLength(FQueues, LLength + 1);
    FQueues[LLength] := AQueue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.Enqueue(
  const AQueueName: string;
  const AAction, ABody: string;
  const AAttributes: TStrings): IHefestoServer;
var
  LRequest: THefestoPublishRequest;
  LNormalizedAttributes: TStringList;
  LTargetQueue: string;
  LFirstQueue: IHefestoQueueAdapter;
begin
  Result := Self;
  LTargetQueue := AQueueName;
  if LTargetQueue.Trim.IsEmpty then
  begin
    FConfigLock.Acquire;
    try
      if Length(FQueues) > 0 then
        LFirstQueue := FQueues[0];
    finally
      FConfigLock.Release;
    end;
    if Assigned(LFirstQueue) then
      LTargetQueue := LFirstQueue.Name;
  end;

  LNormalizedAttributes := CreateNormalizedAttributes(AAttributes, Now);
  try
    InvokeClientEnqueue(LTargetQueue, AAction, ABody, LNormalizedAttributes,
      procedure
      begin
        LRequest := MakePublishRequest(LTargetQueue, AAction, ABody, 0, LNormalizedAttributes);
        try
          FlushClientOutbox;
          DispatchPublish(LRequest);
        except
          FClientOutbox.Save(LRequest);
        end;
      end);
  finally
    LNormalizedAttributes.Free;
  end;
end;

procedure THefestoServer.EnqueueNow(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings);
begin
  Enqueue(AQueueName, AAction, ABody, AAttributes);
end;

function THefestoServer.VisibilityTimeout(const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FFetchOptions.VisibilityTimeout := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.WaitTimeSeconds(const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FFetchOptions.WaitTimeSeconds := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.ScheduledStore(
  const AValue: IHefestoScheduledStore): IHefestoServer;
begin
  Result := Self;
  if Assigned(AValue) then
    FScheduledStore := AValue
  else
    FScheduledStore := THefestoInMemoryScheduledStore.New;
end;

function THefestoServer.RegisterPeriodic(
  const AName, ACronExpression, AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): IHefestoServer;
var
  LLength: Integer;
begin
  Result := Self;
  FPeriodicJobsLock.Acquire;
  try
    LLength := Length(FPeriodicJobs);
    SetLength(FPeriodicJobs, LLength + 1);
    FPeriodicJobs[LLength] := MakePeriodicDefinition(
      AName,
      ACronExpression,
      AQueueName,
      AAction,
      ABody,
      AAttributes);
  finally
    FPeriodicJobsLock.Release;
  end;
end;

function THefestoServer.UseClientMiddleware(
  const AValue: IHefestoClientMiddleware): IHefestoServer;
var
  LLength: Integer;
begin
  Result := Self;
  if not Assigned(AValue) then
    Exit;
  FConfigLock.Acquire;
  try
    LLength := Length(FClientMiddleware);
    SetLength(FClientMiddleware, LLength + 1);
    FClientMiddleware[LLength] := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.UseServerMiddleware(
  const AValue: IHefestoServerMiddleware): IHefestoServer;
var
  LLength: Integer;
begin
  Result := Self;
  if not Assigned(AValue) then
    Exit;
  FConfigLock.Acquire;
  try
    LLength := Length(FServerMiddleware);
    SetLength(FServerMiddleware, LLength + 1);
    FServerMiddleware[LLength] := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.UseJobRouter(
  const ARouter: IHefestoJobRouter): IHefestoServer;
begin
  Result := Self;
  if Assigned(ARouter) then
    FJobRouter := ARouter;
end;

function THefestoServer.FindQueueByName(
  const AName: string): IHefestoQueueAdapter;
var
  LQueue: IHefestoQueueAdapter;
  LSnapshot: TArray<IHefestoQueueAdapter>;
begin
  FConfigLock.Acquire;
  try
    LSnapshot := Copy(FQueues);
  finally
    FConfigLock.Release;
  end;

  Result := nil;
  if AName.Trim.IsEmpty then
  begin
    if Length(LSnapshot) > 0 then
      Result := LSnapshot[0];
    Exit;
  end;
  for LQueue in LSnapshot do
    if SameText(LQueue.Name, AName) then
      Exit(LQueue);
  if Length(LSnapshot) > 0 then
    Result := LSnapshot[0];
end;

procedure THefestoServer.InvokeClientEnqueue(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings;
  const AFinalAction: THefestoNextProc);
var
  LChain: THefestoClientMiddlewareChain;
  LSnapshot: TArray<IHefestoClientMiddleware>;
begin
  FConfigLock.Acquire;
  try
    LSnapshot := Copy(FClientMiddleware);
  finally
    FConfigLock.Release;
  end;

  if Length(LSnapshot) = 0 then
  begin
    AFinalAction();
    Exit;
  end;
  LChain := THefestoClientMiddlewareChain.Create(LSnapshot);
  try
    LChain.Invoke(AQueueName, AAction, ABody, AAttributes, AFinalAction);
  finally
    LChain.Free;
  end;
end;

function THefestoServer.EnqueueIn(
  const AQueueName: string;
  const AAction, ABody: string;
  const ADelaySeconds: Integer;
  const AAttributes: TStrings): IHefestoServer;
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

function THefestoServer.EnqueueAt(
  const AQueueName: string;
  const AAction, ABody: string;
  const ADueAt: TDateTime;
  const AAttributes: TStrings): IHefestoServer;
var
  LEntry: THefestoScheduledEntry;
  LNormalizedAttributes: TStringList;
  LTargetQueue: string;
  LFirstQueue: IHefestoQueueAdapter;
begin
  Result := Self;
  LTargetQueue := AQueueName;
  if LTargetQueue.Trim.IsEmpty then
  begin
    FConfigLock.Acquire;
    try
      if Length(FQueues) > 0 then
        LFirstQueue := FQueues[0];
    finally
      FConfigLock.Release;
    end;
    if Assigned(LFirstQueue) then
      LTargetQueue := LFirstQueue.Name;
  end;

  LNormalizedAttributes := CreateNormalizedAttributes(AAttributes, Now);
  try
    InvokeClientEnqueue(LTargetQueue, AAction, ABody, LNormalizedAttributes,
      procedure
      begin
        LEntry := MakeScheduledEntry(
          LTargetQueue,
          AAction,
          ABody,
          LNormalizedAttributes,
          ADueAt);
        FScheduledStore.Schedule(LEntry);
      end);
  finally
    LNormalizedAttributes.Free;
  end;
end;

procedure THefestoServer.ScheduleAt(
  const AQueueName, AAction, ABody: string;
  const ADueAt: TDateTime;
  const AAttributes: TStrings);
begin
  EnqueueAt(AQueueName, AAction, ABody, ADueAt, AAttributes);
end;

function THefestoServer.FlushClientOutbox: Integer;
var
  LAllEntries: TArray<THefestoOutboxEntry>;
  LEntry: THefestoOutboxEntry;
  LBudget: Integer;
  LByQueue: TObjectDictionary<string, TList<THefestoOutboxEntry>>;
  LGroup: TList<THefestoOutboxEntry>;
  LQueueName: string;
  LRequests: TArray<THefestoPublishRequest>;
  LIndex: Integer;
begin
  Result := 0;
  if not Assigned(FClientOutbox) then
    Exit;

  // Collect up to budget entries, group by queue for pipeline dispatch
  LBudget := FServerOptions.ClientOutboxBudget;
  LAllEntries := FClientOutbox.Entries;
  if Length(LAllEntries) > LBudget then
    SetLength(LAllEntries, LBudget);
  if Length(LAllEntries) = 0 then
    Exit;

  LByQueue := TObjectDictionary<string, TList<THefestoOutboxEntry>>.Create([doOwnsValues]);
  try
    for LEntry in LAllEntries do
    begin
      if not LByQueue.TryGetValue(LEntry.Request.QueueName, LGroup) then
      begin
        LGroup := TList<THefestoOutboxEntry>.Create;
        LByQueue.Add(LEntry.Request.QueueName, LGroup);
      end;
      LGroup.Add(LEntry);
    end;

    for LQueueName in LByQueue.Keys do
    begin
      LGroup := LByQueue[LQueueName];
      SetLength(LRequests, LGroup.Count);
      for LIndex := 0 to Pred(LGroup.Count) do
        LRequests[LIndex] := LGroup[LIndex].Request;
      try
        DispatchPublishBatch(LQueueName, LRequests);
        for LIndex := 0 to Pred(LGroup.Count) do
        begin
          FClientOutbox.Remove(LGroup[LIndex].EntryId);
          Inc(Result);
        end;
      except
        on E: Exception do
          FTelemetry.JobDiscarded(nil,
            'outbox relay failed: ' + E.ClassName + ': ' + E.Message);
      end;
    end;
  finally
    LByQueue.Free;
  end;
end;

function THefestoServer.PromoteBatchCallbacks: Integer;
var
  LRequest: THefestoPublishRequest;
begin
  Result := 0;
  if not Assigned(FBatchService) or not CanRunLeaderDuties then
    Exit;

  for LRequest in FBatchService.PopReadyCallbacks(
    FServerOptions.BatchCallbackPromotionBudget) do
  begin
    try
      DispatchPublish(LRequest);
      Inc(Result);
    except
      FClientOutbox.Save(LRequest);
    end;
  end;
end;

function THefestoServer.PeriodicWindowKey(
  const ADefinition: THefestoPeriodicDefinition;
  const AWindow: TDateTime): string;
begin
  Result := 'periodic:' + ADefinition.Name + ':' + FormatDateTime('yyyymmddhhnn', AWindow);
end;

function THefestoServer.PeriodicPromotionBudget(
  const AValue: Integer): IHefestoServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.PeriodicPromotionBudget := NormalizePositiveBudget(
      AValue,
      THefestoServerOptions.Default.PeriodicPromotionBudget);
  finally
    FConfigLock.Release;
  end;
end;

procedure THefestoServer.SchedulePeriodicDefinition(
  const ADefinition: THefestoPeriodicDefinition;
  const AWindow: TDateTime);
var
  LAttributes: TStringList;
  LEntry: THefestoScheduledEntry;
  LQueueName: string;
  LFirstQueue: IHefestoQueueAdapter;
begin
  LQueueName := ADefinition.QueueName;
  if LQueueName.Trim.IsEmpty then
  begin
    FConfigLock.Acquire;
    try
      if Length(FQueues) > 0 then
        LFirstQueue := FQueues[0];
    finally
      FConfigLock.Release;
    end;
    if Assigned(LFirstQueue) then
      LQueueName := LFirstQueue.Name;
  end;

  LAttributes := TStringList.Create;
  try
    ApplyPeriodicAttributesToStrings(ADefinition, LAttributes);
    LAttributes.Values[THefestoJobAttribute.PeriodicName] := ADefinition.Name;
    LAttributes.Values[THefestoJobAttribute.PeriodicCron] := ADefinition.CronExpression;
    LEntry := MakeScheduledEntry(
      LQueueName,
      ADefinition.Action,
      ADefinition.Body,
      LAttributes,
      AWindow);
    FScheduledStore.Schedule(LEntry);
  finally
    LAttributes.Free;
  end;
end;

function THefestoServer.PromotePeriodic: Integer;
var
  LDefinition: THefestoPeriodicDefinition;
  LWindow: TDateTime;
  LKey: string;
  LBudget: Integer;
  LSnapshot: TArray<THefestoPeriodicDefinition>;
begin
  Result := 0;
  FPeriodicJobsLock.Acquire;
  try
    LSnapshot := Copy(FPeriodicJobs);
  finally
    FPeriodicJobsLock.Release;
  end;

  if (Length(LSnapshot) = 0)
    or (not Assigned(FScheduledStore))
    or (not Assigned(FStateStore))
    or (not CanRunLeaderDuties) then
    Exit;

  LWindow := THefestoCronSchedule.TruncateToMinute(Now);
  LBudget := FServerOptions.PeriodicPromotionBudget;
  for LDefinition in LSnapshot do
  begin
    if Result >= LBudget then
      Break;
    if not THefestoCronSchedule.Matches(LDefinition.CronExpression, LWindow) then
      Continue;

    LKey := PeriodicWindowKey(LDefinition, LWindow);
    if not FStateStore.TryPutIfAbsent(LKey, '1', 172800) then
      Continue;

    SchedulePeriodicDefinition(LDefinition, LWindow);
    Inc(Result);
  end;
end;

function THefestoServer.PromoteScheduled: Integer;
var
  LDue: TArray<THefestoScheduledEntry>;
  LEntry: THefestoScheduledEntry;
  LByQueue: TObjectDictionary<string, TList<THefestoPublishRequest>>;
  LGroup: TList<THefestoPublishRequest>;
  LAttributes: TStringList;
  LRequest: THefestoPublishRequest;
  LQueueName: string;
  LRequests: TArray<THefestoPublishRequest>;
  LIndex: Integer;
begin
  Result := 0;
  if not Assigned(FScheduledStore) or not CanRunLeaderDuties then
    Exit;

  LDue := FScheduledStore.PopDue(Now, FServerOptions.ScheduledPromotionBudget);
  if Length(LDue) = 0 then
    Exit;

  // Group due jobs by queue for pipeline dispatch
  LByQueue := TObjectDictionary<string, TList<THefestoPublishRequest>>.Create([doOwnsValues]);
  try
    for LEntry in LDue do
    begin
      LAttributes := TStringList.Create;
      try
        ApplyAttributesToStrings(LEntry, LAttributes);
        LRequest := MakePublishRequest(
          LEntry.QueueName, LEntry.Action, LEntry.Body, 0, LAttributes);
      finally
        LAttributes.Free;
      end;

      if not LByQueue.TryGetValue(LEntry.QueueName, LGroup) then
      begin
        LGroup := TList<THefestoPublishRequest>.Create;
        LByQueue.Add(LEntry.QueueName, LGroup);
      end;
      LGroup.Add(LRequest);
    end;

    for LQueueName in LByQueue.Keys do
    begin
      LGroup := LByQueue[LQueueName];
      SetLength(LRequests, LGroup.Count);
      for LIndex := 0 to Pred(LGroup.Count) do
        LRequests[LIndex] := LGroup[LIndex];
      try
        DispatchPublishBatch(LQueueName, LRequests);
        Inc(Result, LGroup.Count);
      except
        // Jobs already popped from scheduled store — save to outbox as fallback
        for LIndex := 0 to Pred(LGroup.Count) do
          FClientOutbox.Save(LGroup[LIndex]);
      end;
    end;
  finally
    LByQueue.Free;
  end;
end;

class function THefestoServer.QueuePauseKey(const AQueueName: string): string;
begin
  Result := 'queue:paused:' + AQueueName.Trim.ToLower;
end;

function THefestoServer.ActiveWorkerCount: Integer;
begin
  Result := ActiveWorkers;
end;

function THefestoServer.MaxConcurrencyCount: Integer;
begin
  FConfigLock.Acquire;
  try
    Result := FServerOptions.Concurrency;
  finally
    FConfigLock.Release;
  end;
end;

function THefestoServer.IsServerLeader: Boolean;
begin
  Result := IsLeader;
end;

function THefestoServer.IsServerDraining: Boolean;
begin
  Result := IsDraining;
end;

procedure THefestoServer.PauseQueueByName(const AQueueName: string);
begin
  PauseQueue(AQueueName);
end;

procedure THefestoServer.ResumeQueueByName(const AQueueName: string);
begin
  ResumeQueue(AQueueName);
end;

function THefestoServer.IsQueuePausedByName(const AQueueName: string): Boolean;
begin
  Result := IsQueuePausedInternal(AQueueName);
end;

function THefestoServer.ListPeriodicJobs: TArray<THefestoPeriodicDefinition>;
begin
  Result := GetPeriodicJobs;
end;

class function THefestoServer.TryResolveExpiration(
  const AValue: string;
  out AExpiresAt: TDateTime): Boolean;
var
  LUnix: Int64;
begin
  Result := False;
  if AValue.Trim.IsEmpty then
    Exit;

  if TryStrToInt64(AValue, LUnix) then
  begin
    AExpiresAt := UnixToDateTime(LUnix);
    Exit(True);
  end;

  try
    AExpiresAt := ISO8601ToDate(AValue);
    Result := True;
  except
    Result := False;
  end;
end;

end.
