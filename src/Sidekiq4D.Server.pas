unit Sidekiq4D.Server;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Diagnostics,
  System.DateUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Sidekiq4D.Job,
  Sidekiq4D.Metadata,
  Sidekiq4D.Batch,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Ack,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Options,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Locking,
  Sidekiq4D.Leader,
  Sidekiq4D.Idempotency,
  Sidekiq4D.ClientReliability,
  Sidekiq4D.Middleware,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Periodic,
  Sidekiq4D.Executor,
  Sidekiq4D.ServerReliability,
  Sidekiq4D.RateLimit,
  Sidekiq4D.WorkerPool,
  Sidekiq4D.DeadLetter,
  Sidekiq4D.Web;

type
  ISidekiqServer = interface(ISidekiqBatchEnqueuer)
    ['{BDB5E447-44F6-40FE-99B6-B13773E63DF4}']
    function UseQueue(const AQueue: ISidekiqQueueAdapter): ISidekiqServer;
    function BatchSize(const AValue: Integer): ISidekiqServer;
    function WaitTimeSeconds(const AValue: Integer): ISidekiqServer;
    function VisibilityTimeout(const AValue: Integer): ISidekiqServer;
    function IdleDelayMs(const AValue: Integer): ISidekiqServer;
    function Concurrency(const AValue: Integer): ISidekiqServer;
    function ClientOutboxBudget(const AValue: Integer): ISidekiqServer;
    function BatchCallbackPromotionBudget(const AValue: Integer): ISidekiqServer;
    function PeriodicPromotionBudget(const AValue: Integer): ISidekiqServer;
    function ScheduledPromotionBudget(const AValue: Integer): ISidekiqServer;
    function QueueConcurrency(
      const AQueueName: string;
      const AValue: Integer): ISidekiqServer;
    function QueueWeight(
      const AQueueName: string;
      const AValue: Integer): ISidekiqServer;
    function ActionConcurrency(
      const AAction: string;
      const AValue: Integer): ISidekiqServer;
    function MaxCycles(const AValue: Integer): ISidekiqServer;
    function StopWhenIdle(const AValue: Boolean = True): ISidekiqServer;
    function BeginRollingRestart: ISidekiqServer;
    function ResumeAfterRollingRestart: ISidekiqServer;
    function UseLeaderElection(const AValue: Boolean = True): ISidekiqServer;
    function LeaderName(const AValue: string): ISidekiqServer;
    function LeaderLeaseTtlSeconds(const AValue: Integer): ISidekiqServer;
    function IsLeader: Boolean;
    function IsDraining: Boolean;
    function ActiveWorkers: Integer;
    function PauseQueue(const AQueueName: string): ISidekiqServer;
    function ResumeQueue(const AQueueName: string): ISidekiqServer;
    function QueuePaused(const AQueueName: string): Boolean;
    function AckPolicy(const AValue: ISidekiqAckPolicy): ISidekiqServer;
    function RetryPolicy(const AValue: ISidekiqRetryPolicy): ISidekiqServer;
    function Telemetry(const AValue: ISidekiqTelemetry): ISidekiqServer;
    function StateStore(const AValue: ISidekiqStateStore): ISidekiqServer;
    function LockProvider(const AValue: ISidekiqLockProvider): ISidekiqServer;
    function Idempotency(const AValue: ISidekiqIdempotency): ISidekiqServer;
    function ClientOutbox(const AValue: ISidekiqClientOutbox): ISidekiqServer;
    function RateLimiter(const AValue: ISidekiqRateLimiter): ISidekiqServer;
    function ScheduledStore(const AValue: ISidekiqScheduledStore): ISidekiqServer;
    function RegisterPeriodic(
      const AName, ACronExpression, AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqServer;
    function UseJobRouter(const ARouter: ISidekiqJobRouter): ISidekiqServer;
    function UseClientMiddleware(
      const AValue: ISidekiqClientMiddleware): ISidekiqServer;
    function UseServerMiddleware(
      const AValue: ISidekiqServerMiddleware): ISidekiqServer;
    function RegisterHandler(
      const AAction: string;
      const AHandler: ISidekiqJobHandler): ISidekiqServer;
    function Batch(const ADescription: string = ''): ISidekiqBatch;
    function Enqueue(
      const AQueueName: string;
      const AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqServer;
    function EnqueueIn(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADelaySeconds: Integer;
      const AAttributes: TStrings = nil): ISidekiqServer;
    function FlushClientOutbox: Integer;
    function EnqueueAt(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): ISidekiqServer;
    function PromotePeriodic: Integer;
    function PromoteScheduled: Integer;
    function RunOnce: Integer;
    procedure Run;
    procedure Stop;
    // ── Novas features ────────────────────────────────────────────────
    { Dead Letter Queue: jobs que esgotam retries são registrados aqui. }
    function DeadLetterQueue(
      const AValue: ISidekiqDeadLetterQueue): ISidekiqServer;
    { Define várias filas com peso em uma única chamada. }
    function UseQueues(
      const AConfigs: TArray<TSidekiqQueueConfig>): ISidekiqServer;
    { Dashboard web embutido. Chame Start() antes de Run(). }
    function UseWebDashboard(
      const ADashboard: ISidekiqWebDashboard): ISidekiqServer;
    { Retorna snapshot dos jobs periódicos registrados. }
    function GetPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
  end;

  TSidekiqServer = class(TInterfacedObject, ISidekiqServer, ISidekiqBatchEnqueuer, ISidekiqDashboardServer)
  private
    FQueues: TArray<ISidekiqQueueAdapter>;
    FQueueConcurrencyLimits: TDictionary<string, Integer>;
    FActionConcurrencyLimits: TDictionary<string, Integer>;
    FQueueWeights: TDictionary<string, Integer>;
    FDispatcher: ISidekiqDispatcher;
    FFetchOptions: TSidekiqFetchOptions;
    FServerOptions: TSidekiqServerOptions;
    FAckPolicy: ISidekiqAckPolicy;
    FRetryPolicy: ISidekiqRetryPolicy;
    FTelemetry: ISidekiqTelemetry;
    FStateStore: ISidekiqStateStore;
    FLockProvider: ISidekiqLockProvider;
    FIdempotency: ISidekiqIdempotency;
    FClientOutbox: ISidekiqClientOutbox;
    FRateLimiter: ISidekiqRateLimiter;
    FHasCustomRateLimiter: Boolean;
    FScheduledStore: ISidekiqScheduledStore;
    FBatchService: ISidekiqBatchService;
    FServerReliability: ISidekiqServerReliability;
    FLeaderElection: ISidekiqLeaderElection;
    FPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
    FPeriodicJobsLock: TCriticalSection;
    FJobRouter: ISidekiqJobRouter;
    FClientMiddleware: TArray<ISidekiqClientMiddleware>;
    FServerMiddleware: TArray<ISidekiqServerMiddleware>;
    FDeadLetterQueue: ISidekiqDeadLetterQueue;
    FWebDashboard: ISidekiqWebDashboard;
    // Serialises all mutations to FQueues, FClientMiddleware, FServerMiddleware,
    // FQueueConcurrencyLimits, FActionConcurrencyLimits, and FQueueWeights.
    // These are written during server setup (possibly from different threads)
    // and read by the run-loop and worker threads concurrently.
    FConfigLock: TCriticalSection;
    FLifecycleLock: TObject;
    FStopRequested: Boolean;
    FDrainRequested: Boolean;
    FWorkerActivity: ISidekiqWorkerActivity;
    FFetchRotation: Integer;

    procedure EnsureConfigured;
    function BuildExecutor: ISidekiqJobExecutor;
    function BuildWorkerPool: ISidekiqWorkerPool;
    function BuildFetchQueueOrder: TArray<ISidekiqQueueAdapter>;
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
      const AQueue: ISidekiqQueueAdapter;
      const AJobs: TArray<ISidekiqJobEnvelope>;
      var ATarget: TArray<TSidekiqQueuedJob>);
    function IsQueuePausedInternal(const AQueueName: string): Boolean;
    function IsJobExpired(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure HandleExpiredJob(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope);
    function QueueWeightFor(const AQueueName: string): Integer;
    procedure AppendRunnableJobs(
      const ASource: TArray<TSidekiqQueuedJob>;
      var ATarget: TArray<TSidekiqQueuedJob>);
    function FindQueueByName(const AName: string): ISidekiqQueueAdapter;
    procedure DispatchPublish(const ARequest: TSidekiqPublishRequest);
    // Dispatch N requests to a single queue — uses ISidekiqQueueBatchPublisher
    // when available (one round-trip), falls back to individual Publish otherwise.
    procedure DispatchPublishBatch(
      const AQueueName: string;
      const ARequests: TArray<TSidekiqPublishRequest>);
    function PromoteBatchCallbacks: Integer;
    function PeriodicWindowKey(
      const ADefinition: TSidekiqPeriodicDefinition;
      const AWindow: TDateTime): string;
    procedure SchedulePeriodicDefinition(
      const ADefinition: TSidekiqPeriodicDefinition;
      const AWindow: TDateTime);
    procedure InvokeClientEnqueue(
      const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings;
      const AFinalAction: TSidekiqNextProc);
  public
    constructor Create;
    destructor Destroy; override;
    class function New: ISidekiqServer;

    function UseQueue(const AQueue: ISidekiqQueueAdapter): ISidekiqServer;
    function BatchSize(const AValue: Integer): ISidekiqServer;
    function WaitTimeSeconds(const AValue: Integer): ISidekiqServer;
    function VisibilityTimeout(const AValue: Integer): ISidekiqServer;
    function IdleDelayMs(const AValue: Integer): ISidekiqServer;
    function Concurrency(const AValue: Integer): ISidekiqServer;
    function ClientOutboxBudget(const AValue: Integer): ISidekiqServer;
    function BatchCallbackPromotionBudget(const AValue: Integer): ISidekiqServer;
    function PeriodicPromotionBudget(const AValue: Integer): ISidekiqServer;
    function ScheduledPromotionBudget(const AValue: Integer): ISidekiqServer;
    function QueueConcurrency(
      const AQueueName: string;
      const AValue: Integer): ISidekiqServer;
    function QueueWeight(
      const AQueueName: string;
      const AValue: Integer): ISidekiqServer;
    function ActionConcurrency(
      const AAction: string;
      const AValue: Integer): ISidekiqServer;
    function MaxCycles(const AValue: Integer): ISidekiqServer;
    function StopWhenIdle(const AValue: Boolean = True): ISidekiqServer;
    function BeginRollingRestart: ISidekiqServer;
    function ResumeAfterRollingRestart: ISidekiqServer;
    function UseLeaderElection(const AValue: Boolean = True): ISidekiqServer;
    function LeaderName(const AValue: string): ISidekiqServer;
    function LeaderLeaseTtlSeconds(const AValue: Integer): ISidekiqServer;
    function IsLeader: Boolean;
    function IsDraining: Boolean;
    function ActiveWorkers: Integer;
    function PauseQueue(const AQueueName: string): ISidekiqServer;
    function ResumeQueue(const AQueueName: string): ISidekiqServer;
    function QueuePaused(const AQueueName: string): Boolean;
    function AckPolicy(const AValue: ISidekiqAckPolicy): ISidekiqServer;
    function RetryPolicy(const AValue: ISidekiqRetryPolicy): ISidekiqServer;
    function Telemetry(const AValue: ISidekiqTelemetry): ISidekiqServer;
    function StateStore(const AValue: ISidekiqStateStore): ISidekiqServer;
    function LockProvider(const AValue: ISidekiqLockProvider): ISidekiqServer;
    function Idempotency(const AValue: ISidekiqIdempotency): ISidekiqServer;
    function ClientOutbox(const AValue: ISidekiqClientOutbox): ISidekiqServer;
    function RateLimiter(const AValue: ISidekiqRateLimiter): ISidekiqServer;
    function ScheduledStore(const AValue: ISidekiqScheduledStore): ISidekiqServer;
    function RegisterPeriodic(
      const AName, ACronExpression, AQueueName, AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqServer;
    function UseJobRouter(const ARouter: ISidekiqJobRouter): ISidekiqServer;
    function UseClientMiddleware(
      const AValue: ISidekiqClientMiddleware): ISidekiqServer;
    function UseServerMiddleware(
      const AValue: ISidekiqServerMiddleware): ISidekiqServer;
    function RegisterHandler(
      const AAction: string;
      const AHandler: ISidekiqJobHandler): ISidekiqServer;
    function Batch(const ADescription: string = ''): ISidekiqBatch;
    function Enqueue(
      const AQueueName: string;
      const AAction, ABody: string;
      const AAttributes: TStrings = nil): ISidekiqServer;
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
      const AAttributes: TStrings = nil): ISidekiqServer;
    function FlushClientOutbox: Integer;
    function EnqueueAt(
      const AQueueName: string;
      const AAction, ABody: string;
      const ADueAt: TDateTime;
      const AAttributes: TStrings = nil): ISidekiqServer;
    function PromotePeriodic: Integer;
    function PromoteScheduled: Integer;
    function RunOnce: Integer;
    procedure Run;
    procedure Stop;
    function DeadLetterQueue(
      const AValue: ISidekiqDeadLetterQueue): ISidekiqServer;
    function UseQueues(
      const AConfigs: TArray<TSidekiqQueueConfig>): ISidekiqServer;
    function UseWebDashboard(
      const ADashboard: ISidekiqWebDashboard): ISidekiqServer;
    function GetPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
    // ISidekiqDashboardServer
    function ActiveWorkerCount: Integer;
    function MaxConcurrencyCount: Integer;
    function IsServerLeader: Boolean;
    function IsServerDraining: Boolean;
    procedure PauseQueueByName(const AQueueName: string);
    procedure ResumeQueueByName(const AQueueName: string);
    function IsQueuePausedByName(const AQueueName: string): Boolean;
    function ListPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
  end;

implementation

uses
  Sidekiq4D.Store.InMemory;

{ TSidekiqServer }

function TSidekiqServer.BatchSize(const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FFetchOptions.BatchSize := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.BatchCallbackPromotionBudget(
  const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.BatchCallbackPromotionBudget := NormalizePositiveBudget(
      AValue,
      TSidekiqServerOptions.Default.BatchCallbackPromotionBudget);
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.BuildExecutor: ISidekiqJobExecutor;
var
  LMiddlewareSnapshot: TArray<ISidekiqServerMiddleware>;
begin
  FConfigLock.Acquire;
  try
    LMiddlewareSnapshot := Copy(FServerMiddleware);
  finally
    FConfigLock.Release;
  end;
  Result := TSidekiqJobExecutor.New(
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

function TSidekiqServer.AckPolicy(
  const AValue: ISidekiqAckPolicy): ISidekiqServer;
begin
  Result := Self;
  FAckPolicy := AValue;
end;

function TSidekiqServer.BuildWorkerPool: ISidekiqWorkerPool;
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
    Result := TSidekiqSequentialWorkerPool.New(
      FServerOptions.Concurrency,
      LQueueLimits,
      LActionLimits,
      FWorkerActivity);
  finally
    LQueueLimits.Free;
    LActionLimits.Free;
  end;
end;

function TSidekiqServer.BuildFetchQueueOrder: TArray<ISidekiqQueueAdapter>;
var
  LQueue: ISidekiqQueueAdapter;
  LWeight: Integer;
  LIndex: Integer;
  LSourceIndex: Integer;
  LLength: Integer;
  LOffset: Integer;
  LOrdered: TArray<ISidekiqQueueAdapter>;
  LQueueSnapshot: TArray<ISidekiqQueueAdapter>;
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

function TSidekiqServer.CanRunLeaderDuties: Boolean;
begin
  if not FServerOptions.LeaderElectionEnabled then
    Exit(True);
  Result := Assigned(FLeaderElection) and FLeaderElection.EnsureLeadership;
end;

function TSidekiqServer.ClientOutboxBudget(
  const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.ClientOutboxBudget := NormalizePositiveBudget(
      AValue,
      TSidekiqServerOptions.Default.ClientOutboxBudget);
  finally
    FConfigLock.Release;
  end;
end;

procedure TSidekiqServer.AppendRunnableJobs(
  const ASource: TArray<TSidekiqQueuedJob>;
  var ATarget: TArray<TSidekiqQueuedJob>);
var
  LQueuedJob: TSidekiqQueuedJob;
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

procedure TSidekiqServer.AppendFetchedJobs(
  const AQueue: ISidekiqQueueAdapter;
  const AJobs: TArray<ISidekiqJobEnvelope>;
  var ATarget: TArray<TSidekiqQueuedJob>);
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

constructor TSidekiqServer.Create;
begin
  inherited Create;
  FLifecycleLock := TObject.Create;
  FQueueConcurrencyLimits := TDictionary<string, Integer>.Create;
  FActionConcurrencyLimits := TDictionary<string, Integer>.Create;
  FQueueWeights := TDictionary<string, Integer>.Create;
  FDispatcher := TSidekiqDispatcher.New;
  FFetchOptions := TSidekiqFetchOptions.Default;
  FServerOptions := TSidekiqServerOptions.Default;
  FAckPolicy := TSidekiqDefaultAckPolicy.New;
  FRetryPolicy := TSidekiqSimpleRetryPolicy.New;
  FTelemetry := TSidekiqNoopTelemetry.New;
  FStateStore := TSidekiqInMemoryStateStore.New;
  FLockProvider := TSidekiqInMemoryLockProvider.New(FStateStore);
  FIdempotency := TSidekiqStateStoreIdempotency.New(FStateStore);
  FClientOutbox := TSidekiqFileClientOutbox.NewDefault;
  FRateLimiter := TSidekiqTokenBucketRateLimiter.New(FStateStore);
  FHasCustomRateLimiter := False;
  FScheduledStore := TSidekiqInMemoryScheduledStore.New;
  FBatchService := TSidekiqBatchStateStore.New(FStateStore);
  FServerReliability := TSidekiqServerReliability.New(
    FStateStore,
    FLockProvider,
    FIdempotency);
  FJobRouter := TSidekiqPassThroughRouter.Create;
  FWorkerActivity := TSidekiqWorkerActivity.New;
  FConfigLock := TCriticalSection.Create;
  FPeriodicJobsLock := TCriticalSection.Create;
  RebuildLeaderElection;
  SetLength(FPeriodicJobs, 0);
  SetLength(FClientMiddleware, 0);
  SetLength(FServerMiddleware, 0);
end;

function TSidekiqServer.CreateNormalizedAttributes(
  const AAttributes: TStrings;
  const AReferenceTime: TDateTime): TStringList;
var
  LReferenceTime: TDateTime;
  LExpiresInSeconds: Integer;
begin
  Result := TStringList.Create;
  if Assigned(AAttributes) then
    Result.Assign(AAttributes);

  if not Result.Values[TSidekiqJobAttribute.ExpiresAt].Trim.IsEmpty then
    Exit;

  LExpiresInSeconds := StrToIntDef(
    Result.Values[TSidekiqJobAttribute.ExpiresInSeconds],
    0);
  if LExpiresInSeconds <= 0 then
    Exit;

  LReferenceTime := AReferenceTime;
  if LReferenceTime <= 0 then
    LReferenceTime := Now;
  Result.Values[TSidekiqJobAttribute.ExpiresAt] :=
    IntToStr(DateTimeToUnix(IncSecond(LReferenceTime, LExpiresInSeconds)));
end;

destructor TSidekiqServer.Destroy;
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

function TSidekiqServer.ActiveWorkers: Integer;
begin
  if Assigned(FWorkerActivity) then
    Result := FWorkerActivity.ActiveWorkers
  else
    Result := 0;
end;

function TSidekiqServer.ActionConcurrency(
  const AAction: string;
  const AValue: Integer): ISidekiqServer;
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

function TSidekiqServer.Concurrency(const AValue: Integer): ISidekiqServer;
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

class function TSidekiqServer.NormalizePositiveBudget(
  const AValue, ADefault: Integer): Integer;
begin
  if AValue > 0 then
    Result := AValue
  else
    Result := ADefault;
end;

procedure TSidekiqServer.EnsureConfigured;
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

procedure TSidekiqServer.HandleExpiredJob(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope);
begin
  FAckPolicy.OnDiscard(AQueue, AJob, 'job expired before execution', FTelemetry);
  if Assigned(FBatchService) then
    FBatchService.RecordFinalFailure(AJob.Attribute(TSidekiqJobAttribute.BatchId));
end;

function TSidekiqServer.Idempotency(
  const AValue: ISidekiqIdempotency): ISidekiqServer;
begin
  Result := Self;
  FIdempotency := AValue;
end;

function TSidekiqServer.IdleDelayMs(const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.IdleDelayMs := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.IsLeader: Boolean;
begin
  if not FServerOptions.LeaderElectionEnabled then
    Exit(True);
  Result := Assigned(FLeaderElection) and FLeaderElection.IsLeader;
end;

function TSidekiqServer.IsDraining: Boolean;
begin
  Result := DrainRequested;
end;

function TSidekiqServer.IsJobExpired(const AJob: ISidekiqJobEnvelope): Boolean;
var
  LExpiresAt: TDateTime;
begin
  Result := TryResolveExpiration(
    AJob.Attribute(TSidekiqJobAttribute.ExpiresAt),
    LExpiresAt)
    and (Now >= LExpiresAt);
end;

function TSidekiqServer.IsQueuePausedInternal(const AQueueName: string): Boolean;
begin
  Result := Assigned(FStateStore)
    and not AQueueName.Trim.IsEmpty
    and FStateStore.Exists(QueuePauseKey(AQueueName));
end;

function TSidekiqServer.ClientOutbox(
  const AValue: ISidekiqClientOutbox): ISidekiqServer;
begin
  Result := Self;
  if Assigned(AValue) then
    FClientOutbox := AValue
  else
    FClientOutbox := TSidekiqFileClientOutbox.NewDefault;
end;

function TSidekiqServer.RateLimiter(
  const AValue: ISidekiqRateLimiter): ISidekiqServer;
begin
  Result := Self;
  if Assigned(AValue) then
  begin
    FRateLimiter := AValue;
    FHasCustomRateLimiter := True;
  end
  else
  begin
    FRateLimiter := TSidekiqTokenBucketRateLimiter.New(FStateStore);
    FHasCustomRateLimiter := False;
  end;
end;

function TSidekiqServer.MaxCycles(const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.MaxCycles := AValue;
  finally
    FConfigLock.Release;
  end;
end;

class function TSidekiqServer.New: ISidekiqServer;
begin
  Result := TSidekiqServer.Create;
end;

function TSidekiqServer.LeaderLeaseTtlSeconds(
  const AValue: Integer): ISidekiqServer;
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

function TSidekiqServer.LeaderName(const AValue: string): ISidekiqServer;
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

function TSidekiqServer.PauseQueue(const AQueueName: string): ISidekiqServer;
begin
  Result := Self;
  if AQueueName.Trim.IsEmpty then
    Exit;
  FStateStore.Put(QueuePauseKey(AQueueName), '1');
end;

function TSidekiqServer.QueuePaused(const AQueueName: string): Boolean;
begin
  Result := IsQueuePausedInternal(AQueueName);
end;

function TSidekiqServer.QueueConcurrency(
  const AQueueName: string;
  const AValue: Integer): ISidekiqServer;
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

function TSidekiqServer.QueueWeight(
  const AQueueName: string;
  const AValue: Integer): ISidekiqServer;
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

function TSidekiqServer.QueueWeightFor(const AQueueName: string): Integer;
begin
  FConfigLock.Acquire;
  try
    if not FQueueWeights.TryGetValue(AQueueName, Result) or (Result <= 0) then
      Result := 1;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.RegisterHandler(
  const AAction: string;
  const AHandler: ISidekiqJobHandler): ISidekiqServer;
begin
  Result := Self;
  FDispatcher.RegisterHandler(AAction, AHandler);
end;

function TSidekiqServer.ResumeQueue(const AQueueName: string): ISidekiqServer;
begin
  Result := Self;
  if AQueueName.Trim.IsEmpty then
    Exit;
  FStateStore.Delete(QueuePauseKey(AQueueName));
end;

function TSidekiqServer.ScheduledPromotionBudget(
  const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.ScheduledPromotionBudget := NormalizePositiveBudget(
      AValue,
      TSidekiqServerOptions.Default.ScheduledPromotionBudget);
  finally
    FConfigLock.Release;
  end;
end;

procedure TSidekiqServer.RebuildLeaderElection;
begin
  if Assigned(FLeaderElection) then
    FLeaderElection.Release;

  if FServerOptions.LeaderElectionEnabled then
    FLeaderElection := TSidekiqLeaderElection.New(
      FLockProvider,
      FServerOptions.LeaderName,
      FServerOptions.LeaderLeaseTtlSeconds)
  else
    FLeaderElection := nil;
end;

function TSidekiqServer.Batch(const ADescription: string): ISidekiqBatch;
begin
  Result := TSidekiqBatchBuilder.New(
    FBatchService.CreateBatch(ADescription),
    ADescription,
    Self as ISidekiqBatchEnqueuer,
    FBatchService);
end;

function TSidekiqServer.RetryPolicy(
  const AValue: ISidekiqRetryPolicy): ISidekiqServer;
begin
  Result := Self;
  FRetryPolicy := AValue;
end;

procedure TSidekiqServer.Run;
var
  LProcessed: Integer;
  LCycle: Integer;
  LOpts: TSidekiqServerOptions;
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

function TSidekiqServer.RunOnce: Integer;
var
  LFetchedJobs: TArray<ISidekiqJobEnvelope>;
  LQueuedJobs: TArray<TSidekiqQueuedJob>;
  LRunnableJobs: TArray<TSidekiqQueuedJob>;
  LQueue: ISidekiqQueueAdapter;
  LFetchQueue: ISidekiqQueueAdapter;
  LFetchOrder: TArray<ISidekiqQueueAdapter>;
  LExecutor: ISidekiqJobExecutor;
  LWorkerPool: ISidekiqWorkerPool;
  LFetchOpts: TSidekiqFetchOptions;
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

procedure TSidekiqServer.Stop;
begin
  SetStopRequested(True);
  if Assigned(FWebDashboard) then
    FWebDashboard.Stop;
end;

function TSidekiqServer.DeadLetterQueue(
  const AValue: ISidekiqDeadLetterQueue): ISidekiqServer;
begin
  Result := Self;
  FDeadLetterQueue := AValue;
end;

function TSidekiqServer.UseQueues(
  const AConfigs: TArray<TSidekiqQueueConfig>): ISidekiqServer;
var
  LConfig: TSidekiqQueueConfig;
begin
  Result := Self;
  for LConfig in AConfigs do
    QueueWeight(LConfig.Name, LConfig.Weight);
end;

function TSidekiqServer.UseWebDashboard(
  const ADashboard: ISidekiqWebDashboard): ISidekiqServer;
begin
  Result := Self;
  FWebDashboard := ADashboard;
end;

function TSidekiqServer.GetPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
begin
  FPeriodicJobsLock.Acquire;
  try
    Result := Copy(FPeriodicJobs);
  finally
    FPeriodicJobsLock.Release;
  end;
end;

function TSidekiqServer.StopRequested: Boolean;
begin
  TMonitor.Enter(FLifecycleLock);
  try
    Result := FStopRequested;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

procedure TSidekiqServer.SetStopRequested(const AValue: Boolean);
begin
  TMonitor.Enter(FLifecycleLock);
  try
    FStopRequested := AValue;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

function TSidekiqServer.DrainRequested: Boolean;
begin
  TMonitor.Enter(FLifecycleLock);
  try
    Result := FDrainRequested;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

procedure TSidekiqServer.SetDrainRequested(const AValue: Boolean);
begin
  TMonitor.Enter(FLifecycleLock);
  try
    FDrainRequested := AValue;
  finally
    TMonitor.Exit(FLifecycleLock);
  end;
end;

function TSidekiqServer.StateStore(
  const AValue: ISidekiqStateStore): ISidekiqServer;
begin
  Result := Self;
  FStateStore := AValue;
  FLockProvider := TSidekiqInMemoryLockProvider.New(FStateStore);
  FIdempotency := TSidekiqStateStoreIdempotency.New(FStateStore);
  if not FHasCustomRateLimiter then
    FRateLimiter := TSidekiqTokenBucketRateLimiter.New(FStateStore);
  FBatchService := TSidekiqBatchStateStore.New(FStateStore);
  FServerReliability := TSidekiqServerReliability.New(
    FStateStore,
    FLockProvider,
    FIdempotency);
  RebuildLeaderElection;
end;

function TSidekiqServer.LockProvider(
  const AValue: ISidekiqLockProvider): ISidekiqServer;
begin
  Result := Self;
  FLockProvider := AValue;
  FServerReliability := TSidekiqServerReliability.New(
    FStateStore,
    FLockProvider,
    FIdempotency);
  RebuildLeaderElection;
end;

procedure TSidekiqServer.DispatchPublish(const ARequest: TSidekiqPublishRequest);
var
  LQueue: ISidekiqQueueAdapter;
  LPublisher: ISidekiqQueuePublisher;
  LRequest: TSidekiqPublishRequest;
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

  if not Supports(LQueue, ISidekiqQueuePublisher, LPublisher) then
    raise Exception.CreateFmt(
      'Queue adapter "%s" does not support client publishing.',
      [LQueue.Name]);

  LPublisher.Publish(LRequest);
end;

procedure TSidekiqServer.DispatchPublishBatch(
  const AQueueName: string;
  const ARequests: TArray<TSidekiqPublishRequest>);
var
  LQueue: ISidekiqQueueAdapter;
  LBatchPublisher: ISidekiqQueueBatchPublisher;
  LPublisher: ISidekiqQueuePublisher;
  LRequest: TSidekiqPublishRequest;
begin
  if Length(ARequests) = 0 then
    Exit;

  LQueue := FindQueueByName(AQueueName);
  if not Assigned(LQueue) then
    raise Exception.CreateFmt(
      'Queue adapter not configured for publish: %s', [AQueueName]);

  // Prefer batch (one round-trip) when the adapter supports it
  if Supports(LQueue, ISidekiqQueueBatchPublisher, LBatchPublisher) then
  begin
    LBatchPublisher.PublishBatch(ARequests);
    Exit;
  end;

  // Fallback: individual publish for adapters without batch support
  if not Supports(LQueue, ISidekiqQueuePublisher, LPublisher) then
    raise Exception.CreateFmt(
      'Queue adapter "%s" does not support client publishing.', [LQueue.Name]);

  for LRequest in ARequests do
    LPublisher.Publish(LRequest);
end;

function TSidekiqServer.StopWhenIdle(const AValue: Boolean): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.StopWhenIdle := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.BeginRollingRestart: ISidekiqServer;
begin
  Result := Self;
  SetDrainRequested(True);
  SetStopRequested(True);
end;

function TSidekiqServer.ResumeAfterRollingRestart: ISidekiqServer;
begin
  Result := Self;
  SetDrainRequested(False);
  SetStopRequested(False);
end;

function TSidekiqServer.UseLeaderElection(
  const AValue: Boolean): ISidekiqServer;
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

function TSidekiqServer.Telemetry(
  const AValue: ISidekiqTelemetry): ISidekiqServer;
begin
  Result := Self;
  FTelemetry := AValue;
end;

function TSidekiqServer.UseQueue(
  const AQueue: ISidekiqQueueAdapter): ISidekiqServer;
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

function TSidekiqServer.Enqueue(
  const AQueueName: string;
  const AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqServer;
var
  LRequest: TSidekiqPublishRequest;
  LNormalizedAttributes: TStringList;
  LTargetQueue: string;
  LFirstQueue: ISidekiqQueueAdapter;
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

procedure TSidekiqServer.EnqueueNow(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings);
begin
  Enqueue(AQueueName, AAction, ABody, AAttributes);
end;

function TSidekiqServer.VisibilityTimeout(const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FFetchOptions.VisibilityTimeout := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.WaitTimeSeconds(const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FFetchOptions.WaitTimeSeconds := AValue;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.ScheduledStore(
  const AValue: ISidekiqScheduledStore): ISidekiqServer;
begin
  Result := Self;
  if Assigned(AValue) then
    FScheduledStore := AValue
  else
    FScheduledStore := TSidekiqInMemoryScheduledStore.New;
end;

function TSidekiqServer.RegisterPeriodic(
  const AName, ACronExpression, AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqServer;
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

function TSidekiqServer.UseClientMiddleware(
  const AValue: ISidekiqClientMiddleware): ISidekiqServer;
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

function TSidekiqServer.UseServerMiddleware(
  const AValue: ISidekiqServerMiddleware): ISidekiqServer;
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

function TSidekiqServer.UseJobRouter(
  const ARouter: ISidekiqJobRouter): ISidekiqServer;
begin
  Result := Self;
  if Assigned(ARouter) then
    FJobRouter := ARouter;
end;

function TSidekiqServer.FindQueueByName(
  const AName: string): ISidekiqQueueAdapter;
var
  LQueue: ISidekiqQueueAdapter;
  LSnapshot: TArray<ISidekiqQueueAdapter>;
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

procedure TSidekiqServer.InvokeClientEnqueue(
  const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings;
  const AFinalAction: TSidekiqNextProc);
var
  LChain: TSidekiqClientMiddlewareChain;
  LSnapshot: TArray<ISidekiqClientMiddleware>;
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
  LChain := TSidekiqClientMiddlewareChain.Create(LSnapshot);
  try
    LChain.Invoke(AQueueName, AAction, ABody, AAttributes, AFinalAction);
  finally
    LChain.Free;
  end;
end;

function TSidekiqServer.EnqueueIn(
  const AQueueName: string;
  const AAction, ABody: string;
  const ADelaySeconds: Integer;
  const AAttributes: TStrings): ISidekiqServer;
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

function TSidekiqServer.EnqueueAt(
  const AQueueName: string;
  const AAction, ABody: string;
  const ADueAt: TDateTime;
  const AAttributes: TStrings): ISidekiqServer;
var
  LEntry: TSidekiqScheduledEntry;
  LNormalizedAttributes: TStringList;
  LTargetQueue: string;
  LFirstQueue: ISidekiqQueueAdapter;
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

procedure TSidekiqServer.ScheduleAt(
  const AQueueName, AAction, ABody: string;
  const ADueAt: TDateTime;
  const AAttributes: TStrings);
begin
  EnqueueAt(AQueueName, AAction, ABody, ADueAt, AAttributes);
end;

function TSidekiqServer.FlushClientOutbox: Integer;
var
  LAllEntries: TArray<TSidekiqOutboxEntry>;
  LEntry: TSidekiqOutboxEntry;
  LBudget: Integer;
  LByQueue: TObjectDictionary<string, TList<TSidekiqOutboxEntry>>;
  LGroup: TList<TSidekiqOutboxEntry>;
  LQueueName: string;
  LRequests: TArray<TSidekiqPublishRequest>;
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

  LByQueue := TObjectDictionary<string, TList<TSidekiqOutboxEntry>>.Create([doOwnsValues]);
  try
    for LEntry in LAllEntries do
    begin
      if not LByQueue.TryGetValue(LEntry.Request.QueueName, LGroup) then
      begin
        LGroup := TList<TSidekiqOutboxEntry>.Create;
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

function TSidekiqServer.PromoteBatchCallbacks: Integer;
var
  LRequest: TSidekiqPublishRequest;
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

function TSidekiqServer.PeriodicWindowKey(
  const ADefinition: TSidekiqPeriodicDefinition;
  const AWindow: TDateTime): string;
begin
  Result := 'periodic:' + ADefinition.Name + ':' + FormatDateTime('yyyymmddhhnn', AWindow);
end;

function TSidekiqServer.PeriodicPromotionBudget(
  const AValue: Integer): ISidekiqServer;
begin
  Result := Self;
  FConfigLock.Acquire;
  try
    FServerOptions.PeriodicPromotionBudget := NormalizePositiveBudget(
      AValue,
      TSidekiqServerOptions.Default.PeriodicPromotionBudget);
  finally
    FConfigLock.Release;
  end;
end;

procedure TSidekiqServer.SchedulePeriodicDefinition(
  const ADefinition: TSidekiqPeriodicDefinition;
  const AWindow: TDateTime);
var
  LAttributes: TStringList;
  LEntry: TSidekiqScheduledEntry;
  LQueueName: string;
  LFirstQueue: ISidekiqQueueAdapter;
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
    LAttributes.Values[TSidekiqJobAttribute.PeriodicName] := ADefinition.Name;
    LAttributes.Values[TSidekiqJobAttribute.PeriodicCron] := ADefinition.CronExpression;
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

function TSidekiqServer.PromotePeriodic: Integer;
var
  LDefinition: TSidekiqPeriodicDefinition;
  LWindow: TDateTime;
  LKey: string;
  LBudget: Integer;
  LSnapshot: TArray<TSidekiqPeriodicDefinition>;
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

  LWindow := TSidekiqCronSchedule.TruncateToMinute(Now);
  LBudget := FServerOptions.PeriodicPromotionBudget;
  for LDefinition in LSnapshot do
  begin
    if Result >= LBudget then
      Break;
    if not TSidekiqCronSchedule.Matches(LDefinition.CronExpression, LWindow) then
      Continue;

    LKey := PeriodicWindowKey(LDefinition, LWindow);
    if not FStateStore.TryPutIfAbsent(LKey, '1', 172800) then
      Continue;

    SchedulePeriodicDefinition(LDefinition, LWindow);
    Inc(Result);
  end;
end;

function TSidekiqServer.PromoteScheduled: Integer;
var
  LDue: TArray<TSidekiqScheduledEntry>;
  LEntry: TSidekiqScheduledEntry;
  LByQueue: TObjectDictionary<string, TList<TSidekiqPublishRequest>>;
  LGroup: TList<TSidekiqPublishRequest>;
  LAttributes: TStringList;
  LRequest: TSidekiqPublishRequest;
  LQueueName: string;
  LRequests: TArray<TSidekiqPublishRequest>;
  LIndex: Integer;
begin
  Result := 0;
  if not Assigned(FScheduledStore) or not CanRunLeaderDuties then
    Exit;

  LDue := FScheduledStore.PopDue(Now, FServerOptions.ScheduledPromotionBudget);
  if Length(LDue) = 0 then
    Exit;

  // Group due jobs by queue for pipeline dispatch
  LByQueue := TObjectDictionary<string, TList<TSidekiqPublishRequest>>.Create([doOwnsValues]);
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
        LGroup := TList<TSidekiqPublishRequest>.Create;
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

class function TSidekiqServer.QueuePauseKey(const AQueueName: string): string;
begin
  Result := 'queue:paused:' + AQueueName.Trim.ToLower;
end;

function TSidekiqServer.ActiveWorkerCount: Integer;
begin
  Result := ActiveWorkers;
end;

function TSidekiqServer.MaxConcurrencyCount: Integer;
begin
  FConfigLock.Acquire;
  try
    Result := FServerOptions.Concurrency;
  finally
    FConfigLock.Release;
  end;
end;

function TSidekiqServer.IsServerLeader: Boolean;
begin
  Result := IsLeader;
end;

function TSidekiqServer.IsServerDraining: Boolean;
begin
  Result := IsDraining;
end;

procedure TSidekiqServer.PauseQueueByName(const AQueueName: string);
begin
  PauseQueue(AQueueName);
end;

procedure TSidekiqServer.ResumeQueueByName(const AQueueName: string);
begin
  ResumeQueue(AQueueName);
end;

function TSidekiqServer.IsQueuePausedByName(const AQueueName: string): Boolean;
begin
  Result := IsQueuePausedInternal(AQueueName);
end;

function TSidekiqServer.ListPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
begin
  Result := GetPeriodicJobs;
end;

class function TSidekiqServer.TryResolveExpiration(
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
