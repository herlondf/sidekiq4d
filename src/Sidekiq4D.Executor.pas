unit Sidekiq4D.Executor;

interface

uses
  System.SysUtils,
  System.Diagnostics,
  Sidekiq4D.Job,
  Sidekiq4D.Metadata,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Ack,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Unique,
  Sidekiq4D.Batch,
  Sidekiq4D.Middleware,
  Sidekiq4D.RateLimit,
  Sidekiq4D.ServerReliability,
  Sidekiq4D.DeadLetter;

type
  ISidekiqJobExecutor = interface
    ['{9B46E84A-392E-417D-8CCF-47CB4977C5C7}']
    procedure Execute(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AVisibilityTimeout: Integer);
  end;

  TSidekiqJobExecutor = class(TInterfacedObject, ISidekiqJobExecutor)
  private
    FDispatcher: ISidekiqDispatcher;
    FAckPolicy: ISidekiqAckPolicy;
    FRetryPolicy: ISidekiqRetryPolicy;
    FTelemetry: ISidekiqTelemetry;
    FStateStore: ISidekiqStateStore;
    FLockProvider: ISidekiqLockProvider;
    FIdempotency: ISidekiqIdempotency;
    FRateLimiter: ISidekiqRateLimiter;
    FBatchService: ISidekiqBatchService;
    FServerMiddleware: TArray<ISidekiqServerMiddleware>;
    FServerReliability: ISidekiqServerReliability;
    FDeadLetterQueue: ISidekiqDeadLetterQueue;

    class function ResolveLogicalKey(
      const AJob: ISidekiqJobEnvelope): string; static;
    class function ResolveTtl(
      const AJob: ISidekiqJobEnvelope;
      const AAttributeName: string;
      const ADefault: Integer): Integer; static;
    class function ResolveProcessingTtl(
      const AJob: ISidekiqJobEnvelope): Integer; static;
    class function ResolveCompletionTtl(
      const AJob: ISidekiqJobEnvelope): Integer; static;
    class function ResolveBatchId(
      const AJob: ISidekiqJobEnvelope): string; static;
    procedure HandleSuccess(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AIdempotencyKey: string;
      const AWatch: TStopwatch);
    procedure HandleFailure(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AIdempotencyKey: string;
      const AError: Exception;
      const AWatch: TStopwatch);
    function TryEnterIdempotency(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      out AIdempotencyKey: string;
      out AAlreadyCompleted: Boolean): Boolean;
    function TryAcquireRateLimit(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AIdempotencyKey: string): Boolean;
    function TryAcquireLock(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AIdempotencyKey: string): ISidekiqLockHandle;
  public
    constructor Create(
      const ADispatcher: ISidekiqDispatcher;
      const AAckPolicy: ISidekiqAckPolicy;
      const ARetryPolicy: ISidekiqRetryPolicy;
      const ATelemetry: ISidekiqTelemetry;
      const AStateStore: ISidekiqStateStore;
      const ALockProvider: ISidekiqLockProvider;
      const AIdempotency: ISidekiqIdempotency;
      const ARateLimiter: ISidekiqRateLimiter;
      const ABatchService: ISidekiqBatchService;
      const AServerMiddleware: TArray<ISidekiqServerMiddleware>;
      const AServerReliability: ISidekiqServerReliability;
      const ADeadLetterQueue: ISidekiqDeadLetterQueue = nil);
    class function New(
      const ADispatcher: ISidekiqDispatcher;
      const AAckPolicy: ISidekiqAckPolicy;
      const ARetryPolicy: ISidekiqRetryPolicy;
      const ATelemetry: ISidekiqTelemetry;
      const AStateStore: ISidekiqStateStore;
      const ALockProvider: ISidekiqLockProvider;
      const AIdempotency: ISidekiqIdempotency;
      const ARateLimiter: ISidekiqRateLimiter;
      const ABatchService: ISidekiqBatchService = nil;
      const AServerMiddleware: TArray<ISidekiqServerMiddleware> = nil;
      const AServerReliability: ISidekiqServerReliability = nil;
      const ADeadLetterQueue: ISidekiqDeadLetterQueue = nil): ISidekiqJobExecutor;

    procedure Execute(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AVisibilityTimeout: Integer);
  end;

implementation

{ TSidekiqJobExecutor }

constructor TSidekiqJobExecutor.Create(
  const ADispatcher: ISidekiqDispatcher;
  const AAckPolicy: ISidekiqAckPolicy;
  const ARetryPolicy: ISidekiqRetryPolicy;
  const ATelemetry: ISidekiqTelemetry;
  const AStateStore: ISidekiqStateStore;
  const ALockProvider: ISidekiqLockProvider;
  const AIdempotency: ISidekiqIdempotency;
  const ARateLimiter: ISidekiqRateLimiter;
  const ABatchService: ISidekiqBatchService;
  const AServerMiddleware: TArray<ISidekiqServerMiddleware>;
  const AServerReliability: ISidekiqServerReliability;
  const ADeadLetterQueue: ISidekiqDeadLetterQueue);
begin
  inherited Create;
  FDispatcher := ADispatcher;
  FAckPolicy := AAckPolicy;
  FRetryPolicy := ARetryPolicy;
  FTelemetry := ATelemetry;
  FStateStore := AStateStore;
  FLockProvider := ALockProvider;
  FIdempotency := AIdempotency;
  FRateLimiter := ARateLimiter;
  FBatchService := ABatchService;
  FServerMiddleware := AServerMiddleware;
  FServerReliability := AServerReliability;
  FDeadLetterQueue := ADeadLetterQueue;
end;

procedure TSidekiqJobExecutor.Execute(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AVisibilityTimeout: Integer);
var
  LHandler: ISidekiqJobHandler;
  LContext: ISidekiqJobContext;
  LWatch: TStopwatch;
  LLockHandle: ISidekiqLockHandle;
  LExecutionLease: ISidekiqExecutionLease;
  LIdempotencyKey: string;
  LBatchId: string;
  LAlreadyCompleted: Boolean;
  LChain: TSidekiqServerMiddlewareChain;
begin
  LWatch := TStopwatch.StartNew;
  FTelemetry.JobStarted(AJob);
  try
    try
      LBatchId := ResolveBatchId(AJob);
      LAlreadyCompleted := False;
      if not TryEnterIdempotency(AQueue, AJob, LIdempotencyKey, LAlreadyCompleted) then
      begin
        if LAlreadyCompleted and Assigned(FBatchService) then
          FBatchService.RecordSuccess(LBatchId);
        Exit;
      end;

      if not TryAcquireRateLimit(AQueue, AJob, LIdempotencyKey) then
        Exit;

      LLockHandle := TryAcquireLock(AQueue, AJob, LIdempotencyKey);
      if (not AJob.Attribute('lock_key').IsEmpty or not AJob.Attribute('idempotency_key').IsEmpty)
        and (not Assigned(LLockHandle))
        and (not ResolveLogicalKey(AJob).IsEmpty) then
        Exit;

      if Assigned(FServerReliability) then
        LExecutionLease := FServerReliability.BeginExecution(
          AQueue,
          AJob,
          AVisibilityTimeout,
          LIdempotencyKey,
          LLockHandle);

      LHandler := FDispatcher.Resolve(AJob);
      LContext := TSidekiqJobContext.New(
        AJob,
        FStateStore,
        FLockProvider,
        FIdempotency,
        FRateLimiter);

      if Length(FServerMiddleware) = 0 then
        LHandler.Perform(LContext)
      else
      begin
        LChain := TSidekiqServerMiddlewareChain.Create(FServerMiddleware);
        try
          LChain.Invoke(AQueue, AJob,
            procedure
            begin
              LHandler.Perform(LContext);
            end);
        finally
          LChain.Free;
        end;
      end;

      HandleSuccess(AQueue, AJob, LIdempotencyKey, LWatch);
    except
      on E: Exception do
        HandleFailure(AQueue, AJob, LIdempotencyKey, E, LWatch);
    end;
  finally
    LExecutionLease := nil;
    if Assigned(LLockHandle) then
      LLockHandle.Release;
  end;
end;

procedure TSidekiqJobExecutor.HandleFailure(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AIdempotencyKey: string;
  const AError: Exception;
  const AWatch: TStopwatch);
var
  LDecision: TSidekiqRetryDecision;
  LStrategy: TSidekiqUniqueStrategy;
  LBatchId: string;
begin
  LBatchId := ResolveBatchId(AJob);
  if not AIdempotencyKey.IsEmpty then
  begin
    LStrategy := TSidekiqUniqueStrategy.FromJob(AJob);
    if LStrategy = us_UntilExpired then
      FIdempotency.MarkCompleted(AIdempotencyKey, ResolveCompletionTtl(AJob))
    else
      FIdempotency.Clear(AIdempotencyKey);
  end;

  FTelemetry.JobFailed(AJob, AError, AWatch.ElapsedMilliseconds);
  LDecision := FRetryPolicy.Decide(AJob, AError);
  case LDecision.Kind of
    rdRetry:
      FAckPolicy.OnRetry(AQueue, AJob, LDecision.DelaySeconds, FTelemetry);
    rdDeadLetter:
      begin
        FAckPolicy.OnDeadLetter(AQueue, AJob, LDecision.Reason, FTelemetry);
        if Assigned(FBatchService) then
          FBatchService.RecordFinalFailure(LBatchId);
        if Assigned(FDeadLetterQueue) then
        begin
          var LDlqEntry: TSidekiqDeadLetterEntry;
          LDlqEntry.JobId         := AJob.Id;
          LDlqEntry.JobJson       := AJob.Body;
          LDlqEntry.Action        := AJob.Action;
          LDlqEntry.OriginalQueue := AQueue.Name;
          LDlqEntry.ErrorMessage  := AError.Message;
          LDlqEntry.RetryCount    := AJob.Attempts;
          LDlqEntry.FailedAt      := Now;
          FDeadLetterQueue.Push(LDlqEntry);
        end;
      end;
    rdDiscard:
      begin
        FAckPolicy.OnDiscard(AQueue, AJob, LDecision.Reason, FTelemetry);
        if Assigned(FBatchService) then
          FBatchService.RecordFinalFailure(LBatchId);
      end;
    rdReraise:
      begin
        if Assigned(FBatchService) then
          FBatchService.RecordFinalFailure(LBatchId);
        raise AError;
      end;
  end;
end;

procedure TSidekiqJobExecutor.HandleSuccess(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AIdempotencyKey: string;
  const AWatch: TStopwatch);
begin
  if not AIdempotencyKey.IsEmpty then
    FIdempotency.MarkCompleted(AIdempotencyKey, ResolveCompletionTtl(AJob));

  if Assigned(FBatchService) then
    FBatchService.RecordSuccess(ResolveBatchId(AJob));
  FAckPolicy.OnSuccess(AQueue, AJob, FTelemetry);
  FTelemetry.JobSucceeded(AJob, AWatch.ElapsedMilliseconds);
end;

class function TSidekiqJobExecutor.New(
  const ADispatcher: ISidekiqDispatcher;
  const AAckPolicy: ISidekiqAckPolicy;
  const ARetryPolicy: ISidekiqRetryPolicy;
  const ATelemetry: ISidekiqTelemetry;
  const AStateStore: ISidekiqStateStore;
  const ALockProvider: ISidekiqLockProvider;
  const AIdempotency: ISidekiqIdempotency;
  const ARateLimiter: ISidekiqRateLimiter;
  const ABatchService: ISidekiqBatchService;
  const AServerMiddleware: TArray<ISidekiqServerMiddleware>;
  const AServerReliability: ISidekiqServerReliability;
  const ADeadLetterQueue: ISidekiqDeadLetterQueue): ISidekiqJobExecutor;
begin
  Result := TSidekiqJobExecutor.Create(
    ADispatcher,
    AAckPolicy,
    ARetryPolicy,
    ATelemetry,
    AStateStore,
    ALockProvider,
    AIdempotency,
    ARateLimiter,
    ABatchService,
    AServerMiddleware,
    AServerReliability,
    ADeadLetterQueue);
end;

class function TSidekiqJobExecutor.ResolveBatchId(
  const AJob: ISidekiqJobEnvelope): string;
begin
  Result := AJob.Attribute(TSidekiqJobAttribute.BatchId);
end;

class function TSidekiqJobExecutor.ResolveLogicalKey(
  const AJob: ISidekiqJobEnvelope): string;
begin
  Result := AJob.Attribute(TSidekiqJobAttribute.LockKey);
  if Result.IsEmpty then
    Result := AJob.Attribute(TSidekiqJobAttribute.IdempotencyKey);
end;

class function TSidekiqJobExecutor.ResolveTtl(
  const AJob: ISidekiqJobEnvelope;
  const AAttributeName: string;
  const ADefault: Integer): Integer;
begin
  Result := StrToIntDef(AJob.Attribute(AAttributeName), ADefault);
  if Result <= 0 then
    Result := ADefault;
end;

class function TSidekiqJobExecutor.ResolveProcessingTtl(
  const AJob: ISidekiqJobEnvelope): Integer;
var
  LUnique: Integer;
begin
  LUnique := StrToIntDef(AJob.Attribute(TSidekiqJobAttribute.UniqueTtlSeconds), 0);
  if LUnique > 0 then
    Exit(LUnique);

  Result := ResolveTtl(AJob, TSidekiqJobAttribute.IdempotencyTtlSeconds, 3600);
end;

class function TSidekiqJobExecutor.ResolveCompletionTtl(
  const AJob: ISidekiqJobEnvelope): Integer;
var
  LUnique: Integer;
begin
  LUnique := StrToIntDef(AJob.Attribute(TSidekiqJobAttribute.UniqueTtlSeconds), 0);
  if LUnique > 0 then
    Exit(LUnique);

  Result := 86400;
end;

function TSidekiqJobExecutor.TryAcquireLock(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AIdempotencyKey: string): ISidekiqLockHandle;
var
  LLogicalKey: string;
  LLockTtl: Integer;
begin
  Result := nil;
  LLogicalKey := ResolveLogicalKey(AJob);
  if LLogicalKey.IsEmpty then
    Exit;

  LLockTtl := ResolveTtl(AJob, TSidekiqJobAttribute.LockTtlSeconds, 300);
  Result := FLockProvider.TryAcquire(LLogicalKey, LLockTtl);
  if not Assigned(Result) then
  begin
    if not AIdempotencyKey.IsEmpty then
      FIdempotency.Clear(AIdempotencyKey);
    FAckPolicy.OnRetry(AQueue, AJob, 5, FTelemetry);
  end;
end;

function TSidekiqJobExecutor.TryAcquireRateLimit(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AIdempotencyKey: string): Boolean;
var
  LKey: string;
  LCapacity: Integer;
  LIntervalSeconds: Integer;
  LCost: Integer;
  LRetryAfterSeconds: Integer;
begin
  Result := True;
  LKey := AJob.Attribute(TSidekiqJobAttribute.RateLimitKey);
  if LKey.IsEmpty then
    Exit;

  LCapacity := StrToIntDef(
    AJob.Attribute(TSidekiqJobAttribute.RateLimitCapacity),
    0);
  LIntervalSeconds := StrToIntDef(
    AJob.Attribute(TSidekiqJobAttribute.RateLimitIntervalSeconds),
    0);
  LCost := StrToIntDef(
    AJob.Attribute(TSidekiqJobAttribute.RateLimitCost),
    1);

  if not Assigned(FRateLimiter) then
    raise ESidekiqRateLimiter.Create(
      'Rate limit attributes require an active rate limiter.');

  if not FRateLimiter.TryAcquire(
    LKey,
    LCapacity,
    LIntervalSeconds,
    LCost,
    LRetryAfterSeconds) then
  begin
    if not AIdempotencyKey.IsEmpty then
      FIdempotency.Clear(AIdempotencyKey);
    FAckPolicy.OnRetry(AQueue, AJob, LRetryAfterSeconds, FTelemetry);
    Exit(False);
  end;
end;

function TSidekiqJobExecutor.TryEnterIdempotency(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  out AIdempotencyKey: string;
  out AAlreadyCompleted: Boolean): Boolean;
var
  LIdempotencyTtl: Integer;
begin
  Result := True;
  AAlreadyCompleted := False;
  AIdempotencyKey := FIdempotency.KeyForJob(AJob);
  if AIdempotencyKey.IsEmpty then
    Exit;

  if FIdempotency.IsCompleted(AIdempotencyKey) then
  begin
    AAlreadyCompleted := True;
    FAckPolicy.OnAlreadyCompleted(AQueue, AJob, FTelemetry);
    Exit(False);
  end;

  LIdempotencyTtl := ResolveProcessingTtl(AJob);
  if not FIdempotency.TryBegin(AIdempotencyKey, LIdempotencyTtl) then
  begin
    FAckPolicy.OnRetry(AQueue, AJob, 5, FTelemetry);
    Exit(False);
  end;
end;

end.
