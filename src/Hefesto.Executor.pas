unit Hefesto.Executor;

interface

uses
  System.SysUtils,
  System.Diagnostics,
  Hefesto.Job,
  Hefesto.Metadata,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Queue.Interfaces,
  Hefesto.Dispatcher,
  Hefesto.Ack,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Store.Interfaces,
  Hefesto.Locking,
  Hefesto.Idempotency,
  Hefesto.Unique,
  Hefesto.Batch,
  Hefesto.Middleware,
  Hefesto.RateLimit,
  Hefesto.ServerReliability,
  Hefesto.DeadLetter;

type
  IHefestoJobExecutor = interface
    ['{9B46E84A-392E-417D-8CCF-47CB4977C5C7}']
    procedure Execute(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer);
  end;

  THefestoJobExecutor = class(TInterfacedObject, IHefestoJobExecutor)
  private
    FDispatcher: IHefestoDispatcher;
    FAckPolicy: IHefestoAckPolicy;
    FRetryPolicy: IHefestoRetryPolicy;
    FTelemetry: IHefestoTelemetry;
    FStateStore: IHefestoStateStore;
    FLockProvider: IHefestoLockProvider;
    FIdempotency: IHefestoIdempotency;
    FRateLimiter: IHefestoRateLimiter;
    FBatchService: IHefestoBatchService;
    FServerMiddleware: TArray<IHefestoServerMiddleware>;
    FServerReliability: IHefestoServerReliability;
    FDeadLetterQueue: IHefestoDeadLetterQueue;

    class function ResolveLogicalKey(
      const AJob: IHefestoJobEnvelope): string; static;
    class function ResolveTtl(
      const AJob: IHefestoJobEnvelope;
      const AAttributeName: string;
      const ADefault: Integer): Integer; static;
    class function ResolveProcessingTtl(
      const AJob: IHefestoJobEnvelope): Integer; static;
    class function ResolveCompletionTtl(
      const AJob: IHefestoJobEnvelope): Integer; static;
    class function ResolveBatchId(
      const AJob: IHefestoJobEnvelope): string; static;
    procedure HandleSuccess(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AIdempotencyKey: string;
      const AWatch: TStopwatch);
    procedure HandleFailure(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AIdempotencyKey: string;
      const AError: Exception;
      const AWatch: TStopwatch);
    function TryEnterIdempotency(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      out AIdempotencyKey: string;
      out AAlreadyCompleted: Boolean): Boolean;
    function TryAcquireRateLimit(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AIdempotencyKey: string): Boolean;
    function TryAcquireLock(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AIdempotencyKey: string): IHefestoLockHandle;
  public
    constructor Create(
      const ADispatcher: IHefestoDispatcher;
      const AAckPolicy: IHefestoAckPolicy;
      const ARetryPolicy: IHefestoRetryPolicy;
      const ATelemetry: IHefestoTelemetry;
      const AStateStore: IHefestoStateStore;
      const ALockProvider: IHefestoLockProvider;
      const AIdempotency: IHefestoIdempotency;
      const ARateLimiter: IHefestoRateLimiter;
      const ABatchService: IHefestoBatchService;
      const AServerMiddleware: TArray<IHefestoServerMiddleware>;
      const AServerReliability: IHefestoServerReliability;
      const ADeadLetterQueue: IHefestoDeadLetterQueue = nil);
    class function New(
      const ADispatcher: IHefestoDispatcher;
      const AAckPolicy: IHefestoAckPolicy;
      const ARetryPolicy: IHefestoRetryPolicy;
      const ATelemetry: IHefestoTelemetry;
      const AStateStore: IHefestoStateStore;
      const ALockProvider: IHefestoLockProvider;
      const AIdempotency: IHefestoIdempotency;
      const ARateLimiter: IHefestoRateLimiter;
      const ABatchService: IHefestoBatchService = nil;
      const AServerMiddleware: TArray<IHefestoServerMiddleware> = nil;
      const AServerReliability: IHefestoServerReliability = nil;
      const ADeadLetterQueue: IHefestoDeadLetterQueue = nil): IHefestoJobExecutor;

    procedure Execute(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer);
  end;

implementation

{ THefestoJobExecutor }

constructor THefestoJobExecutor.Create(
  const ADispatcher: IHefestoDispatcher;
  const AAckPolicy: IHefestoAckPolicy;
  const ARetryPolicy: IHefestoRetryPolicy;
  const ATelemetry: IHefestoTelemetry;
  const AStateStore: IHefestoStateStore;
  const ALockProvider: IHefestoLockProvider;
  const AIdempotency: IHefestoIdempotency;
  const ARateLimiter: IHefestoRateLimiter;
  const ABatchService: IHefestoBatchService;
  const AServerMiddleware: TArray<IHefestoServerMiddleware>;
  const AServerReliability: IHefestoServerReliability;
  const ADeadLetterQueue: IHefestoDeadLetterQueue);
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

procedure THefestoJobExecutor.Execute(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AVisibilityTimeout: Integer);
var
  LHandler: IHefestoJobHandler;
  LContext: IHefestoJobContext;
  LWatch: TStopwatch;
  LLockHandle: IHefestoLockHandle;
  LExecutionLease: IHefestoExecutionLease;
  LIdempotencyKey: string;
  LBatchId: string;
  LAlreadyCompleted: Boolean;
  LChain: THefestoServerMiddlewareChain;
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
      LContext := THefestoJobContext.New(
        AJob,
        FStateStore,
        FLockProvider,
        FIdempotency,
        FRateLimiter);

      if Length(FServerMiddleware) = 0 then
        LHandler.Perform(LContext)
      else
      begin
        LChain := THefestoServerMiddlewareChain.Create(FServerMiddleware);
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

procedure THefestoJobExecutor.HandleFailure(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AIdempotencyKey: string;
  const AError: Exception;
  const AWatch: TStopwatch);
var
  LDecision: THefestoRetryDecision;
  LStrategy: THefestoUniqueStrategy;
  LBatchId: string;
begin
  LBatchId := ResolveBatchId(AJob);
  if not AIdempotencyKey.IsEmpty then
  begin
    LStrategy := THefestoUniqueStrategy.FromJob(AJob);
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
          var LDlqEntry: THefestoDeadLetterEntry;
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

procedure THefestoJobExecutor.HandleSuccess(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
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

class function THefestoJobExecutor.New(
  const ADispatcher: IHefestoDispatcher;
  const AAckPolicy: IHefestoAckPolicy;
  const ARetryPolicy: IHefestoRetryPolicy;
  const ATelemetry: IHefestoTelemetry;
  const AStateStore: IHefestoStateStore;
  const ALockProvider: IHefestoLockProvider;
  const AIdempotency: IHefestoIdempotency;
  const ARateLimiter: IHefestoRateLimiter;
  const ABatchService: IHefestoBatchService;
  const AServerMiddleware: TArray<IHefestoServerMiddleware>;
  const AServerReliability: IHefestoServerReliability;
  const ADeadLetterQueue: IHefestoDeadLetterQueue): IHefestoJobExecutor;
begin
  Result := THefestoJobExecutor.Create(
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

class function THefestoJobExecutor.ResolveBatchId(
  const AJob: IHefestoJobEnvelope): string;
begin
  Result := AJob.Attribute(THefestoJobAttribute.BatchId);
end;

class function THefestoJobExecutor.ResolveLogicalKey(
  const AJob: IHefestoJobEnvelope): string;
begin
  Result := AJob.Attribute(THefestoJobAttribute.LockKey);
  if Result.IsEmpty then
    Result := AJob.Attribute(THefestoJobAttribute.IdempotencyKey);
end;

class function THefestoJobExecutor.ResolveTtl(
  const AJob: IHefestoJobEnvelope;
  const AAttributeName: string;
  const ADefault: Integer): Integer;
begin
  Result := StrToIntDef(AJob.Attribute(AAttributeName), ADefault);
  if Result <= 0 then
    Result := ADefault;
end;

class function THefestoJobExecutor.ResolveProcessingTtl(
  const AJob: IHefestoJobEnvelope): Integer;
var
  LUnique: Integer;
begin
  LUnique := StrToIntDef(AJob.Attribute(THefestoJobAttribute.UniqueTtlSeconds), 0);
  if LUnique > 0 then
    Exit(LUnique);

  Result := ResolveTtl(AJob, THefestoJobAttribute.IdempotencyTtlSeconds, 3600);
end;

class function THefestoJobExecutor.ResolveCompletionTtl(
  const AJob: IHefestoJobEnvelope): Integer;
var
  LUnique: Integer;
begin
  LUnique := StrToIntDef(AJob.Attribute(THefestoJobAttribute.UniqueTtlSeconds), 0);
  if LUnique > 0 then
    Exit(LUnique);

  Result := 86400;
end;

function THefestoJobExecutor.TryAcquireLock(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AIdempotencyKey: string): IHefestoLockHandle;
var
  LLogicalKey: string;
  LLockTtl: Integer;
begin
  Result := nil;
  LLogicalKey := ResolveLogicalKey(AJob);
  if LLogicalKey.IsEmpty then
    Exit;

  LLockTtl := ResolveTtl(AJob, THefestoJobAttribute.LockTtlSeconds, 300);
  Result := FLockProvider.TryAcquire(LLogicalKey, LLockTtl);
  if not Assigned(Result) then
  begin
    if not AIdempotencyKey.IsEmpty then
      FIdempotency.Clear(AIdempotencyKey);
    FAckPolicy.OnRetry(AQueue, AJob, 5, FTelemetry);
  end;
end;

function THefestoJobExecutor.TryAcquireRateLimit(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AIdempotencyKey: string): Boolean;
var
  LKey: string;
  LCapacity: Integer;
  LIntervalSeconds: Integer;
  LCost: Integer;
  LRetryAfterSeconds: Integer;
begin
  Result := True;
  LKey := AJob.Attribute(THefestoJobAttribute.RateLimitKey);
  if LKey.IsEmpty then
    Exit;

  LCapacity := StrToIntDef(
    AJob.Attribute(THefestoJobAttribute.RateLimitCapacity),
    0);
  LIntervalSeconds := StrToIntDef(
    AJob.Attribute(THefestoJobAttribute.RateLimitIntervalSeconds),
    0);
  LCost := StrToIntDef(
    AJob.Attribute(THefestoJobAttribute.RateLimitCost),
    1);

  if not Assigned(FRateLimiter) then
    raise EHefestoRateLimiter.Create(
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

function THefestoJobExecutor.TryEnterIdempotency(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
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
