unit Sidekiq4D.WorkerPool;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Threading,
  System.Generics.Collections,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Executor;

type
  ESidekiqWorkerPool = class(Exception);

  ISidekiqWorkerActivity = interface
    ['{24F47D51-2D6E-4AD4-BD37-5A193A0EDCE8}']
    procedure WorkerStarted;
    procedure WorkerFinished;
    function ActiveWorkers: Integer;
  end;

  TSidekiqWorkerActivity = class(TInterfacedObject, ISidekiqWorkerActivity)
  private
    FLock: TObject;
    FActiveWorkers: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    class function New: ISidekiqWorkerActivity;

    procedure WorkerStarted;
    procedure WorkerFinished;
    function ActiveWorkers: Integer;
  end;

  TSidekiqWorkerPoolFailureState = class
  private
    FLock: TObject;
    FHasFailure: Boolean;
    FMessageText: string;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Capture(const E: Exception);
    function HasFailure: Boolean;
    function MessageText: string;
  end;

  TSidekiqQueuedJob = record
    Queue: ISidekiqQueueAdapter;
    Job: ISidekiqJobEnvelope;
    VisibilityTimeout: Integer;
  end;

  ISidekiqWorkerPool = interface
    ['{F81B25C6-F038-41D2-B0C4-28079EEBA8AB}']
    procedure Execute(
      const AJobs: TArray<TSidekiqQueuedJob>;
      const AExecutor: ISidekiqJobExecutor);
  end;

  TSidekiqSequentialWorkerPool = class(TInterfacedObject, ISidekiqWorkerPool)
  private
    FConcurrency: Integer;
    FActivity: ISidekiqWorkerActivity;
    FQueueSemaphores: TObjectDictionary<string, TSemaphore>;
    FActionSemaphores: TObjectDictionary<string, TSemaphore>;
    class procedure AcquireSemaphore(const ASemaphore: TSemaphore); static;
    class function BuildSemaphores(
      const ALimits: TDictionary<string, Integer>): TObjectDictionary<string, TSemaphore>; static;
    class function BuildTask(
      const AQueuedJob: TSidekiqQueuedJob;
      const AExecutor: ISidekiqJobExecutor;
      const AGlobalSemaphore: TSemaphore;
      const AQueueSemaphore: TSemaphore;
      const AActionSemaphore: TSemaphore;
      const AActivity: ISidekiqWorkerActivity;
      const AFailureState: TSidekiqWorkerPoolFailureState): ITask; static;
    class procedure ReleaseSemaphore(const ASemaphore: TSemaphore); static;
  public
    constructor Create(
      const AConcurrency: Integer = 1;
      const AQueueLimits: TDictionary<string, Integer> = nil;
      const AActionLimits: TDictionary<string, Integer> = nil;
      const AActivity: ISidekiqWorkerActivity = nil);
    destructor Destroy; override;
    class function New(
      const AConcurrency: Integer = 1;
      const AQueueLimits: TDictionary<string, Integer> = nil;
      const AActionLimits: TDictionary<string, Integer> = nil;
      const AActivity: ISidekiqWorkerActivity = nil): ISidekiqWorkerPool;

    procedure Execute(
      const AJobs: TArray<TSidekiqQueuedJob>;
      const AExecutor: ISidekiqJobExecutor);
  end;

implementation

uses
  Winapi.Windows;

{ TSidekiqWorkerActivity }

function TSidekiqWorkerActivity.ActiveWorkers: Integer;
begin
  TMonitor.Enter(FLock);
  try
    Result := FActiveWorkers;
  finally
    TMonitor.Exit(FLock);
  end;
end;

constructor TSidekiqWorkerActivity.Create;
begin
  inherited Create;
  FLock := TObject.Create;
end;

destructor TSidekiqWorkerActivity.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function TSidekiqWorkerActivity.New: ISidekiqWorkerActivity;
begin
  Result := TSidekiqWorkerActivity.Create;
end;

procedure TSidekiqWorkerActivity.WorkerFinished;
begin
  TMonitor.Enter(FLock);
  try
    if FActiveWorkers > 0 then
      Dec(FActiveWorkers);
  finally
    TMonitor.Exit(FLock);
  end;
end;

procedure TSidekiqWorkerActivity.WorkerStarted;
begin
  TMonitor.Enter(FLock);
  try
    Inc(FActiveWorkers);
  finally
    TMonitor.Exit(FLock);
  end;
end;

{ TSidekiqWorkerPoolFailureState }

procedure TSidekiqWorkerPoolFailureState.Capture(const E: Exception);
begin
  TMonitor.Enter(FLock);
  try
    if not FHasFailure then
    begin
      FHasFailure := True;
      FMessageText := E.ClassName + ': ' + E.Message;
    end;
  finally
    TMonitor.Exit(FLock);
  end;
end;

constructor TSidekiqWorkerPoolFailureState.Create;
begin
  inherited Create;
  FLock := TObject.Create;
end;

destructor TSidekiqWorkerPoolFailureState.Destroy;
begin
  FLock.Free;
  inherited;
end;

function TSidekiqWorkerPoolFailureState.HasFailure: Boolean;
begin
  TMonitor.Enter(FLock);
  try
    Result := FHasFailure;
  finally
    TMonitor.Exit(FLock);
  end;
end;

function TSidekiqWorkerPoolFailureState.MessageText: string;
begin
  TMonitor.Enter(FLock);
  try
    Result := FMessageText;
  finally
    TMonitor.Exit(FLock);
  end;
end;

{ TSidekiqSequentialWorkerPool }

class procedure TSidekiqSequentialWorkerPool.AcquireSemaphore(
  const ASemaphore: TSemaphore);
begin
  if Assigned(ASemaphore) then
    ASemaphore.Acquire;
end;

class function TSidekiqSequentialWorkerPool.BuildSemaphores(
  const ALimits: TDictionary<string, Integer>): TObjectDictionary<string, TSemaphore>;
var
  LPair: TPair<string, Integer>;
begin
  Result := TObjectDictionary<string, TSemaphore>.Create([doOwnsValues]);
  if not Assigned(ALimits) then
    Exit;

  for LPair in ALimits do
    if (LPair.Value > 0) and (not LPair.Key.Trim.IsEmpty) then
      Result.AddOrSetValue(
        LPair.Key,
        TSemaphore.Create(nil, LPair.Value, LPair.Value,
          'Sidekiq4D.Pool.' + LPair.Key + '.' + IntToStr(GetCurrentProcessId)));
end;

class function TSidekiqSequentialWorkerPool.BuildTask(
  const AQueuedJob: TSidekiqQueuedJob;
  const AExecutor: ISidekiqJobExecutor;
  const AGlobalSemaphore: TSemaphore;
  const AQueueSemaphore: TSemaphore;
  const AActionSemaphore: TSemaphore;
  const AActivity: ISidekiqWorkerActivity;
  const AFailureState: TSidekiqWorkerPoolFailureState): ITask;
begin
  Result := TTask.Run(
    procedure
    begin
      AcquireSemaphore(AGlobalSemaphore);
      AcquireSemaphore(AQueueSemaphore);
      AcquireSemaphore(AActionSemaphore);
      try
        if Assigned(AActivity) then
          AActivity.WorkerStarted;
        try
          try
            AExecutor.Execute(
              AQueuedJob.Queue,
              AQueuedJob.Job,
              AQueuedJob.VisibilityTimeout);
          except
            on E: Exception do
              AFailureState.Capture(E);
          end;
        finally
          if Assigned(AActivity) then
            AActivity.WorkerFinished;
        end;
      finally
        ReleaseSemaphore(AActionSemaphore);
        ReleaseSemaphore(AQueueSemaphore);
        ReleaseSemaphore(AGlobalSemaphore);
      end;
    end);
end;

constructor TSidekiqSequentialWorkerPool.Create(
  const AConcurrency: Integer;
  const AQueueLimits: TDictionary<string, Integer>;
  const AActionLimits: TDictionary<string, Integer>;
  const AActivity: ISidekiqWorkerActivity);
begin
  inherited Create;
  if AConcurrency > 0 then
    FConcurrency := AConcurrency
  else
    FConcurrency := 1;
  FActivity := AActivity;
  FQueueSemaphores := BuildSemaphores(AQueueLimits);
  FActionSemaphores := BuildSemaphores(AActionLimits);
end;

destructor TSidekiqSequentialWorkerPool.Destroy;
begin
  FActionSemaphores.Free;
  FQueueSemaphores.Free;
  inherited;
end;

procedure TSidekiqSequentialWorkerPool.Execute(
  const AJobs: TArray<TSidekiqQueuedJob>;
  const AExecutor: ISidekiqJobExecutor);
var
  LQueuedJob: TSidekiqQueuedJob;
  LTasks: TArray<ITask>;
  LGlobalSemaphore: TSemaphore;
  LQueueSemaphore: TSemaphore;
  LActionSemaphore: TSemaphore;
  LFailureState: TSidekiqWorkerPoolFailureState;
  LIndex: Integer;
begin
  if Length(AJobs) = 0 then
    Exit;

  if FConcurrency <= 1 then
  begin
    for LQueuedJob in AJobs do
    begin
      if Assigned(FActivity) then
        FActivity.WorkerStarted;
      try
        AExecutor.Execute(
          LQueuedJob.Queue,
          LQueuedJob.Job,
          LQueuedJob.VisibilityTimeout);
      finally
        if Assigned(FActivity) then
          FActivity.WorkerFinished;
      end;
    end;
    Exit;
  end;

  LFailureState := TSidekiqWorkerPoolFailureState.Create;
  LGlobalSemaphore := TSemaphore.Create(nil, FConcurrency, FConcurrency,
    'Sidekiq4D.WorkerPool.Global.' + IntToStr(GetCurrentProcessId));
  try
    SetLength(LTasks, Length(AJobs));
    for LIndex := 0 to Pred(Length(AJobs)) do
    begin
      LQueueSemaphore := nil;
      LActionSemaphore := nil;
      if Assigned(FQueueSemaphores) then
        FQueueSemaphores.TryGetValue(AJobs[LIndex].Queue.Name, LQueueSemaphore);
      if Assigned(FActionSemaphores) then
        FActionSemaphores.TryGetValue(AJobs[LIndex].Job.Action, LActionSemaphore);
      LTasks[LIndex] := BuildTask(
        AJobs[LIndex],
        AExecutor,
        LGlobalSemaphore,
        LQueueSemaphore,
        LActionSemaphore,
        FActivity,
        LFailureState);
    end;

    if not TTask.WaitForAll(LTasks, 30000) then
      LFailureState.Capture(
        ESidekiqWorkerPool.Create('worker pool shutdown timed out after 30s'));
    if LFailureState.HasFailure then
      raise ESidekiqWorkerPool.Create(
        'worker pool execution failed: ' + LFailureState.MessageText);
  finally
    LGlobalSemaphore.Free;
    LFailureState.Free;
  end;
end;

class function TSidekiqSequentialWorkerPool.New(
  const AConcurrency: Integer;
  const AQueueLimits: TDictionary<string, Integer>;
  const AActionLimits: TDictionary<string, Integer>;
  const AActivity: ISidekiqWorkerActivity): ISidekiqWorkerPool;
begin
  Result := TSidekiqSequentialWorkerPool.Create(
    AConcurrency,
    AQueueLimits,
    AActionLimits,
    AActivity);
end;

class procedure TSidekiqSequentialWorkerPool.ReleaseSemaphore(
  const ASemaphore: TSemaphore);
begin
  if Assigned(ASemaphore) then
    ASemaphore.Release;
end;

end.
