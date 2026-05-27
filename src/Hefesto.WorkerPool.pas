unit Hefesto.WorkerPool;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Threading,
  System.Generics.Collections,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Executor;

type
  EHefestoWorkerPool = class(Exception);

  IHefestoWorkerActivity = interface
    ['{24F47D51-2D6E-4AD4-BD37-5A193A0EDCE8}']
    procedure WorkerStarted;
    procedure WorkerFinished;
    function ActiveWorkers: Integer;
  end;

  THefestoWorkerActivity = class(TInterfacedObject, IHefestoWorkerActivity)
  private
    FLock: TObject;
    FActiveWorkers: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    class function New: IHefestoWorkerActivity;

    procedure WorkerStarted;
    procedure WorkerFinished;
    function ActiveWorkers: Integer;
  end;

  THefestoWorkerPoolFailureState = class
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

  THefestoQueuedJob = record
    Queue: IHefestoQueueAdapter;
    Job: IHefestoJobEnvelope;
    VisibilityTimeout: Integer;
  end;

  IHefestoWorkerPool = interface
    ['{F81B25C6-F038-41D2-B0C4-28079EEBA8AB}']
    procedure Execute(
      const AJobs: TArray<THefestoQueuedJob>;
      const AExecutor: IHefestoJobExecutor);
  end;

  THefestoSequentialWorkerPool = class(TInterfacedObject, IHefestoWorkerPool)
  private
    FConcurrency: Integer;
    FActivity: IHefestoWorkerActivity;
    FQueueSemaphores: TObjectDictionary<string, TSemaphore>;
    FActionSemaphores: TObjectDictionary<string, TSemaphore>;
    class procedure AcquireSemaphore(const ASemaphore: TSemaphore); static;
    class function BuildSemaphores(
      const ALimits: TDictionary<string, Integer>): TObjectDictionary<string, TSemaphore>; static;
    class function BuildTask(
      const AQueuedJob: THefestoQueuedJob;
      const AExecutor: IHefestoJobExecutor;
      const AGlobalSemaphore: TSemaphore;
      const AQueueSemaphore: TSemaphore;
      const AActionSemaphore: TSemaphore;
      const AActivity: IHefestoWorkerActivity;
      const AFailureState: THefestoWorkerPoolFailureState): ITask; static;
    class procedure ReleaseSemaphore(const ASemaphore: TSemaphore); static;
  public
    constructor Create(
      const AConcurrency: Integer = 1;
      const AQueueLimits: TDictionary<string, Integer> = nil;
      const AActionLimits: TDictionary<string, Integer> = nil;
      const AActivity: IHefestoWorkerActivity = nil);
    destructor Destroy; override;
    class function New(
      const AConcurrency: Integer = 1;
      const AQueueLimits: TDictionary<string, Integer> = nil;
      const AActionLimits: TDictionary<string, Integer> = nil;
      const AActivity: IHefestoWorkerActivity = nil): IHefestoWorkerPool;

    procedure Execute(
      const AJobs: TArray<THefestoQueuedJob>;
      const AExecutor: IHefestoJobExecutor);
  end;

implementation

uses
  Winapi.Windows;

{ THefestoWorkerActivity }

function THefestoWorkerActivity.ActiveWorkers: Integer;
begin
  TMonitor.Enter(FLock);
  try
    Result := FActiveWorkers;
  finally
    TMonitor.Exit(FLock);
  end;
end;

constructor THefestoWorkerActivity.Create;
begin
  inherited Create;
  FLock := TObject.Create;
end;

destructor THefestoWorkerActivity.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function THefestoWorkerActivity.New: IHefestoWorkerActivity;
begin
  Result := THefestoWorkerActivity.Create;
end;

procedure THefestoWorkerActivity.WorkerFinished;
begin
  TMonitor.Enter(FLock);
  try
    if FActiveWorkers > 0 then
      Dec(FActiveWorkers);
  finally
    TMonitor.Exit(FLock);
  end;
end;

procedure THefestoWorkerActivity.WorkerStarted;
begin
  TMonitor.Enter(FLock);
  try
    Inc(FActiveWorkers);
  finally
    TMonitor.Exit(FLock);
  end;
end;

{ THefestoWorkerPoolFailureState }

procedure THefestoWorkerPoolFailureState.Capture(const E: Exception);
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

constructor THefestoWorkerPoolFailureState.Create;
begin
  inherited Create;
  FLock := TObject.Create;
end;

destructor THefestoWorkerPoolFailureState.Destroy;
begin
  FLock.Free;
  inherited;
end;

function THefestoWorkerPoolFailureState.HasFailure: Boolean;
begin
  TMonitor.Enter(FLock);
  try
    Result := FHasFailure;
  finally
    TMonitor.Exit(FLock);
  end;
end;

function THefestoWorkerPoolFailureState.MessageText: string;
begin
  TMonitor.Enter(FLock);
  try
    Result := FMessageText;
  finally
    TMonitor.Exit(FLock);
  end;
end;

{ THefestoSequentialWorkerPool }

class procedure THefestoSequentialWorkerPool.AcquireSemaphore(
  const ASemaphore: TSemaphore);
begin
  if Assigned(ASemaphore) then
    ASemaphore.Acquire;
end;

class function THefestoSequentialWorkerPool.BuildSemaphores(
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
          'Hefesto.Pool.' + LPair.Key + '.' + IntToStr(GetCurrentProcessId)));
end;

class function THefestoSequentialWorkerPool.BuildTask(
  const AQueuedJob: THefestoQueuedJob;
  const AExecutor: IHefestoJobExecutor;
  const AGlobalSemaphore: TSemaphore;
  const AQueueSemaphore: TSemaphore;
  const AActionSemaphore: TSemaphore;
  const AActivity: IHefestoWorkerActivity;
  const AFailureState: THefestoWorkerPoolFailureState): ITask;
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

constructor THefestoSequentialWorkerPool.Create(
  const AConcurrency: Integer;
  const AQueueLimits: TDictionary<string, Integer>;
  const AActionLimits: TDictionary<string, Integer>;
  const AActivity: IHefestoWorkerActivity);
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

destructor THefestoSequentialWorkerPool.Destroy;
begin
  FActionSemaphores.Free;
  FQueueSemaphores.Free;
  inherited;
end;

procedure THefestoSequentialWorkerPool.Execute(
  const AJobs: TArray<THefestoQueuedJob>;
  const AExecutor: IHefestoJobExecutor);
var
  LQueuedJob: THefestoQueuedJob;
  LTasks: TArray<ITask>;
  LGlobalSemaphore: TSemaphore;
  LQueueSemaphore: TSemaphore;
  LActionSemaphore: TSemaphore;
  LFailureState: THefestoWorkerPoolFailureState;
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

  LFailureState := THefestoWorkerPoolFailureState.Create;
  LGlobalSemaphore := TSemaphore.Create(nil, FConcurrency, FConcurrency,
    'Hefesto.WorkerPool.Global.' + IntToStr(GetCurrentProcessId));
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
        EHefestoWorkerPool.Create('worker pool shutdown timed out after 30s'));
    if LFailureState.HasFailure then
      raise EHefestoWorkerPool.Create(
        'worker pool execution failed: ' + LFailureState.MessageText);
  finally
    LGlobalSemaphore.Free;
    LFailureState.Free;
  end;
end;

class function THefestoSequentialWorkerPool.New(
  const AConcurrency: Integer;
  const AQueueLimits: TDictionary<string, Integer>;
  const AActionLimits: TDictionary<string, Integer>;
  const AActivity: IHefestoWorkerActivity): IHefestoWorkerPool;
begin
  Result := THefestoSequentialWorkerPool.Create(
    AConcurrency,
    AQueueLimits,
    AActionLimits,
    AActivity);
end;

class procedure THefestoSequentialWorkerPool.ReleaseSemaphore(
  const ASemaphore: TSemaphore);
begin
  if Assigned(ASemaphore) then
    ASemaphore.Release;
end;

end.
