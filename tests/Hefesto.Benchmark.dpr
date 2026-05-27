program Hefesto.Benchmark;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.Diagnostics,
  Hefesto.Job in '..\src\Hefesto.Job.pas',
  Hefesto.Metadata in '..\src\Hefesto.Metadata.pas',
  Hefesto.Context in '..\src\Hefesto.Context.pas',
  Hefesto.Handler in '..\src\Hefesto.Handler.pas',
  Hefesto.Options in '..\src\Hefesto.Options.pas',
  Hefesto.Store.Interfaces in '..\src\Hefesto.Store.Interfaces.pas',
  Hefesto.Store.InMemory in '..\src\Hefesto.Store.InMemory.pas',
  Hefesto.Locking in '..\src\Hefesto.Locking.pas',
  Hefesto.Idempotency in '..\src\Hefesto.Idempotency.pas',
  Hefesto.ClientReliability in '..\src\Hefesto.ClientReliability.pas',
  Hefesto.RateLimit in '..\src\Hefesto.RateLimit.pas',
  Hefesto.Unique in '..\src\Hefesto.Unique.pas',
  Hefesto.Ack in '..\src\Hefesto.Ack.pas',
  Hefesto.Queue.Interfaces in '..\src\Hefesto.Queue.Interfaces.pas',
  Hefesto.Queue.InMemory in '..\src\Hefesto.Queue.InMemory.pas',
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
  TNoopHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

procedure PrintMetric(const AName: string; const ACount: Integer; const AWatch: TStopwatch);
var
  LThroughput: Double;
begin
  if AWatch.Elapsed.TotalSeconds > 0 then
    LThroughput := ACount / AWatch.Elapsed.TotalSeconds
  else
    LThroughput := 0;
  Writeln(Format('%s | count=%d | ms=%d | throughput=%.2f/s',
    [AName, ACount, AWatch.ElapsedMilliseconds, LThroughput]));
end;

function TNoopHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TNoopHandler.Perform(const AContext: IHefestoJobContext);
begin
end;

procedure BenchmarkRunOnceWithOutboxBudget;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LOutbox: IHefestoClientOutbox;
  LServer: IHefestoServer;
  LWatch: TStopwatch;
  LIndex: Integer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('default');
  LOutbox := THefestoInMemoryClientOutbox.New;
  for LIndex := 1 to 1000 do
    LOutbox.Save(MakePublishRequest(LQueue.Name, 'email', '{"id":1}', 0, nil));

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .ClientOutbox(LOutbox)
    .ClientOutboxBudget(100)
    .BatchSize(1)
    .RegisterHandler('email', TNoopHandler.Create);

  LWatch := TStopwatch.StartNew;
  LServer.RunOnce;
  LWatch.Stop;
  PrintMetric('runonce_outbox_budget', 100, LWatch);
end;

procedure BenchmarkScheduledBacklogPromotion;
var
  LQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
  LWatch: TStopwatch;
  LIndex: Integer;
  LPastDue: TDateTime;
begin
  LQueue := THefestoInMemoryQueueAdapter.New('scheduled');
  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .ScheduledPromotionBudget(500);

  LPastDue := IncSecond(Now, -1);
  for LIndex := 1 to 5000 do
    LServer.EnqueueAt(LQueue.Name, 'email', '{"id":1}', LPastDue);

  LWatch := TStopwatch.StartNew;
  LServer.PromoteScheduled;
  LWatch.Stop;
  PrintMetric('scheduled_backlog_promotion', 500, LWatch);
end;

procedure BenchmarkWeightedFetchCycle;
var
  LCriticalQueue: THefestoInMemoryQueueAdapter;
  LDefaultQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
  LWatch: TStopwatch;
  LIndex: Integer;
begin
  LCriticalQueue := THefestoInMemoryQueueAdapter.New('critical');
  LDefaultQueue := THefestoInMemoryQueueAdapter.New('default');
  for LIndex := 1 to 200 do
  begin
    LCriticalQueue.Enqueue('email', '{"id":1}');
    LDefaultQueue.Enqueue('email', '{"id":1}');
  end;

  LServer := THefestoServer.New
    .UseQueue(LCriticalQueue)
    .UseQueue(LDefaultQueue)
    .QueueWeight('critical', 3)
    .QueueWeight('default', 1)
    .BatchSize(1)
    .RegisterHandler('email', TNoopHandler.Create);

  LWatch := TStopwatch.StartNew;
  for LIndex := 1 to 100 do
    LServer.RunOnce;
  LWatch.Stop;
  PrintMetric('weighted_fetch_cycle', 100, LWatch);
end;

begin
  try
    BenchmarkRunOnceWithOutboxBudget;
    BenchmarkScheduledBacklogPromotion;
    BenchmarkWeightedFetchCycle;
  except
    on E: Exception do
    begin
      Writeln(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
