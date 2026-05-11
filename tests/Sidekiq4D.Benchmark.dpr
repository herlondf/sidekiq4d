program Sidekiq4D.Benchmark;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.Diagnostics,
  Sidekiq4D.Job in '..\src\Sidekiq4D.Job.pas',
  Sidekiq4D.Metadata in '..\src\Sidekiq4D.Metadata.pas',
  Sidekiq4D.Context in '..\src\Sidekiq4D.Context.pas',
  Sidekiq4D.Handler in '..\src\Sidekiq4D.Handler.pas',
  Sidekiq4D.Options in '..\src\Sidekiq4D.Options.pas',
  Sidekiq4D.Store.Interfaces in '..\src\Sidekiq4D.Store.Interfaces.pas',
  Sidekiq4D.Store.InMemory in '..\src\Sidekiq4D.Store.InMemory.pas',
  Sidekiq4D.Locking in '..\src\Sidekiq4D.Locking.pas',
  Sidekiq4D.Idempotency in '..\src\Sidekiq4D.Idempotency.pas',
  Sidekiq4D.ClientReliability in '..\src\Sidekiq4D.ClientReliability.pas',
  Sidekiq4D.RateLimit in '..\src\Sidekiq4D.RateLimit.pas',
  Sidekiq4D.Unique in '..\src\Sidekiq4D.Unique.pas',
  Sidekiq4D.Ack in '..\src\Sidekiq4D.Ack.pas',
  Sidekiq4D.Queue.Interfaces in '..\src\Sidekiq4D.Queue.Interfaces.pas',
  Sidekiq4D.Queue.InMemory in '..\src\Sidekiq4D.Queue.InMemory.pas',
  Sidekiq4D.Dispatcher in '..\src\Sidekiq4D.Dispatcher.pas',
  Sidekiq4D.Retry in '..\src\Sidekiq4D.Retry.pas',
  Sidekiq4D.Telemetry in '..\src\Sidekiq4D.Telemetry.pas',
  Sidekiq4D.Middleware in '..\src\Sidekiq4D.Middleware.pas',
  Sidekiq4D.Scheduled in '..\src\Sidekiq4D.Scheduled.pas',
  Sidekiq4D.Periodic in '..\src\Sidekiq4D.Periodic.pas',
  Sidekiq4D.Batch in '..\src\Sidekiq4D.Batch.pas',
  Sidekiq4D.Executor in '..\src\Sidekiq4D.Executor.pas',
  Sidekiq4D.WorkerPool in '..\src\Sidekiq4D.WorkerPool.pas',
  Sidekiq4D.Server in '..\src\Sidekiq4D.Server.pas';

type
  TNoopHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
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

function TNoopHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TNoopHandler.Perform(const AContext: ISidekiqJobContext);
begin
end;

procedure BenchmarkRunOnceWithOutboxBudget;
var
  LQueue: TSidekiqInMemoryQueueAdapter;
  LOutbox: ISidekiqClientOutbox;
  LServer: ISidekiqServer;
  LWatch: TStopwatch;
  LIndex: Integer;
begin
  LQueue := TSidekiqInMemoryQueueAdapter.New('default');
  LOutbox := TSidekiqInMemoryClientOutbox.New;
  for LIndex := 1 to 1000 do
    LOutbox.Save(MakePublishRequest(LQueue.Name, 'email', '{"id":1}', 0, nil));

  LServer := TSidekiqServer.New
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
  LQueue: TSidekiqInMemoryQueueAdapter;
  LServer: ISidekiqServer;
  LWatch: TStopwatch;
  LIndex: Integer;
  LPastDue: TDateTime;
begin
  LQueue := TSidekiqInMemoryQueueAdapter.New('scheduled');
  LServer := TSidekiqServer.New
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
  LCriticalQueue: TSidekiqInMemoryQueueAdapter;
  LDefaultQueue: TSidekiqInMemoryQueueAdapter;
  LServer: ISidekiqServer;
  LWatch: TStopwatch;
  LIndex: Integer;
begin
  LCriticalQueue := TSidekiqInMemoryQueueAdapter.New('critical');
  LDefaultQueue := TSidekiqInMemoryQueueAdapter.New('default');
  for LIndex := 1 to 200 do
  begin
    LCriticalQueue.Enqueue('email', '{"id":1}');
    LDefaultQueue.Enqueue('email', '{"id":1}');
  end;

  LServer := TSidekiqServer.New
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
