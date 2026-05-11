program Sidekiq4D.UnitTests.Runner;

{$APPTYPE CONSOLE}

{
  Runner DUnitX para os testes unitários do Sidekiq4D.

  Cobre sem dependências externas (InMemory store/queue):
    - Retry policy (SimpleRetryPolicy, ExponentialRetryPolicy)
    - Idempotency (StateStoreIdempotency, RenewableIdempotency)
    - Rate Limiting (NoopRateLimiter, TokenBucketRateLimiter)
    - Leader Election (LeaderElection com InMemoryLockProvider)
    - Batch Service (BatchStateStore — contadores e callbacks)
    - Middlewares (CircuitBreaker, Deduplication)
    - Job Graph DAG (topological sort, ciclos, cancelamento, paralelo)
    - Dead Letter Queue (Push, Pop, List, Delete, Retry)
    - Scheduled Store (Schedule, PopDue, ordenação cronológica)
    - Periodic (CronSchedule matching, TruncateToMinute, MakePeriodicDefinition)
    - Outbox (contrato ISidekiqOutbox/ISidekiqOutboxPoller via mock)
}

uses
  System.SysUtils,
  DUnitX.TestFramework,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.XML.NUnit,
  Sidekiq4D.Retry.Tests       in 'Sidekiq4D.Retry.Tests.pas',
  Sidekiq4D.Idempotency.Tests in 'Sidekiq4D.Idempotency.Tests.pas',
  Sidekiq4D.RateLimit.Tests   in 'Sidekiq4D.RateLimit.Tests.pas',
  Sidekiq4D.Leader.Tests      in 'Sidekiq4D.Leader.Tests.pas',
  Sidekiq4D.Batch.Tests       in 'Sidekiq4D.Batch.Tests.pas',
  Sidekiq4D.Middleware.Tests  in 'Sidekiq4D.Middleware.Tests.pas',
  Sidekiq4D.Graph.Tests       in 'Sidekiq4D.Graph.Tests.pas',
  Sidekiq4D.DeadLetter.Tests  in 'Sidekiq4D.DeadLetter.Tests.pas',
  Sidekiq4D.Scheduled.Tests   in 'Sidekiq4D.Scheduled.Tests.pas',
  Sidekiq4D.Periodic.Tests    in 'Sidekiq4D.Periodic.Tests.pas',
  Sidekiq4D.Outbox.Tests      in 'Sidekiq4D.Outbox.Tests.pas';

var
  LRunner: ITestRunner;
  LResults: IRunResults;
  LLogger: ITestLogger;
  LXmlLogger: ITestLogger;

begin
  ReportMemoryLeaksOnShutdown := True;
  try
    LRunner := TDUnitX.CreateRunner;
    LRunner.UseRTTI := True;

    LLogger := TDUnitXConsoleLogger.Create(True);
    LRunner.AddLogger(LLogger);

    if ParamCount > 0 then
    begin
      LXmlLogger := TDUnitXXMLNUnitFileLogger.Create(ParamStr(1));
      LRunner.AddLogger(LXmlLogger);
    end;

    LResults := LRunner.Execute;
    if not LResults.AllPassed then
      System.ExitCode := 1;
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName, ': ', E.Message);
      System.ExitCode := 1;
    end;
  end;
end.
