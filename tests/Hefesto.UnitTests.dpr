program Hefesto.UnitTests.Runner;

{$APPTYPE CONSOLE}

{
  Runner DUnitX para os testes unitários do Hefesto.

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
    - Outbox (contrato IHefestoOutbox/IHefestoOutboxPoller via mock)
    - WebSocket Hub (THefestoInMemoryWebSocketHub — contagem, broadcast, frame encoding)
}

uses
  System.SysUtils,
  DUnitX.TestFramework,
  DUnitX.Loggers.Console,
  DUnitX.Loggers.XML.NUnit,
  Hefesto.Retry.Tests       in 'Hefesto.Retry.Tests.pas',
  Hefesto.Idempotency.Tests in 'Hefesto.Idempotency.Tests.pas',
  Hefesto.RateLimit.Tests   in 'Hefesto.RateLimit.Tests.pas',
  Hefesto.Leader.Tests      in 'Hefesto.Leader.Tests.pas',
  Hefesto.Batch.Tests       in 'Hefesto.Batch.Tests.pas',
  Hefesto.Middleware.Tests  in 'Hefesto.Middleware.Tests.pas',
  Hefesto.Graph.Tests       in 'Hefesto.Graph.Tests.pas',
  Hefesto.DeadLetter.Tests  in 'Hefesto.DeadLetter.Tests.pas',
  Hefesto.Scheduled.Tests   in 'Hefesto.Scheduled.Tests.pas',
  Hefesto.Periodic.Tests    in 'Hefesto.Periodic.Tests.pas',
  Hefesto.Outbox.Tests      in 'Hefesto.Outbox.Tests.pas',
  Hefesto.WebSocket.Tests   in 'Hefesto.WebSocket.Tests.pas';

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
