# Recipe: Exponential Retry and Dead Letter Queue

Server with exponential retry and inspection of jobs in the DLQ.

```pascal
program RetryAndDLQ;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

type
  TFailingHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FAttempt: Integer;
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TFailingHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'simulated_failure';
end;

procedure TFailingHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Inc(FAttempt);
  Writeln(Format('Attempt %d for job %s', [FAttempt, AJob.JobId]));

  // Fail on the first 2 attempts
  if FAttempt < 3 then
    raise Exception.Create('Simulated error: service unavailable');

  Writeln('Success on attempt 3!');
end;

var
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(1)
    .StateStore(THefestoInMemoryStateStore.New)
    // 5 attempts: delay = 15 × n² seconds (max 3600s)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('simulated_failure', TFailingHandler.Create)
    .Run;

  // Enqueue job that will fail and then succeed
  LQueue.Enqueue('simulated_failure', '{"id": 1}');

  Writeln('Job enqueued. Watch the retry attempts...');
  ReadLn;
  LServer.Stop;
end.
```

**Expected delays with THefestoExponentialRetryPolicy.New(5, 15, 3600):**
```
Attempt 1: immediate execution
Attempt 2: wait 15s  (15 × 1²)
Attempt 3: wait 60s  (15 × 2²)
Attempt 4: wait 135s (15 × 3²)
Attempt 5: wait 240s (15 × 4²)
→ After 5 attempts: MoveToDeadLetter
```

**Using fixed delay:**
```pascal
// 3 attempts with 30 seconds between each
.RetryPolicy(THefestoSimpleRetryPolicy.New(3, 30))
```

**Inspecting the DLQ via dashboard:**
- Access `GET /api/dlq` — lists jobs in the dead letter queue
- `POST /api/dlq/reprocess` — returns jobs to the main queue

See [retry-e-dlq.md](../03-configuration/retry-e-dlq.md) for formula reference.
