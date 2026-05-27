# Server Configuration

## Fluent API — overview

```pascal
uses
  Hefesto.Server,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console;

var
  LStore: IHefestoStateStore;
  LServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;

  LServer := THefestoServer.New
    // --- Queue ---
    .UseQueue(THefestoInMemoryQueueAdapter.New)

    // --- Concurrency ---
    .Concurrency(4)        // worker threads
    .BatchSize(10)         // jobs fetched per cycle
    .IdleDelayMs(1000)     // ms to wait when queue is empty

    // --- State store ---
    .StateStore(LStore)

    // --- Idempotency (optional) ---
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))

    // --- Retry + Dead Letter ---
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))

    // --- Telemetry ---
    .Telemetry(THefestoConsoleTelemetry.New)

    // --- Handlers ---
    .RegisterHandler('send_email', TSendEmailHandler.Create)
    .RegisterHandler('process_report', TProcessReportHandler.Create)

    // --- Start ---
    .Run;

  // ... wait for stop signal ...

  LServer.Stop;
end;
```

## Method reference

| Method | Type | Default | Description |
|--------|------|---------|-------------|
| `.UseQueue(adapter)` | Required | — | Queue adapter to use |
| `.Concurrency(N)` | Integer | 1 | Number of parallel workers |
| `.BatchSize(N)` | Integer | 1 | Jobs fetched per fetch cycle |
| `.IdleDelayMs(N)` | Integer | 500 | Pause (ms) when queue is empty |
| `.StateStore(store)` | Optional | InMemory | State store for features |
| `.LockProvider(lock)` | Optional | — | Required for Leader Election |
| `.Idempotency(impl)` | Optional | — | Prevents reprocessing |
| `.RetryPolicy(policy)` | Optional | No retry | Retry policy |
| `.Telemetry(tel)` | Optional | Noop | Telemetry provider |
| `.Use(middleware)` | Optional | — | Adds middleware (chainable) |
| `.RegisterHandler(action, handler)` | Required | — | Maps action → handler |
| `.LeaderName(name)` | Optional | — | Leader election group name |
| `.LeaderLeaseTtlSeconds(N)` | Optional | 30 | Leadership lease TTL |
| `.UseLeaderElection` | Optional | — | Enables leader election |
| `.StopWhenIdle` | Optional | — | Stops server when queue empties |
| `.Run` | — | — | Starts workers (non-blocking) |
| `.Stop` | — | — | Gracefully stops workers |

## Minimal handler

```pascal
type
  TSendEmailHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TSendEmailHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'send_email';
end;

procedure TSendEmailHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // AJob.Body contains the payload JSON
  // AJob.Action contains the action string
  SendEmail(AJob.Body);
end;
```

## Stopping the server

`.Run` is non-blocking. To wait indefinitely for an external signal:

```pascal
LServer.Run;
ReadLn;  // wait for Enter in the console
LServer.Stop;
```

For a Windows Service, replace `ReadLn` with the service loop.

See the complete recipe in [06-recipes/windows-service.md](../06-recipes/windows-service.md).
