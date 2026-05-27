# Recipe: Idempotency

Prevent the same job from being processed more than once when the broker delivers duplicates.

```pascal
program Idempotency;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console;

type
  TTransferHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TTransferHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'transfer';
end;

procedure TTransferHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // This code executes at most once per JobId
  Writeln('Executing transfer: ', AJob.Body);
  Writeln('JobId: ', AJob.JobId);
end;

var
  LStore: IHefestoStateStore;
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('transfer', TTransferHandler.Create)
    .Run;

  // Simulate duplicate broker delivery
  LQueue.Enqueue('transfer', '{"amount": 100, "jobId": "abc123"}');
  LQueue.Enqueue('transfer', '{"amount": 100, "jobId": "abc123"}');  // duplicate
  LQueue.Enqueue('transfer', '{"amount": 100, "jobId": "abc123"}');  // duplicate

  // Only the first will be processed — the others are silently discarded
  Writeln('3 messages enqueued (2 are duplicates).');
  Writeln('Only 1 should be processed. Enter to stop...');
  ReadLn;
  LServer.Stop;
end.
```

**With TTL (reprocess after expiration):**

```pascal
// Allow reprocessing the same job after 24 hours
.Idempotency(
  THefestoRenewableIdempotency.New(LStore, 86400)  // 86400s = 24h
)
```

**With Redis for persistence across restarts:**

```pascal
LStore := THefestoRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');

.Idempotency(THefestoStateStoreIdempotency.New(LStore))
```

See [idempotency.md](../04-features/idempotency.md) for interface details.
