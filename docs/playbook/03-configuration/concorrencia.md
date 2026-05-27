# Concurrency and Worker Pool Behavior

## Main parameters

### Concurrency

```pascal
.Concurrency(N)
```

Number of worker threads running in parallel. Each worker has its own execution context.

- **Default:** 1 (sequential)
- **CPU-bound:** use `Concurrency = number of CPU cores`
- **I/O-bound:** can use `Concurrency > cores` (e.g. 8–16 for jobs waiting on network/database)
- **Handlers are not thread-safe by default:** if the handler accesses shared state, protect it with `TCriticalSection`

### BatchSize

```pascal
.BatchSize(N)
```

How many jobs the server tries to fetch from the queue per fetch cycle.

- **Default:** 1
- Increasing this reduces round-trips to the broker, useful for high-volume queues
- Must be compatible with what the adapter supports (some adapters return 1 at a time regardless of BatchSize)

### IdleDelayMs

```pascal
.IdleDelayMs(N)
```

Wait time in milliseconds when the queue is empty before trying to fetch again.

- **Default:** 500ms
- Increasing reduces broker load in low-volume systems
- Decreasing reduces latency for processing new jobs

### StopWhenIdle

```pascal
.StopWhenIdle
```

Stops the server automatically when the queue empties. Useful for scheduled batch processing:

```pascal
LServer := THefestoServer.New
  .UseQueue(TMyAdapter.New)
  .Concurrency(4)
  .StopWhenIdle
  .RegisterHandler('process', TMyHandler.Create)
  .Run;

// Enqueue jobs...
EnqueueJobs(LServer);

// Wait for complete processing
LServer.WaitForIdle;
```

## Recommended sizing

| Job type | Suggested Concurrency |
|----------|----------------------|
| CPU-bound (calculation, compression) | = number of cores |
| I/O-bound (database, external HTTP) | 2× to 4× number of cores |
| Mixed | Test with 4–8, adjust by metrics |
| Mandatory sequential processing | 1 |

## Handler thread-safety

The framework guarantees that each job is processed by exactly one thread at a time. But multiple handler instances can execute in parallel when `Concurrency > 1`.

If your handler maintains state between executions (e.g. cache, database connection), protect it:

```pascal
TMyHandler = class(TInterfacedObject, IHefestoJobHandler)
private
  FLock: TCriticalSection;
  FCache: TDictionary<string, string>;
public
  constructor Create;
  destructor Destroy; override;
  // ...
end;

constructor TMyHandler.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FCache := TDictionary<string, string>.Create;
end;
```

See details in [thread-safety.md](../05-operations-and-runtime/thread-safety.md).
