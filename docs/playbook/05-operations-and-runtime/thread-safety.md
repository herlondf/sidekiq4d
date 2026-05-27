# Thread-Safety

## Threading model

The Hefesto server uses a fixed thread pool defined by `.Concurrency(N)`. Each worker thread operates independently:

- **Fetch:** each worker does its own queue fetch (no explicit contention — the adapter must be thread-safe)
- **Execution:** each job is executed by exactly one thread at a time
- **Handlers:** can have multiple instances executing in parallel when `Concurrency > 1`

## Framework guarantees

| What the framework guarantees | What you are responsible for |
|-------------------------------|------------------------------|
| A job processed by at most one thread | Thread-safety of your handler |
| Atomic Ack/Nack operations in the adapter | Thread-safety of shared state in the handler |
| `TCriticalSection` in the metrics history | Thread-safety of external resources (database connections) |
| `TInterlocked` on internal counters | Synchronization of caches in handlers |

## Protecting shared state

### In the handler (state shared between executions)

```pascal
TMyHandler = class(TInterfacedObject, IHefestoJobHandler)
private
  FLock: TCriticalSection;
  FSharedCache: TDictionary<string, string>;
public
  constructor Create;
  destructor Destroy; override;
  procedure Execute(const AJob: IHefestoJobEnvelope);
end;

constructor TMyHandler.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FSharedCache := TDictionary<string, string>.Create;
end;

destructor TMyHandler.Destroy;
begin
  FSharedCache.Free;
  FLock.Free;
  inherited;
end;

procedure TMyHandler.Execute(const AJob: IHefestoJobEnvelope);
var
  LValue: string;
begin
  FLock.Enter;
  try
    if not FSharedCache.TryGetValue(AJob.Body, LValue) then
    begin
      LValue := ComputeExpensive(AJob.Body);
      FSharedCache.Add(AJob.Body, LValue);
    end;
  finally
    FLock.Leave;
  end;
  
  ProcessWithValue(LValue);
end;
```

### Using TMonitor (lighter alternative)

```pascal
procedure TMyHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  TMonitor.Enter(FSharedObject);
  try
    // critical section
  finally
    TMonitor.Exit(FSharedObject);
  end;
end;
```

## HTTP Ingress Adapter

The `THefestoHTTPIngressAdapter` uses `TThreadedQueue<IHefestoJobEnvelope>` internally to receive jobs via HTTP and deliver them to the worker pool in a thread-safe manner.

If the internal queue fills up (request burst), new jobs are rejected with a timeout. To increase capacity:

```pascal
// Capacity and timeout configurable in the adapter constructor
THefestoHTTPIngressAdapter.New(
  Port,          // HTTP port
  Capacity,      // internal queue capacity (default varies)
  PushTimeoutMs  // push timeout (ms)
)
```

## Telemetry and historical metrics

`THefestoHistoricalMetricsTelemetry` uses `TCriticalSection` internally to protect metric buckets. No additional synchronization is needed when using it.

## Database connections

FireDAC and other ORMs are not thread-safe per connection. Recommended patterns:

1. **Connection pool:** use FireDAC's pool configured for multiple connections
2. **Connection per thread:** create and destroy connection inside `Execute` (per-job overhead)
3. **TThreadLocalStorage:** one connection per worker thread

Prefer the FireDAC connection pool — it is the most efficient and correct approach.
