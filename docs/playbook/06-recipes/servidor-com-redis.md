# Recipe: Server with Redis Streams

Requires local or remote Redis. Uses Redis4D via XREADGROUP for consumer groups.

```pascal
program RedisServer;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.RedisStreams,
  Hefesto.Store.Redis4D,
  Hefesto.Retry,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console;

const
  REDIS_URL = 'redis://localhost:6379';

type
  TProcessHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TProcessHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process';
end;

procedure TProcessHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processing: ', AJob.Body);
end;

var
  LStore: IHefestoStateStore;
  LServer: IHefestoServer;
begin
  LStore := THefestoRedis4DStateStore.New
    .ConnectionString(REDIS_URL);

  LServer := THefestoServer.New
    .UseQueue(
      THefestoRedisStreamsAdapter.New
        .ConnectionString(REDIS_URL)
        .StreamName('sidekiq4d:jobs')
        .ConsumerGroup('workers')
        .ConsumerName('worker-1')   // unique per instance
        .BlockMs(2000)              // block timeout on XREADGROUP
    )
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('process', TProcessHandler.Create)
    .Run;

  Writeln('Waiting for jobs from Redis...');
  ReadLn;
  LServer.Stop;
end.
```

**Notes:**
- `ConsumerName` must be unique per instance for consumer groups to work correctly
- `BlockMs` controls the XREADGROUP timeout — 0 blocks indefinitely
- The Redis `LStore` also persists idempotency and state across restarts
- For multiple workers on the same machine, use different names: `worker-1`, `worker-2`, etc.

**Starting Redis with Docker:**
```bash
docker run -d -p 6379:6379 redis:7-alpine
```
