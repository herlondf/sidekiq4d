# Recipe: Leader Election

Ensure that only one instance in a cluster executes exclusive tasks.

```pascal
program LeaderElection;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Threading,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.RedisStreams,
  Hefesto.Store.Redis4D,
  Hefesto.Locking,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

const
  REDIS_URL = 'redis://localhost:6379';

type
  TMaintenanceHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FServer: IHefestoServer;
  public
    constructor Create(const AServer: IHefestoServer);
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TNormalJobHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

constructor TMaintenanceHandler.Create(const AServer: IHefestoServer);
begin
  inherited Create;
  FServer := AServer;
end;

function TMaintenanceHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'maintenance';
end;

procedure TMaintenanceHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // Only the leader executes maintenance
  if not FServer.IsLeader then
  begin
    Writeln('I am not the leader. Ignoring maintenance task.');
    Exit;
  end;

  Writeln('I AM THE LEADER. Running maintenance...');
  // log cleanup, compaction, reports, etc.
end;

function TNormalJobHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process';
end;

procedure TNormalJobHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // All workers process normal jobs
  Writeln('Processing: ', AJob.Body);
end;

var
  LServer: IHefestoServer;
begin
  // LServer is declared before being passed to the handler
  LServer := THefestoServer.New
    .UseQueue(
      THefestoRedisStreamsAdapter.New
        .ConnectionString(REDIS_URL)
        .StreamName('sidekiq4d:jobs')
        .ConsumerGroup('cluster-workers')
        .ConsumerName('worker-' + IntToStr(GetCurrentProcessId))
    )
    .Concurrency(4)
    .StateStore(
      THefestoRedis4DStateStore.New
        .ConnectionString(REDIS_URL)
    )
    .LockProvider(
      THefestoRedis4DLockProvider.New
        .ConnectionString(REDIS_URL)
    )
    .LeaderName('my-cluster')
    .LeaderLeaseTtlSeconds(30)
    .UseLeaderElection
    .RetryPolicy(THefestoExponentialRetryPolicy.New(3, 10, 60))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('maintenance', TMaintenanceHandler.Create(LServer))
    .RegisterHandler('process',     TNormalJobHandler.Create)
    .Run;

  Writeln(Format('Worker PID=%d started. Is leader? %s',
    [GetCurrentProcessId, BoolToStr(LServer.IsLeader, True)]));

  ReadLn;
  LServer.Stop;
end.
```

**Simulating failover:**
1. Start this program in 2 different terminals
2. Only one will be the leader
3. Close the leader — after 30s (TTL), the other takes over

**Starting Redis for testing:**
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

See [leader-election.md](../04-features/leader-election.md) for mechanism details.
