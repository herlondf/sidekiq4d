# Leader Election

Ensures that only one process (among multiple running in parallel) executes certain exclusive tasks — such as scheduled jobs, periodic maintenance, or singleton tasks.

## Configuration

```pascal
uses
  Hefesto.Server,
  Hefesto.Locking,
  Hefesto.Store.Redis4D;

var
  LServer: IHefestoServer;
begin
  LServer := THefestoServer.New
    .UseQueue(TMyAdapter.New)
    .LockProvider(
      THefestoRedis4DLockProvider.New
        .ConnectionString('redis://localhost:6379')
    )
    .LeaderName('my-cluster')            // election group name
    .LeaderLeaseTtlSeconds(30)           // lease TTL
    .UseLeaderElection                   // activates the mechanism
    .RegisterHandler('cron_job', TCronHandler.Create)
    .Run;
end;
```

## Checking if leader

```pascal
if LServer.IsLeader then
  ExecuteExclusiveTask;
```

The server tries to renew the lease periodically. If the current leader fails, another process takes over after the TTL expires.

## Internal mechanism

1. On startup, the server tries to write a lock key in the state store via `TryPutIfAbsent`
2. If successful, it becomes the leader and renews the lease before the TTL expires
3. If it fails (another process is already the leader), it stays in follower mode and retries periodically
4. When the leader stops or the lease expires without renewal, another process wins the next attempt

## Requirements

**The `LockProvider` must be Redis-based to work across multiple processes.**

`THefestoInMemoryStateStore` guarantees exclusion only within the same process — it does not work in environments with multiple hosts.

Supported providers:
- `THefestoRedis4DLockProvider` — recommended for production
- InMemory — only for tests with a single process

## Exclusive leader tasks

Example: only the leader executes scheduled jobs

```pascal
TSchedulerHandler = class(TInterfacedObject, IHefestoJobHandler)
private
  FServer: IHefestoServer;
public
  procedure Execute(const AJob: IHefestoJobEnvelope);
end;

procedure TSchedulerHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  if not FServer.IsLeader then
    Exit;  // followers ignore exclusive tasks
  
  ProcessExclusiveTask;
end;
```

## Recommended TTL

| Scenario | Suggested TTL |
|----------|--------------|
| Fast renewal (low latency) | 15–30s |
| Tolerance to network failures | 60s |
| Long jobs that should not be interrupted | 120s+ |

TTL too short: risk of false expiration during GC or CPU spike. TTL too long: longer delay for failover.

See recipe in [06-recipes/leader-election.md](../06-recipes/leader-election.md).
