# Testing and Validation

## Unit test suite (no external broker)

11 DUnitX fixtures in `tests/Hefesto.UnitTests.dpr`. All use `THefestoInMemoryStateStore` and `THefestoInMemoryQueueAdapter` — no external services required.

### Covered fixtures

| Fixture | What it tests |
|---------|--------------|
| Retry (Simple + Exponential) | Attempt count, delays, escalation |
| DeadLetter | Moving to DLQ after exhaustion |
| Scheduled | Scheduling, `PopDue`, listing, cancellation |
| Idempotency | `TryBegin`, `MarkCompleted`, duplicates |
| RateLimit | Token bucket, rate rejection, refill |
| Leader | Election, lease renewal, failover |
| Batch | `OnComplete`/`OnSuccess` callbacks, counters |
| Middleware | Chaining, `Call`/`ANext`, circuit breaker |
| Periodic | Cron expressions, next occurrence, registration |
| Outbox | `Save`, `Entries`, `Remove`, `Count` |
| Graph | DAG, `DependsOn`, parallel execution, order |

### Compile and run

```
delphi-build sidekiq4delphi-tests
tests\Hefesto.UnitTests.Runner.exe
```

Expected output: all tests green, no memory leak warnings.

### Fixture pattern

```pascal
uses
  DUnitX.TestFramework,
  Hefesto.Store.InMemory;

[TestFixture('MyFeature')]
TMyFixture = class
private
  FStore: IHefestoStateStore;
public
  [Setup]
  procedure Setup;
  [TearDown]
  procedure TearDown;

  [Test]
  [Category('Unit')]
  procedure Method_Scenario_ExpectedResult;
end;

procedure TMyFixture.Setup;
begin
  FStore := THefestoInMemoryStateStore.New;
end;

procedure TMyFixture.TearDown;
begin
  FStore := nil;
end;

procedure TMyFixture.Method_Scenario_ExpectedResult;
begin
  Assert.IsTrue(SomeCondition);
end;
```

### Adding a new test

1. Create `tests/Hefesto.<Feature>.Tests.pas`
2. Add it to the runner `tests/Hefesto.UnitTests.dpr`
3. Follow the fixture pattern above

## Concurrency stress test

Verifies race conditions and deadlocks under load.

```
delphi-build sidekiq4delphi-threadsafety
tests\Hefesto.ThreadSafety.Tests.Runner.exe
```

The test uses multiple threads to enqueue and process jobs simultaneously, verifying counter consistency and absence of concurrent access exceptions.

## Smoke test with real Redis

Requires local Redis at `localhost:6379`.

```
delphi-build sidekiq4delphi-redis4d-real-smoke
```

Tests complete integration with Redis: enqueueing, fetch, ack, nack, DLQ, and scheduled jobs using the real `THefestoRedis4DStateStore` adapter.

## Memory leak verification

Delphi detects leaks automatically when `ReportMemoryLeaksOnShutdown := True` is set in the `.dpr`:

```pascal
program Hefesto.UnitTests.Runner;

{$APPTYPE CONSOLE}

uses
  // ...
begin
  ReportMemoryLeaksOnShutdown := True;
  // ...
end.
```

If leaks appear in the output, check:
- Interfaces without `TInterfacedObject` (reference counting)
- Objects created in `Setup` and not released in `TearDown`
- Registered handlers that do not implement `TInterfacedObject`
