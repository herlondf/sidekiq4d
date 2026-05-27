# Middlewares

Implement `IHefestoServerMiddleware` (`src/Hefesto.Middleware.pas`). Executed in a chain for each job before the handler.

## Table

| Middleware | Unit | What it does |
|-----------|------|-------------|
| `THefestoCircuitBreakerMiddleware` | `Hefesto.Middleware.CircuitBreaker` | Opens the circuit after N consecutive failures, temporarily blocks jobs |
| `THefestoCompressionMiddleware` | `Hefesto.Middleware.Compression` | Decompresses ZLib payload before the handler |
| `THefestoDeduplicationMiddleware` | `Hefesto.Middleware.Deduplication` | Discards duplicate jobs based on a hash key of the payload |
| `THefestoLoggingMiddleware` | `Hefesto.Middleware.Logging` | Logs the start and end of each job in structured JSON |
| `THefestoPrometheusMiddleware` | `Hefesto.Middleware.Prometheus` | Increments Prometheus counters by queue and status |
| `THefestoTimeoutMiddleware` | `Hefesto.Middleware.Timeout` | Aborts jobs that exceed the configured maximum time |
| `THefestoHorseMiddleware` | `Hefesto.Middleware.Horse` | Integration with the Horse framework for HTTP context |

## How to chain

Middlewares are registered via `.Use(...)` in the server configuration. The registration order defines the execution order (FIFO):

```pascal
THefestoServer.New
  .Use(THefestoLoggingMiddleware.New)
  .Use(THefestoTimeoutMiddleware.New(30000))       // 30 seconds
  .Use(THefestoCircuitBreakerMiddleware.New(5, 60)) // 5 failures, 60s open
  .Use(THefestoDeduplicationMiddleware.New(LStore))
  ...
```

In this example the execution order for each job is:
```
Logging → Timeout → CircuitBreaker → Deduplication → Handler
```

## IHefestoServerMiddleware interface

```pascal
IHefestoServerMiddleware = interface
  procedure Call(
    const AQueue: string;
    const AJob: THefestoJobEnvelope;
    const ANext: TProc);
end;
```

The middleware must call `ANext` to continue the chain. If it does not call it, the job is silently discarded (useful for deduplication).

## Custom middleware example

```pascal
TMyAuditMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
public
  procedure Call(
    const AQueue: string;
    const AJob: THefestoJobEnvelope;
    const ANext: TProc);
end;

procedure TMyAuditMiddleware.Call(
  const AQueue: string;
  const AJob: THefestoJobEnvelope;
  const ANext: TProc);
begin
  LogAudit(AQueue, AJob.Action);
  ANext();  // without this the job does not execute
end;
```

## How to implement a custom middleware

See [CLAUDE.md](../../CLAUDE.md) — section "Adding a Middleware".
