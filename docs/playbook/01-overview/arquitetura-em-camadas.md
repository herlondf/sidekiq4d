# Layered Architecture

## Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    Application / Host                    │
├─────────────────────────────────────────────────────────┤
│              IHefestoServer  (public API)                │
├───────────────┬─────────────────────────────────────────┤
│  Core Engine  │  Features                               │
│               │                                         │
│  Job          │  Retry          Scheduled               │
│  Handler      │  Batch          Periodic                │
│  Dispatcher   │  Job Graph      Idempotency             │
│  Server       │  Rate Limit     Leader Election         │
│  Executor     │  Dead Letter    Outbox                  │
│  WorkerPool   │  Locking        Dashboard               │
├───────────────┴─────────────────────────────────────────┤
│  Adapters (pluggable — zero dependency in core)         │
│                                                         │
│  Queue: InMemory, Redis Streams, RabbitMQ, Kafka,       │
│         SQS, Azure SB, Google PubSub, HTTP, TCP         │
│                                                         │
│  Store: InMemory, Redis4D, Sentinel, PostgreSQL,        │
│         MongoDB, FireDAC/SQLite                         │
│                                                         │
│  Middleware: CircuitBreaker, Compression, Dedup,        │
│              Logging, Prometheus, Timeout, Horse        │
│                                                         │
│  Telemetry: Noop, Console, StatsD, OTLP, Historical    │
└─────────────────────────────────────────────────────────┘
```

## Dependency rule

```
Core → no external dependencies
Adapters → depend only on Core
Features → depend on Core, some on Adapters
```

The core never imports adapter or external broker units. This guarantees the framework compiles without Redis, RabbitMQ, or any external service installed.

## Main Core units

| Unit | Responsibility |
|------|---------------|
| `Hefesto.Job` | Job envelope (`THefestoJobEnvelope`), serialization |
| `Hefesto.Handler` | Interface `IHefestoJobHandler`, base `TBaseHefestoHandler` |
| `Hefesto.Dispatcher` | Routes jobs to registered handlers |
| `Hefesto.Server` | `THefestoServer` — main entry point |
| `Hefesto.Executor` | Executes a single job with error handling |
| `Hefesto.WorkerPool` | Thread pool for parallel execution |

## Public interfaces

| Interface | File |
|-----------|------|
| `IHefestoServer` | `Hefesto.Server` |
| `IHefestoQueueAdapter` | `Hefesto.Queue.Interfaces` |
| `IHefestoStateStore` | `Hefesto.Store.Interfaces` |
| `IHefestoJobHandler` | `Hefesto.Handler` |
| `IHefestoTelemetry` | `Hefesto.Telemetry` |
| `IHefestoRetryPolicy` | `Hefesto.Retry` |
| `IHefestoScheduledStore` | `Hefesto.Scheduled` |
| `IHefestoIdempotency` | `Hefesto.Idempotency` |
| `IHefestoRateLimiter` | `Hefesto.RateLimit` |
| `IHefestoLockProvider` | `Hefesto.Locking` |

## Job lifecycle

```
Enqueue → Queue Adapter → Fetch → Middleware chain
       → Idempotency check → Handler.Execute
       → [Success] Ack → Telemetry.JobSucceeded
       → [Failure]  Retry policy → Nack / MoveToDeadLetter
```
