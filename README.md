# Sidekiq4D

<p align="center">
  <img src="docs/logo.png" alt="Sidekiq4D" width="280">
</p>

<p align="center">
  Simple, efficient background job processing for Delphi.
</p>

<p align="center">
  <a href="https://github.com/herlondf/sidekiq4d/releases"><img src="https://img.shields.io/github/v/release/herlondf/sidekiq4d?style=flat-square&color=blue" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="MIT License"></a>
  <a href="https://www.embarcadero.com/products/delphi"><img src="https://img.shields.io/badge/Delphi-11%2B-red?style=flat-square" alt="Delphi 11+"></a>
  <a href="#queue-adapters"><img src="https://img.shields.io/badge/adapters-9%20brokers-purple?style=flat-square" alt="9 Adapters"></a>
  <a href="#exemplos"><img src="https://img.shields.io/badge/demos-25%20examples-orange?style=flat-square" alt="25 Demos"></a>
</p>

---

Sidekiq4D is a Delphi framework for processing jobs asynchronously, reliably
and efficiently. It replaces manual queue polling loops with a declarative,
fluent API that handles concurrency, retry, observability and lifecycle — so
you focus on business logic, not plumbing.

## Performance

Real-world benchmark consuming 200 SQS messages via LocalStack, each job
performing 50ms of simulated work:

| | Manual loop | Sidekiq4D | Improvement |
|---|:---:|:---:|:---:|
| **Throughput** | 3.6 jobs/s | **15.1 jobs/s** | **4.2x** |
| **Total time** | 56.1s | **13.2s** | **4.3x faster** |
| **Peak throughput** | 4.9 jobs/s | **26.1 jobs/s** | **5.3x** |
| Batch size | 1 msg/request | 10 msgs/request | |
| Workers | 1 (sequential) | 4 (concurrent) | |
| Idle strategy | Sleep 15s | Long-poll 5s | |

> The manual loop pattern (`Get(1)` + `Sleep(15s)`) represents the actual
> consumption strategy found in production Delphi SQS workers.
> Sidekiq4D's improvement comes from batching, concurrency, and long-polling —
> with zero changes to the job handler itself.

## Getting Started

### 1. Installation

Clone and add `src/` to your Delphi project search path:

```
git clone https://github.com/herlondf/sidekiq4d.git
```

```
Search path: sidekiq4d\src\
```

### 2. Define a job handler

```pascal
type
  TEmailHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

procedure TEmailHandler.Perform(const AContext: ISidekiqJobContext);
begin
  SendEmail(AContext.Job.Body);
end;
```

### 3. Configure and run

```pascal
TSidekiqServer.New
  .UseQueue(MySQSAdapter)
  .Concurrency(4)
  .BatchSize(10)
  .RetryPolicy(TSidekiqSimpleRetryPolicy.New(5, 30))
  .Telemetry(TSidekiqConsoleTelemetry.New)
  .RegisterHandler('send_email', TEmailHandler.Create)
  .Run;
```

That's it. Batch, long-polling, concurrency, retry, dead-letter and telemetry —
all configured in 8 lines.

## Requirements

- **Delphi 11 Alexandria** or later
- No mandatory external dependencies

Optional (adapter-specific):
- **Redis4D** — for Redis state store, locks, scheduled store
- **FireDAC** (included in Delphi) — for SQLite/Postgres state store
- **Synapse** (included in `src/vendor/synalist/`) — for TCP adapter

## Queue Adapters

| Adapter | Protocol | Use case |
|---------|----------|----------|
| **InMemory** | — | Testing, development |
| **SQS** | AWS HTTP + Sig V4 | AWS production |
| **RabbitMQ** | HTTP Management API | On-premise messaging |
| **Kafka** | Confluent REST Proxy | High-throughput streaming |
| **Azure Service Bus** | Azure REST + SAS | Azure cloud |
| **Google Pub/Sub** | Google REST + Bearer | GCP cloud |
| **Redis Streams** | Redis4D (XREADGROUP) | Redis as broker |
| **HTTP Ingress** | Indy HTTP Server | Webhooks, telemetry agents |
| **TCP** | Indy or Synapse | Legacy Delphi apps, IoT |

All adapters use **HTTP only** — no external SDK required.
Implement `ISidekiqQueueAdapter` (5 methods) to add your own.

## Features

### Core
- Configurable batch size (1-10 per fetch)
- Long-polling support
- Global, per-queue and per-action concurrency limits
- Retry policies: `TSidekiqSimpleRetryPolicy` (fixed delay) and `TSidekiqExponentialRetryPolicy` (base × n²)
- Automatic dead-letter queue
- Graceful shutdown (no lost jobs)

### Pro-level
- **Scheduled jobs** — `EnqueueIn(delay)` / `EnqueueAt(datetime)`
- **Periodic jobs** — native cron scheduling (5-field expressions)
- **Batch jobs** — `OnComplete` / `OnSuccess` callbacks
- **Unique jobs** — deduplication strategies
- **Rate limiting** — token-bucket per key
- **Idempotency** — duplicate execution prevention

### Enterprise-level
- **Leader election** — cluster-wide singleton operations
- **Client reliability** — outbox pattern for crash recovery
- **Server reliability** — heartbeat + stale execution recovery
- **Rolling restarts** — graceful drain and resume
- **Pause/resume** — operational queue control
- **Circuit breaker** — external provider protection
- **Job Graph (DAG)** — declarative job dependencies, parallel execution, cascade cancellation
- **OpenTelemetry** — OTLP trace exporter for Jaeger, Grafana Tempo, Honeycomb, Datadog
- **Dashboard Web** — SPA with real-time metrics (SSE), `/health`, `/metrics` (Prometheus scrape), cancel scheduled jobs

> All features included. No paid tiers.

## Sidekiq4D vs Sidekiq Ruby

| Feature | Sidekiq OSS | Sidekiq Pro | Sidekiq Enterprise | **Sidekiq4D** |
|---------|:-:|:-:|:-:|:-:|
| Job processing + retry | x | x | x | **x** |
| Scheduled + middleware | x | x | x | **x** |
| Batch + unique jobs | | $99/mo | $99/mo | **free** |
| Rate limiting | | $99/mo | $99/mo | **free** |
| Periodic jobs | | | $179/mo | **free** |
| Leader election | | | $179/mo | **free** |
| Rolling restarts | | | $179/mo | **free** |

## Middlewares

Chainable pipeline — runs before publish (client) or before execution (server):

| Middleware | Type | Purpose |
|-----------|------|---------|
| Logging | Server | Structured JSON output |
| Timeout | Server | Abort after N ms |
| Deduplication | Server | Skip by idempotency key |
| Compression | Client+Server | ZLib body compression |
| Prometheus | Server | Counters + histogram metrics |
| Circuit Breaker | Server | Open/closed/half-open per action |

Implement `ISidekiqServerMiddleware` (1 method) to add your own.

## State Stores

| Store | Dependency | Use case |
|-------|------------|----------|
| **InMemory** | None | Testing |
| **SQLite** | FireDAC (included) | Single-process, Windows Service |
| **PostgreSQL** | FireDAC + libpq | Multi-process, distributed |
| **Redis** | Redis4D | High performance |
| **MongoDB** | HTTP (Atlas API) | Cloud-native |

## Exemplos

| Example | Scenario |
|---------|----------|
| [BasicConsole](examples/BasicConsole) | Simple job, handler, telemetry |
| [ScheduledJobs](examples/ScheduledJobs) | EnqueueIn, EnqueueAt, Periodic (cron) |
| [BatchJobs](examples/BatchJobs) | Batch with OnComplete/OnSuccess callbacks |
| [ConcurrencyControl](examples/ConcurrencyControl) | Limits, rate limit, unique, idempotency |
| [Middleware](examples/Middleware) | Client + server middleware pipeline |
| [Reliability](examples/Reliability) | Outbox, leader election, pause/resume |
| [RetryDLQ](examples/RetryDLQ) | Exponential backoff, dead-letter |
| [SQLite](examples/SQLite) | SQLite state store via FireDAC |
| [Postgres](examples/Postgres) | PostgreSQL state store via FireDAC |
| [Redis](examples/Redis) | Full Redis4D integration |
| [WindowsService](examples/WindowsService) | Windows Service template |
| [SqsConsole](examples/SqsConsole) | AWS SQS with Signature V4 |
| [HTTPIngress](examples/HTTPIngress) | HTTP webhook receiver |
| [CircuitBreaker](examples/CircuitBreaker) | External provider protection |
| [TelemetryAgent](examples/TelemetryAgent) | Full telemetry agent (HTTP + providers) |
| [EmailSender](examples/EmailSender) | Async email with rate limiting |
| [PDFGenerator](examples/PDFGenerator) | Batch PDF generation with callback |
| [WebhookDispatcher](examples/WebhookDispatcher) | Resilient webhook delivery |
| [ETLPipeline](examples/ETLPipeline) | Job chaining (extract->transform->load) |
| [NotificationHub](examples/NotificationHub) | Multi-channel fan-out routing |
| [SQSBenchmark](examples/SQSBenchmark) | Real benchmark: manual vs Sidekiq4D |
| [VCLDemo](examples/VCLDemo) | Visual split-screen comparison |
| [JobGraph](examples/JobGraph) | DAG of dependent jobs with parallel execution |
| [OTLPTelemetry](examples/OTLPTelemetry) | OpenTelemetry trace export to Jaeger/Tempo |
| [HorseIntegration](examples/HorseIntegration) | Horse framework middleware integration |

## Project Structure

```
src/                ~35 framework units
src/adapters/       ~35 adapter units (queues, middlewares, providers, telemetry)
src/vendor/         Synapse (bundled)
examples/           25 runnable demos
tests/              DUnitX unit tests (11 fixtures) + thread-safety + Redis smoke
docs/               Presentation (md + pptx)
docker/             docker-compose for local Redis, Postgres and Jaeger
```

## Contributing

Pull requests are welcome. For major changes, open an issue first to discuss.

**Patterns to follow:**
- Interfaces for extensibility (`ISidekiqQueueAdapter`, `ISidekiqServerMiddleware`)
- Fluent API (method chaining returning `Self`)
- `class function New` as factory
- Thread-safety: `TCriticalSection` / `TInterlocked` / `TThreadedQueue`

## License

[MIT](LICENSE) — use freely in commercial and open-source projects.
