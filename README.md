# Hefesto

<p align="center">
  <img src="docs/logo.png" alt="Hefesto" width="280">
</p>

<p align="center">
  Simple, efficient background job processing for Delphi.
</p>

<p align="center">
  <a href="https://github.com/herlondf/hefesto/releases"><img src="https://img.shields.io/github/v/release/herlondf/hefesto?style=flat-square&color=blue" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="MIT License"></a>
  <a href="https://www.embarcadero.com/products/delphi"><img src="https://img.shields.io/badge/Delphi-11%2B-red?style=flat-square" alt="Delphi 11+"></a>
  <a href="#queue-adapters"><img src="https://img.shields.io/badge/adapters-9%20brokers-purple?style=flat-square" alt="9 Adapters"></a>
  <a href="#exemplos"><img src="https://img.shields.io/badge/demos-25%20examples-orange?style=flat-square" alt="25 Demos"></a>
</p>

---

Hefesto is a Delphi framework for processing jobs asynchronously, reliably
and efficiently. It replaces manual queue polling loops with a declarative,
fluent API that handles concurrency, retry, observability and lifecycle — so
you focus on business logic, not plumbing.

## Performance

Real-world benchmark consuming 200 SQS messages via LocalStack, each job
performing 50ms of simulated work:

| | Manual loop | Hefesto | Improvement |
|---|:---:|:---:|:---:|
| **Throughput** | 3.6 jobs/s | **15.1 jobs/s** | **4.2x** |
| **Total time** | 56.1s | **13.2s** | **4.3x faster** |
| **Peak throughput** | 4.9 jobs/s | **26.1 jobs/s** | **5.3x** |
| Batch size | 1 msg/request | 10 msgs/request | |
| Workers | 1 (sequential) | 4 (concurrent) | |
| Idle strategy | Sleep 15s | Long-poll 5s | |

> The manual loop pattern (`Get(1)` + `Sleep(15s)`) represents the actual
> consumption strategy found in production Delphi SQS workers.
> Hefesto's improvement comes from batching, concurrency, and long-polling —
> with zero changes to the job handler itself.

## Getting Started

### 1. Installation

Clone and add `src/` to your Delphi project search path:

```
git clone https://github.com/herlondf/hefesto.git
```

```
Search path: hefesto\src\
```

### 2. Define a job handler

```pascal
type
  TEmailHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

procedure TEmailHandler.Perform(const AContext: IHefestoJobContext);
begin
  SendEmail(AContext.Job.Body);
end;
```

### 3. Configure and run

```pascal
THefestoServer.New
  .UseQueue(MySQSAdapter)
  .Concurrency(4)
  .BatchSize(10)
  .RetryPolicy(THefestoSimpleRetryPolicy.New(5, 30))
  .Telemetry(THefestoConsoleTelemetry.New)
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
Implement `IHefestoQueueAdapter` (5 methods) to add your own.

## Features

### Core
- Configurable batch size (1-10 per fetch)
- Long-polling support
- Global, per-queue and per-action concurrency limits
- Retry policies: `THefestoSimpleRetryPolicy` (fixed delay) and `THefestoExponentialRetryPolicy` (base × n²)
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

## Hefesto vs Hefesto Ruby

| Feature | Hefesto OSS | Hefesto Pro | Hefesto Enterprise | **Hefesto** |
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

Implement `IHefestoServerMiddleware` (1 method) to add your own.

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
| [BasicConsole](samples/BasicConsole) | Simple job, handler, telemetry |
| [ScheduledJobs](samples/ScheduledJobs) | EnqueueIn, EnqueueAt, Periodic (cron) |
| [BatchJobs](samples/BatchJobs) | Batch with OnComplete/OnSuccess callbacks |
| [ConcurrencyControl](samples/ConcurrencyControl) | Limits, rate limit, unique, idempotency |
| [Middleware](samples/Middleware) | Client + server middleware pipeline |
| [Reliability](samples/Reliability) | Outbox, leader election, pause/resume |
| [RetryDLQ](samples/RetryDLQ) | Exponential backoff, dead-letter |
| [SQLite](samples/SQLite) | SQLite state store via FireDAC |
| [Postgres](samples/Postgres) | PostgreSQL state store via FireDAC |
| [Redis](samples/Redis) | Full Redis4D integration |
| [WindowsService](samples/WindowsService) | Windows Service template |
| [SqsConsole](samples/SqsConsole) | AWS SQS with Signature V4 |
| [HTTPIngress](samples/HTTPIngress) | HTTP webhook receiver |
| [CircuitBreaker](samples/CircuitBreaker) | External provider protection |
| [TelemetryAgent](samples/TelemetryAgent) | Full telemetry agent (HTTP + providers) |
| [EmailSender](samples/EmailSender) | Async email with rate limiting |
| [PDFGenerator](samples/PDFGenerator) | Batch PDF generation with callback |
| [WebhookDispatcher](samples/WebhookDispatcher) | Resilient webhook delivery |
| [ETLPipeline](samples/ETLPipeline) | Job chaining (extract->transform->load) |
| [NotificationHub](samples/NotificationHub) | Multi-channel fan-out routing |
| [SQSBenchmark](samples/SQSBenchmark) | Real benchmark: manual vs Hefesto |
| [VCLDemo](samples/VCLDemo) | Visual split-screen comparison |
| [JobGraph](samples/JobGraph) | DAG of dependent jobs with parallel execution |
| [OTLPTelemetry](samples/OTLPTelemetry) | OpenTelemetry trace export to Jaeger/Tempo |
| [HorseIntegration](samples/HorseIntegration) | Horse framework middleware integration |

## Project Structure

```
src/                ~35 framework units
src/adapters/       ~35 adapter units (queues, middlewares, providers, telemetry)
src/vendor/         Synapse (bundled)
samples/           25 runnable demos
tests/              DUnitX unit tests (11 fixtures) + thread-safety + Redis smoke
docs/               Presentation (md + pptx)
docker/             docker-compose for local Redis, Postgres and Jaeger
```

## Contributing

See [CONTRIBUTING.md](./docs/CONTRIBUTING.md) for guidelines on adapters, middleware, state stores, tests, and pull request flow.
Also available in Portuguese: [CONTRIBUTING_pt-br.md](./docs/CONTRIBUTING_pt-br.md)

## The Olympian Family

> *Poseidon comanda os mares — transporte bruto, a força das ondas.*
> *Triton guarda as águas do pai — gerencia o que flui, retém o que não pode se perder.*
> *Pégaso voa pelos céus — nasceu do sangue de Medusa, pela espada que Hermes deu a Perseu.*
> *Hermes percorre todos os reinos — carrega mensagens entre deuses, mortais e monstros, mais rápido que qualquer onda.*
> *Hefesto forja nas profundezas — invisível, incansável, transformando matéria bruta em obra acabada.*

| Project | Myth | Role |
|---------|------|------|
| [**Poseidon**](https://github.com/herlondf/poseidon) | God of the seas | Async transport layer — IOCP/epoll, raw I/O |
| [**Triton**](https://github.com/herlondf/triton) | Son of Poseidon, guardian of the depths | Generic resource pool — connections, clients, SMTP |
| [**Pegasus**](https://github.com/herlondf/pegasus) | Born from Poseidon's blood, ridden by heroes | HTTP framework — routing, middleware, providers |
| [**Hermes**](https://github.com/herlondf/hermes) | Messenger of the gods, guide between realms | Redis client — fast key-value, pub/sub, messaging |
| **Hefesto** (this lib) | Forgemaster of the gods, works unseen in the depths | Background jobs — queues, workers, retry, scheduling |

---

## Inspiration

Hefesto is inspired by [Sidekiq](https://github.com/sidekiq/sidekiq), the battle-tested background job framework for Ruby. The same core ideas — reliable queues, concurrency, retry with backoff, dead-letter, observability — brought natively to Delphi.

---

## License

[MIT](LICENSE) — use freely in commercial and open-source projects.

---

> 🇧🇷 Leia este documento em português: [README_pt-br.md](./README_pt-br.md)
