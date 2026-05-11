# Arquitetura em Camadas

## Diagrama

```
┌─────────────────────────────────────────────────────────┐
│                    Aplicação / Host                      │
├─────────────────────────────────────────────────────────┤
│              ISidekiqServer  (API pública)               │
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
│  Adapters (plugáveis — zero dependência no core)        │
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

## Regra de dependência

```
Core → nenhuma dependência externa
Adapters → dependem apenas do Core
Features → dependem do Core, algumas de Adapters
```

O core nunca importa units de adapters ou brokers externos. Isso garante que o framework compila sem Redis, RabbitMQ ou qualquer serviço externo instalado.

## Units principais do Core

| Unit | Responsabilidade |
|------|-----------------|
| `Sidekiq4D.Job` | Envelope de job (`TSidekiqJobEnvelope`), serialização |
| `Sidekiq4D.Handler` | Interface `ISidekiqJobHandler`, base `TBaseSidekiqHandler` |
| `Sidekiq4D.Dispatcher` | Roteamento de jobs para handlers registrados |
| `Sidekiq4D.Server` | `TSidekiqServer` — ponto de entrada principal |
| `Sidekiq4D.Executor` | Execução de um job único com tratamento de erro |
| `Sidekiq4D.WorkerPool` | Pool de threads para execução paralela |

## Interfaces públicas

| Interface | Arquivo |
|-----------|---------|
| `ISidekiqServer` | `Sidekiq4D.Server` |
| `ISidekiqQueueAdapter` | `Sidekiq4D.Queue.Interfaces` |
| `ISidekiqStateStore` | `Sidekiq4D.Store.Interfaces` |
| `ISidekiqJobHandler` | `Sidekiq4D.Handler` |
| `ISidekiqTelemetry` | `Sidekiq4D.Telemetry` |
| `ISidekiqRetryPolicy` | `Sidekiq4D.Retry` |
| `ISidekiqScheduledStore` | `Sidekiq4D.Scheduled` |
| `ISidekiqIdempotency` | `Sidekiq4D.Idempotency` |
| `ISidekiqRateLimiter` | `Sidekiq4D.RateLimit` |
| `ISidekiqLockProvider` | `Sidekiq4D.Locking` |

## Ciclo de vida de um job

```
Enqueue → Queue Adapter → Fetch → Middleware chain
       → Idempotency check → Handler.Execute
       → [Sucesso] Ack → Telemetry.JobSucceeded
       → [Falha]   Retry policy → Nack / MoveToDeadLetter
```
