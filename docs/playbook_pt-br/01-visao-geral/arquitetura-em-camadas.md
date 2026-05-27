# Arquitetura em Camadas

## Diagrama

```
┌─────────────────────────────────────────────────────────┐
│                    Aplicação / Host                      │
├─────────────────────────────────────────────────────────┤
│              IHefestoServer  (API pública)               │
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
| `Hefesto.Job` | Envelope de job (`THefestoJobEnvelope`), serialização |
| `Hefesto.Handler` | Interface `IHefestoJobHandler`, base `TBaseHefestoHandler` |
| `Hefesto.Dispatcher` | Roteamento de jobs para handlers registrados |
| `Hefesto.Server` | `THefestoServer` — ponto de entrada principal |
| `Hefesto.Executor` | Execução de um job único com tratamento de erro |
| `Hefesto.WorkerPool` | Pool de threads para execução paralela |

## Interfaces públicas

| Interface | Arquivo |
|-----------|---------|
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

## Ciclo de vida de um job

```
Enqueue → Queue Adapter → Fetch → Middleware chain
       → Idempotency check → Handler.Execute
       → [Sucesso] Ack → Telemetry.JobSucceeded
       → [Falha]   Retry policy → Nack / MoveToDeadLetter
```
