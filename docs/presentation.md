# Sidekiq4D — Async Job Framework for Delphi

<p align="center">
  <img src="logo.png" alt="Sidekiq4D" width="180">
</p>

<p align="center">
  <strong>Framework de processamento assincrono de jobs para Delphi</strong><br>
  Inspirado no Sidekiq Ruby. Implementado nativamente em Object Pascal.
</p>

---

## O Problema

Aplicacoes Delphi que consomem filas (SQS, RabbitMQ, etc.) tipicamente usam loops manuais:

```pascal
// Padrao legado — encontrado em producao
while True do
begin
  Msg := Queue.Get(1);          // 1 mensagem por vez
  if Msg = nil then
  begin
    Sleep(15000);               // 15 segundos parado
    Continue;
  end;
  try
    ProcessMessage(Msg);        // Sequencial, sem concorrencia
    Queue.Delete(Msg);          // Ack manual
  except
    // Retry? Log? Dead-letter? — depende do dev
  end;
end;
```

**Problemas:**

| Aspecto | Impacto |
|---------|---------|
| 1 mensagem por fetch | Subutiliza a rede (SQS suporta 10) |
| Sleep fixo de 15s | Latencia alta quando fila enche |
| Sem concorrencia | CPU ociosa enquanto espera I/O |
| Sem retry estruturado | Mensagens perdidas em erro |
| Sem observabilidade | Impossivel monitorar em producao |
| Codigo espalhado | Dificil testar e evoluir |

---

## A Solucao

```pascal
TSidekiqServer.New
  .UseQueue(MySQSAdapter)
  .Concurrency(4)
  .BatchSize(10)
  .WaitTimeSeconds(5)
  .RetryPolicy(TSidekiqSimpleRetryPolicy.New(5, 30))
  .Telemetry(TSidekiqConsoleTelemetry.New)
  .RegisterHandler('emissao', TEmissaoHandler.Create)
  .Run;
```

**10 linhas.** Batch, long-polling, concorrencia, retry, telemetria — tudo configurado.

---

## Antes vs Depois

| | Loop Manual | Sidekiq4D |
|---|---|---|
| **Linhas de codigo** | 30-50 por worker | 10-15 |
| **Fetch** | 1 msg/request | 10 msgs/request |
| **Concorrencia** | Nenhuma | 1-N configuravel |
| **Retry** | Manual (ou inexistente) | Politica com backoff |
| **Dead-letter** | Manual | Automatico |
| **Observabilidade** | WriteLn | Eventos estruturados |
| **Testabilidade** | Dificil (acoplado ao loop) | Facil (handlers isolados) |

---

## Benchmark Real — SQS LocalStack

200 mensagens, trabalho simulado de 50ms por mensagem:

| Metrica | Sem Sidekiq4D | Com Sidekiq4D | Ganho |
|---------|:---:|:---:|:---:|
| **Throughput** | 3,6 msgs/s | **15,1 msgs/s** | **4,2x** |
| **Tempo total** | 56,1s | **13,2s** | **4,3x** |
| **Pico instantaneo** | 4,9 msgs/s | **26,1 msgs/s** | **5,3x** |
| Msgs por fetch | 1 | 10 | |
| Workers | 1 | 4 | |
| Idle wait | 15s fixo | 5s long-poll | |

> Testado com LocalStack SQS real. Ganho proporcional ao numero de workers.

---

## Arquitetura

```
Aplicacao Delphi
  |
  v
TSidekiqServer (API fluente)
  |
  +-- ISidekiqQueueAdapter (InMemory, SQS, RabbitMQ, Kafka, TCP, HTTP...)
  |
  +-- ISidekiqJobDispatcher (routing por action)
  |
  +-- ISidekiqServerMiddleware[] (pipeline encadeavel)
  |
  +-- TWorkerPool (concorrencia N threads)
  |     |
  |     +-- ISidekiqJobHandler.Perform()
  |
  +-- ISidekiqAckPolicy (ack apos sucesso)
  +-- ISidekiqRetryPolicy (backoff / DLQ)
  +-- ISidekiqTelemetry (eventos de ciclo de vida)
  +-- ISidekiqStateStore (persistencia distribuida)
  +-- ISidekiqLockProvider (locks com TTL)
```

**Principio:** cada componente atras de interface. Troque o adapter sem mudar o handler.

---

## Queue Adapters

| Adapter | Protocolo | Cenario |
|---------|-----------|---------|
| **InMemory** | — | Testes, desenvolvimento |
| **SQS** | AWS HTTP + Sig V4 | Producao AWS |
| **RabbitMQ** | HTTP Management API | On-premise, mensageria |
| **Kafka** | Confluent REST Proxy | Streaming, alta vazao |
| **Azure Service Bus** | Azure REST + SAS | Azure cloud |
| **Google Pub/Sub** | Google REST + Bearer | GCP cloud |
| **Redis Streams** | Redis4D (XREADGROUP) | Redis como broker |
| **HTTP Ingress** | Indy HTTP Server | Webhooks, telemetria |
| **TCP** | Indy ou Synapse | Apps Delphi legadas, IoT |

**Plugavel:** implemente `ISidekiqQueueAdapter` (5 metodos) para qualquer broker.

---

## Features Core

### Batch & Long-polling
```pascal
.BatchSize(10)          // Busca 10 msgs por requisicao
.WaitTimeSeconds(20)    // Long-poll: espera ate 20s por mensagens
```

### Concorrencia
```pascal
.Concurrency(8)                    // 8 workers globais
.QueueConcurrency('critical', 4)   // Max 4 na fila 'critical'
.ActionConcurrency('emissao', 2)   // Max 2 para emissao simultanea
```

### Retry & Dead-Letter
```pascal
.RetryPolicy(TSidekiqSimpleRetryPolicy.New(5, 30))
// 5 tentativas, 30s entre cada
// Apos 5: move para dead-letter automaticamente
```

### Shutdown Gracioso
```pascal
Server.Stop;
// Aguarda workers ativos finalizarem
// Nenhum job perdido
```

---

## Features Pro

### Scheduled Jobs
```pascal
Server.EnqueueIn('emails', 'welcome', '{"user":1}', 300);  // Em 5 min
Server.EnqueueAt('reports', 'daily', '{}', Tomorrow9AM);    // Horario fixo
```

### Periodic Jobs (Cron)
```pascal
Server.RegisterPeriodic('cleanup', '0 3 * * *', 'default', 'cleanup', '{}');
// Todo dia as 3h da manha
```

### Batch Jobs
```pascal
var Batch := Server.Batch('importacao-lote-42');
Batch
  .Enqueue('default', 'import', '{"file":"clientes.csv"}')
  .Enqueue('default', 'import', '{"file":"produtos.csv"}')
  .OnComplete('default', 'notify', '{"msg":"Lote pronto"}');
// Callback executa quando TODOS os jobs do batch concluem
```

### Unique Jobs
```pascal
// Previne duplicatas na fila
Attrs.Values['unique_key'] := 'nota-123';
Attrs.Values['unique_strategy'] := 'until_executed';
```

### Rate Limiting
```pascal
.RateLimiter(TSidekiqTokenBucketRateLimiter.New(StateStore))
// Token bucket: 100 requests por 60 segundos por chave
```

---

## Features Enterprise

### Leader Election
```pascal
.UseLeaderElection(True)
.LeaderName('worker-cluster-1')
// Apenas 1 processo executa periodic/scheduled promotion
// Automatico com StateStore distribuido
```

### Client Reliability (Outbox)
```pascal
.ClientOutbox(TSidekiqFileClientOutbox.New('outbox.json'))
// Jobs sobrevivem a crash do processo
// Flush automatico a cada ciclo
```

### Server Reliability
```pascal
// Heartbeat automatico durante execucao longa
// Renova visibility + lock + idempotency
// Recovery de jobs de servidores que crasharam
```

### Circuit Breaker
```pascal
.UseServerMiddleware(TSidekiqCircuitBreakerMiddleware.New
  .FailureThreshold(5)
  .CooldownSeconds(60))
// Protege providers externos de cascata de falhas
```

---

## Middlewares

Pipeline encadeavel que executa antes (client) ou durante (server) cada job:

```pascal
TSidekiqServer.New
  // Client: roda antes de publicar
  .UseClientMiddleware(TLoggingClientMiddleware.Create)

  // Server: roda antes de executar
  .UseServerMiddleware(TValidationMiddleware.Create)
  .UseServerMiddleware(TTimingMiddleware.Create)
  .UseServerMiddleware(TCircuitBreakerMiddleware.Create)
```

### Middlewares incluidos

| Middleware | Tipo | Funcao |
|-----------|------|--------|
| Logging | Server | JSON estruturado (timestamp, id, action, duration) |
| Timeout | Server | Aborta apos N ms |
| Deduplication | Server | Skip por idempotency key |
| Compression | Client+Server | ZLib no body |
| Prometheus | Server | Counters + histogram |
| Circuit Breaker | Server | Open/closed/half-open por action |

**Crie o seu:** implemente `ISidekiqServerMiddleware` (1 metodo: `Call`).

---

## State Stores

| Store | Dependencia | Cenario |
|-------|-------------|---------|
| **InMemory** | Nenhuma | Testes, single-process simples |
| **SQLite** | FireDAC (incluso no Delphi) | Single-process, Windows Service |
| **PostgreSQL** | FireDAC + libpq.dll | Multi-processo, produção |
| **Redis** | Redis4D | Alta performance, distribuido |
| **MongoDB** | HTTP (Atlas Data API) | Cloud-native |

Usado para: locks, idempotency, leader election, rate limiting, batch state, periodic windows.

---

## Telemetria & Observabilidade

### Eventos de ciclo de vida
```
server.started / server.stopped
job.started / job.succeeded / job.failed
job.retried / job.dead_lettered / job.acked
```

### Implementacoes
```pascal
// Console (desenvolvimento)
.Telemetry(TSidekiqConsoleTelemetry.New)

// StatsD (producao)
.Telemetry(TSidekiqMetricsTelemetry.New('statsd://localhost:8125'))

// Composto (ambos)
.Telemetry(TSidekiqCompositeTelemetry.New([Console, StatsD]))
```

### Middleware Prometheus
```pascal
var Prom := TSidekiqPrometheusMiddleware.New;
Server.UseServerMiddleware(Prom);
// Depois: Prom.Expose retorna metricas em formato Prometheus text
```

---

## Comparativo com Sidekiq Ruby

| Feature | Sidekiq OSS | Sidekiq Pro | Sidekiq Enterprise | **Sidekiq4D** |
|---------|:-:|:-:|:-:|:-:|
| Job processing | x | x | x | **x** |
| Retry/backoff | x | x | x | **x** |
| Scheduled jobs | x | x | x | **x** |
| Concurrency | x | x | x | **x** |
| Middleware | x | x | x | **x** |
| Batch jobs | | x | x | **x** |
| Unique jobs | | x | x | **x** |
| Rate limiting | | x | x | **x** |
| Periodic jobs | | | x | **x** |
| Leader election | | | x | **x** |
| Rolling restarts | | | x | **x** |
| Pause/resume | | | x | **x** |
| Multi-queue | | | x | **x** |
| **Preco** | Gratis | $99/mo | $179/mo | **Gratis** |

> Sidekiq4D inclui todas as features Pro + Enterprise **sem custo**.

---

## Casos de Uso

| Caso | Padrao usado |
|------|--------------|
| Workers SQS (NFSe, NFe, NFCe) | Adapter SQS + Concurrency + Retry |
| Agente de telemetria | HTTP Ingress + Circuit Breaker + Providers |
| Email sender | Rate Limiting + Retry |
| PDF generator | Batch + OnComplete callback |
| Webhook dispatcher | Circuit Breaker + Exponential backoff |
| ETL pipeline | Job chaining (EnqueueIn) |
| Notification hub | Fan-out routing + per-channel concurrency |
| Windows Service | Template Service + StopWhenIdle |
| IoT data collector | TCP adapter + Pool processing |

---

## Getting Started

### 1. Clone

```bash
git clone https://github.com/herlondf/sidekiq4d.git
```

### 2. Adicione ao search path

```
sidekiq4d/src/
```

### 3. Crie um handler

```pascal
type
  TMyHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TMyHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'my_action';
end;

procedure TMyHandler.Perform(const AContext: ISidekiqJobContext);
begin
  // Sua logica aqui
  ProcessData(AContext.Job.Body);
end;
```

### 4. Configure o servidor

```pascal
var Queue := TSidekiqInMemoryQueueAdapter.New;
Queue.Enqueue('my_action', '{"data":"hello"}');

TSidekiqServer.New
  .UseQueue(Queue)
  .RegisterHandler('my_action', TMyHandler.Create)
  .Run;
```

**Pronto.** 4 passos, menos de 20 linhas.

---

## Demos Incluidos

| # | Demo | Cenario |
|---|------|---------|
| 1 | BasicConsole | Job simples |
| 2 | ScheduledJobs | Cron + EnqueueIn |
| 3 | BatchJobs | Batch + callbacks |
| 4 | ConcurrencyControl | Limits + rate + unique |
| 5 | Middleware | Pipeline client/server |
| 6 | Reliability | Outbox + leader + pause |
| 7 | RetryDLQ | Backoff + dead-letter |
| 8 | SQLite | FireDAC local |
| 9 | Postgres | FireDAC distribuido |
| 10 | Redis | Redis4D full |
| 11 | WindowsService | Template service |
| 12 | SqsConsole | AWS SQS |
| 13 | HTTPIngress | Webhook receiver |
| 14 | CircuitBreaker | Provider protection |
| 15 | TelemetryAgent | Agente completo |
| 16 | EmailSender | Rate limit |
| 17 | PDFGenerator | Batch parallel |
| 18 | WebhookDispatcher | Resilient delivery |
| 19 | ETLPipeline | Job chaining |
| 20 | NotificationHub | Multi-channel routing |
| 21 | SQSBenchmark | Comparativo com/sem |

---

## Roadmap

- [ ] Dashboard web (real-time job monitoring)
- [ ] Adapter NATS / Apache Pulsar
- [ ] OpenTelemetry nativo (traces + metrics)
- [ ] Client SDK cross-language (publish de qualquer stack)
- [ ] Delphi package manager (Boss / Delphinus)

---

## Contribuindo

- **License:** BSD
- **GitHub:** github.com/herlondf/sidekiq4d
- **Issues:** bug reports, feature requests
- **PRs:** novos adapters, middlewares, stores

### Padroes

- Interfaces para extensibilidade
- API fluente (method chaining)
- `class function New` como factory
- Thread-safety: `TCriticalSection` / `TInterlocked` / `TThreadedQueue`

---

<p align="center">
  <img src="logo.png" alt="Sidekiq4D" width="120"><br>
  <strong>Sidekiq4D</strong><br>
  <em>Async Job Framework for Delphi</em><br><br>
  github.com/herlondf/sidekiq4d
</p>
