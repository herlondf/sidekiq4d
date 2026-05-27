# Hefesto

<p align="center">
  <img src="docs/logo.png" alt="Hefesto" width="280">
</p>

<p align="center">
  Processamento de background jobs simples e eficiente para Delphi.
</p>

<p align="center">
  <a href="https://github.com/herlondf/hefesto/releases"><img src="https://img.shields.io/github/v/release/herlondf/hefesto?style=flat-square&color=blue" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="MIT License"></a>
  <a href="https://www.embarcadero.com/products/delphi"><img src="https://img.shields.io/badge/Delphi-11%2B-red?style=flat-square" alt="Delphi 11+"></a>
  <a href="#queue-adapters"><img src="https://img.shields.io/badge/adapters-9%20brokers-purple?style=flat-square" alt="9 Adapters"></a>
  <a href="#exemplos"><img src="https://img.shields.io/badge/demos-25%20examples-orange?style=flat-square" alt="25 Demos"></a>
</p>

---

Hefesto é um framework Delphi para processar jobs de forma assíncrona, confiável
e eficiente. Ele substitui loops manuais de polling de fila por uma API declarativa
e fluente que gerencia concorrência, retry, observabilidade e ciclo de vida — para
você focar na lógica de negócio, não na infraestrutura.

## Performance

Benchmark em ambiente real consumindo 200 mensagens SQS via LocalStack, cada job
executando 50ms de trabalho simulado:

| | Loop manual | Hefesto | Melhoria |
|---|:---:|:---:|:---:|
| **Throughput** | 3.6 jobs/s | **15.1 jobs/s** | **4.2x** |
| **Tempo total** | 56.1s | **13.2s** | **4.3x mais rápido** |
| **Throughput de pico** | 4.9 jobs/s | **26.1 jobs/s** | **5.3x** |
| Tamanho do lote | 1 msg/requisição | 10 msgs/requisição | |
| Workers | 1 (sequencial) | 4 (concorrente) | |
| Estratégia ociosa | Sleep 15s | Long-poll 5s | |

> O padrão de loop manual (`Get(1)` + `Sleep(15s)`) representa a estratégia de
> consumo real encontrada em workers SQS Delphi em produção.
> A melhoria do Hefesto vem de batching, concorrência e long-polling —
> sem nenhuma alteração no próprio handler do job.

## Primeiros Passos

### 1. Instalação

Clone e adicione `src/` ao search path do seu projeto Delphi:

```
git clone https://github.com/herlondf/hefesto.git
```

```
Search path: hefesto\src\
```

### 2. Defina um handler de job

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

### 3. Configure e execute

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

Pronto. Batch, long-polling, concorrência, retry, dead-letter e telemetria —
tudo configurado em 8 linhas.

## Requisitos

- **Delphi 11 Alexandria** ou superior
- Sem dependências externas obrigatórias

Opcionais (específicos por adapter):
- **Redis4D** — para state store Redis, locks, scheduled store
- **FireDAC** (incluído no Delphi) — para state store SQLite/Postgres
- **Synapse** (incluído em `src/vendor/synalist/`) — para adapter TCP

## Queue Adapters

| Adapter | Protocolo | Caso de uso |
|---------|-----------|-------------|
| **InMemory** | — | Testes, desenvolvimento |
| **SQS** | AWS HTTP + Sig V4 | AWS produção |
| **RabbitMQ** | HTTP Management API | Mensageria on-premise |
| **Kafka** | Confluent REST Proxy | Streaming de alto throughput |
| **Azure Service Bus** | Azure REST + SAS | Cloud Azure |
| **Google Pub/Sub** | Google REST + Bearer | Cloud GCP |
| **Redis Streams** | Redis4D (XREADGROUP) | Redis como broker |
| **HTTP Ingress** | Indy HTTP Server | Webhooks, agentes de telemetria |
| **TCP** | Indy ou Synapse | Apps Delphi legados, IoT |

Todos os adapters usam **somente HTTP** — nenhum SDK externo necessário.
Implemente `IHefestoQueueAdapter` (5 métodos) para adicionar o seu próprio.

## Funcionalidades

### Core
- Tamanho de lote configurável (1-10 por fetch)
- Suporte a long-polling
- Limites de concorrência globais, por fila e por ação
- Políticas de retry: `THefestoSimpleRetryPolicy` (delay fixo) e `THefestoExponentialRetryPolicy` (base × n²)
- Fila dead-letter automática
- Graceful shutdown (sem perda de jobs)

### Nível Pro
- **Jobs agendados** — `EnqueueIn(delay)` / `EnqueueAt(datetime)`
- **Jobs periódicos** — agendamento cron nativo (expressões de 5 campos)
- **Batch jobs** — callbacks `OnComplete` / `OnSuccess`
- **Unique jobs** — estratégias de deduplicação
- **Rate limiting** — token-bucket por chave
- **Idempotência** — prevenção de execução duplicada

### Nível Enterprise
- **Leader election** — operações singleton em cluster
- **Confiabilidade do cliente** — padrão outbox para recuperação de falhas
- **Confiabilidade do servidor** — heartbeat + recuperação de execuções travadas
- **Rolling restarts** — drain e retomada graciosa
- **Pause/resume** — controle operacional de filas
- **Circuit breaker** — proteção de provedores externos
- **Job Graph (DAG)** — dependências declarativas entre jobs, execução paralela, cancelamento em cascata
- **OpenTelemetry** — exportador de traces OTLP para Jaeger, Grafana Tempo, Honeycomb, Datadog
- **Dashboard Web** — SPA com métricas em tempo real (SSE), `/health`, `/metrics` (scrape Prometheus), cancelar jobs agendados

> Todas as funcionalidades incluídas. Sem planos pagos.

## Hefesto vs Sidekiq Ruby

| Funcionalidade | Sidekiq OSS | Sidekiq Pro | Sidekiq Enterprise | **Hefesto** |
|----------------|:-----------:|:-----------:|:------------------:|:-----------:|
| Processamento de jobs + retry | x | x | x | **x** |
| Agendamento + middleware | x | x | x | **x** |
| Batch + unique jobs | | $99/mês | $99/mês | **grátis** |
| Rate limiting | | $99/mês | $99/mês | **grátis** |
| Jobs periódicos | | | $179/mês | **grátis** |
| Leader election | | | $179/mês | **grátis** |
| Rolling restarts | | | $179/mês | **grátis** |

## Middlewares

Pipeline encadeável — executa antes da publicação (client) ou antes da execução (server):

| Middleware | Tipo | Finalidade |
|-----------|------|------------|
| Logging | Server | Saída JSON estruturada |
| Timeout | Server | Abortar após N ms |
| Deduplication | Server | Ignorar por chave de idempotência |
| Compression | Client+Server | Compressão ZLib do body |
| Prometheus | Server | Contadores + métricas de histograma |
| Circuit Breaker | Server | Open/closed/half-open por ação |

Implemente `IHefestoServerMiddleware` (1 método) para adicionar o seu próprio.

## State Stores

| Store | Dependência | Caso de uso |
|-------|-------------|-------------|
| **InMemory** | Nenhuma | Testes |
| **SQLite** | FireDAC (incluído) | Processo único, Windows Service |
| **PostgreSQL** | FireDAC + libpq | Multi-processo, distribuído |
| **Redis** | Redis4D | Alta performance |
| **MongoDB** | HTTP (Atlas API) | Cloud-native |

## Exemplos

| Exemplo | Cenário |
|---------|---------|
| [BasicConsole](samples/BasicConsole) | Job simples, handler, telemetria |
| [ScheduledJobs](samples/ScheduledJobs) | EnqueueIn, EnqueueAt, Periodic (cron) |
| [BatchJobs](samples/BatchJobs) | Batch com callbacks OnComplete/OnSuccess |
| [ConcurrencyControl](samples/ConcurrencyControl) | Limites, rate limit, unique, idempotência |
| [Middleware](samples/Middleware) | Pipeline de middleware client + server |
| [Reliability](samples/Reliability) | Outbox, leader election, pause/resume |
| [RetryDLQ](samples/RetryDLQ) | Backoff exponencial, dead-letter |
| [SQLite](samples/SQLite) | State store SQLite via FireDAC |
| [Postgres](samples/Postgres) | State store PostgreSQL via FireDAC |
| [Redis](samples/Redis) | Integração completa com Redis4D |
| [WindowsService](samples/WindowsService) | Template de Windows Service |
| [SqsConsole](samples/SqsConsole) | AWS SQS com Signature V4 |
| [HTTPIngress](samples/HTTPIngress) | Receptor de webhook HTTP |
| [CircuitBreaker](samples/CircuitBreaker) | Proteção de provedores externos |
| [TelemetryAgent](samples/TelemetryAgent) | Agente de telemetria completo (HTTP + provedores) |
| [EmailSender](samples/EmailSender) | E-mail assíncrono com rate limiting |
| [PDFGenerator](samples/PDFGenerator) | Geração de PDF em lote com callback |
| [WebhookDispatcher](samples/WebhookDispatcher) | Entrega resiliente de webhooks |
| [ETLPipeline](samples/ETLPipeline) | Encadeamento de jobs (extract->transform->load) |
| [NotificationHub](samples/NotificationHub) | Roteamento fan-out multi-canal |
| [SQSBenchmark](samples/SQSBenchmark) | Benchmark real: manual vs Hefesto |
| [VCLDemo](samples/VCLDemo) | Comparação visual split-screen |
| [JobGraph](samples/JobGraph) | DAG de jobs dependentes com execução paralela |
| [OTLPTelemetry](samples/OTLPTelemetry) | Exportação de traces OpenTelemetry para Jaeger/Tempo |
| [HorseIntegration](samples/HorseIntegration) | Integração com middleware do framework Horse |

## Estrutura do Projeto

```
src/                ~35 units do framework
src/adapters/       ~35 units de adapters (filas, middlewares, provedores, telemetria)
src/vendor/         Synapse (embutido)
samples/            25 demos executáveis
tests/              Testes unitários DUnitX (11 fixtures) + thread-safety + smoke Redis
docs/               Apresentação (md + pptx)
docker/             docker-compose para Redis, Postgres e Jaeger locais
```

## Contribuindo

Pull requests são bem-vindos. Para mudanças maiores, abra uma issue primeiro para discussão.

**Padrões a seguir:**
- Interfaces para extensibilidade (`IHefestoQueueAdapter`, `IHefestoServerMiddleware`)
- API fluente (encadeamento de métodos retornando `Self`)
- `class function New` como factory
- Thread-safety: `TCriticalSection` / `TInterlocked` / `TThreadedQueue`

## A Família Olímpica

> *Poseidon comanda os mares — transporte bruto, a força das ondas.*
> *Triton guarda as águas do pai — gerencia o que flui, retém o que não pode se perder.*
> *Pégaso voa pelos céus — nasceu do sangue de Medusa, pela espada que Hermes deu a Perseu.*
> *Hermes percorre todos os reinos — carrega mensagens entre deuses, mortais e monstros, mais rápido que qualquer onda.*
> *Hefesto forja nas profundezas — invisível, incansável, transformando matéria bruta em obra acabada.*

| Projeto | Mito | Papel |
|---------|------|-------|
| [**Poseidon**](https://github.com/herlondf/poseidon) | Deus dos mares | Camada de transporte assíncrono — IOCP/epoll, I/O bruto |
| [**Triton**](https://github.com/herlondf/triton) | Filho de Poseidon, guardião das profundezas | Pool genérico de recursos — conexões, clients, SMTP |
| [**Pegasus**](https://github.com/herlondf/pegasus) | Nascido do sangue de Poseidon, montado por heróis | Framework HTTP — roteamento, middleware, provedores |
| [**Hermes**](https://github.com/herlondf/hermes) | Mensageiro dos deuses, guia entre os reinos | Client Redis — key-value rápido, pub/sub, mensageria |
| **Hefesto** (esta lib) | Ferreiro dos deuses, trabalha invisível nas profundezas | Background jobs — filas, workers, retry, agendamento |

---

## Inspiração

Hefesto é inspirado pelo [Sidekiq](https://github.com/sidekiq/sidekiq), o framework de background jobs battle-tested do ecossistema Ruby. As mesmas ideias centrais — filas confiáveis, concorrência, retry com backoff, dead-letter, observabilidade — trazidas nativamente para o Delphi.

---

## Licença

[MIT](LICENSE) — use livremente em projetos comerciais e open-source.

> 🇺🇸 Read this document in English: [README.md](./README.md)
