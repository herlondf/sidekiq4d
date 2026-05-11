# Guia de Apresentacao — Sidekiq4D

## Publico-alvo
Comunidade Delphi (open source) — desenvolvedores que precisam de processamento
assincrono robusto em aplicacoes Delphi.

## Formatos
- `docs/presentation.md` — manual/apresentacao navegavel (Markdown)
- `docs/presentation.pptx` — slides para apresentacao (PowerPoint)

---

## Estrutura de Topicos

### Slide 1 — Capa
- Logo Sidekiq4D
- Tagline: "Async Job Framework for Delphi"
- GitHub URL

### Slide 2 — O Problema
- Loops manuais de consumo de fila (Sleep + Get(1))
- Processamento sequencial, 1 mensagem por vez
- Sem retry, sem observabilidade, sem controle de concorrencia
- Codigo espalhado, dificil de testar

### Slide 3 — A Solucao
- Framework completo de processamento assincrono
- API fluente: configuracao em 10 linhas
- Seguro por padrao (Concurrency=1, ack apos sucesso)
- Extensivel: adapters, middlewares, stores

### Slide 4 — Antes vs Depois (codigo)
- **Antes**: loop manual com 30+ linhas, Sleep, try/except
- **Depois**: TSidekiqServer.New.UseQueue(...).RegisterHandler(...).Run
- Side-by-side visual

### Slide 5 — Benchmark Real (SQS)
- Tabela: 3.6 msgs/s → 15.1 msgs/s (**4.2x**)
- Tempo: 56s → 13s
- Grafico de barras comparativo

### Slide 6 — Arquitetura
- Diagrama em camadas:
  - App → Server → Queue Adapter → Dispatcher → Worker Pool → Handler
  - Providers: StateStore, Lock, Retry, Telemetry

### Slide 7 — Queue Adapters
- Tabela com todos os adapters disponiveis:
  - InMemory, SQS, RabbitMQ, Kafka, Azure Service Bus, Google Pub/Sub
  - Redis Streams, HTTP Ingress, TCP (Indy/Synapse)
- "Plugavel: implemente ISidekiqQueueAdapter"

### Slide 8 — Features Core
- Batch + BatchSize configuravel
- Long-polling
- Concurrency (global, por fila, por action)
- Retry com backoff exponencial
- Dead-letter queue
- Shutdown gracioso

### Slide 9 — Features Pro
- Scheduled Jobs (EnqueueIn / EnqueueAt)
- Periodic Jobs (cron nativo)
- Batch Jobs (OnComplete / OnSuccess callbacks)
- Unique Jobs (deduplicacao)
- Rate Limiting (token bucket)
- Idempotency

### Slide 10 — Features Enterprise
- Leader Election (cluster-wide singleton)
- Client Reliability (outbox pattern)
- Server Reliability (heartbeat + stale recovery)
- Rolling Restarts (drain gracioso)
- Pause/Resume de filas
- Circuit Breaker (por provider)

### Slide 11 — Middlewares
- Pipeline encadeavel (client + server)
- Exemplos: Logging, Timeout, Deduplication, Compression, Prometheus
- "Crie o seu: implemente ISidekiqServerMiddleware"

### Slide 12 — State Stores
- InMemory (zero dependencia)
- SQLite (single-process, arquivo local)
- PostgreSQL (multi-processo)
- Redis (via Redis4D, alta performance)
- MongoDB (Atlas Data API)

### Slide 13 — Telemetria & Observabilidade
- Console, StatsD/DogStatsD, Historical metrics
- Composite (combine multiplos)
- Eventos: started, succeeded, failed, retried, dead-lettered
- Middleware Prometheus com /metrics

### Slide 14 — Comparativo com Sidekiq Ruby
- Tabela: OSS vs Pro vs Enterprise vs **Sidekiq4D**
- Tudo incluido, sem licenca por tier
- Zero dependencia obrigatoria

### Slide 15 — Casos de Uso
- Workers SQS (NFSe, NFe, NFCe)
- Agente de telemetria (HTTP → providers)
- Email sender com rate limit
- PDF generator com batch
- Webhook dispatcher com circuit breaker
- ETL pipeline com job chaining
- Notification hub multi-canal

### Slide 16 — Getting Started
- 3 passos:
  1. Clone + adicione `src/` ao search path
  2. Crie handler (ISidekiqJobHandler)
  3. Configure server (TSidekiqServer.New...)
- Codigo minimo funcional (5 linhas)

### Slide 17 — Exemplos & Demos
- Lista dos 20+ demos com descricao curta
- Link para cada pasta de exemplo

### Slide 18 — Roadmap
- Dashboard web (futuro)
- Adapter NATS/Pulsar
- Metricas OpenTelemetry nativas
- Client SDK para publicacao cross-language

### Slide 19 — Contribuindo
- MIT/BSD License
- GitHub: issues, PRs
- Padroes: interfaces, fluent API, adapters

### Slide 20 — Encerramento
- Logo
- GitHub URL
- "Star the repo!"

---

## Notas de construcao

- Slides devem ter pouco texto, mais visual (diagramas, tabelas, codigo)
- Codigo deve ser sintaticamente valido e curto (max 10 linhas por slide)
- Tabelas devem ter numeros reais do benchmark
- Usar cores consistentes: roxo (brand), verde (sucesso), vermelho (problema)
