# Dashboard Web

Interface web para monitoramento e gestão do servidor Sidekiq4D em tempo real.

## Iniciando

```pascal
uses
  Sidekiq4D.Dashboard;

TSidekiqWebDashboard.New
  .Port(8080)
  .Start;
```

Acesse em `http://localhost:8080`.

## Endpoints REST

### Saúde e métricas

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/health` | Status do servidor (JSON) |
| GET | `/metrics` | Métricas no formato Prometheus |

### Monitoramento

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/api/workers` | Workers ativos e status |
| GET | `/api/queues` | Filas e contagem de jobs |
| GET | `/api/dlq` | Jobs na Dead Letter Queue |

### Gestão

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| DELETE | `/api/scheduled` | Remove jobs agendados |
| POST | `/api/dlq/reprocess` | Recoloca jobs da DLQ na fila |

## Interface web

O dashboard usa Bootstrap 5 + Chart.js com:
- Visualização em tempo real via SSE (Server-Sent Events)
- Gráficos de throughput e taxa de erros
- Auto-refresh sem necessidade de F5
- Listagem de jobs agendados com opção de cancelamento
- Inspeção de jobs na DLQ com reprocessamento individual

## Métricas Prometheus

O endpoint `/metrics` expõe contadores compatíveis com Prometheus:

```
sidekiq4d_jobs_processed_total{queue="default",status="success"}
sidekiq4d_jobs_processed_total{queue="default",status="failed"}
sidekiq4d_workers_active
sidekiq4d_queue_depth{queue="default"}
```

Configure o Prometheus para scrape em `http://host:8080/metrics`.

## Jaeger / OTLP com Docker

Para observabilidade completa com traces:

```yaml
# docker/docker-compose.yml
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"   # UI Jaeger
      - "4318:4318"     # OTLP HTTP
```

```bash
cd docker && docker-compose up -d jaeger
```

Configurar o servidor para enviar traces:
```pascal
.Telemetry(TSidekiqOTLPTraceTelemetry.New(
  'http://localhost:4318',  // endpoint OTLP
  'meu-servico'             // service name
))
```

Acessar UI em `http://localhost:16686`.

Ver receita completa em [06-receitas/telemetria-otlp.md](../06-receitas/telemetria-otlp.md) e [06-receitas/dashboard-web.md](../06-receitas/dashboard-web.md).
