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
- Atualização em tempo real via **WebSocket** (push a cada 1s sem polling)
- Fallback automático: WebSocket → SSE → polling HTTP a cada 5s
- Indicador de status de conexão no canto da tela
- Gráficos de throughput e taxa de erros
- Listagem de jobs agendados com opção de cancelamento
- Inspeção de jobs na DLQ com reprocessamento individual

## WebSocket real-time

O endpoint `/ws` aceita conexões WebSocket (RFC 6455). O servidor faz push de métricas
a cada 1 segundo para todos os clientes conectados.

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');
ws.onmessage = (e) => {
  const data = JSON.parse(e.data);
  console.log(data.queued, data.workers, data.processed_total);
};
```

O payload enviado tem o mesmo formato do endpoint `/api/overview`:

```json
{
  "queued": 42,
  "workers": 4,
  "processed_total": 1234,
  "failed_total": 3,
  "scheduled": 7,
  "dlq": 0
}
```

A SPA do dashboard detecta suporte a WebSocket automaticamente. Se a conexão
falhar ou o navegador não suportar, ela cai para SSE (`/api/stream`) e depois
para polling HTTP a cada 5s.

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
