# Web Dashboard

Web interface for real-time monitoring and management of the Hefesto server.

## Starting

```pascal
uses
  Hefesto.Dashboard;

THefestoWebDashboard.New
  .Port(8080)
  .Start;
```

Access at `http://localhost:8080`.

## REST Endpoints

### Health and metrics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Server status (JSON) |
| GET | `/metrics` | Metrics in Prometheus format |

### Monitoring

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/workers` | Active workers and status |
| GET | `/api/queues` | Queues and job counts |
| GET | `/api/dlq` | Jobs in the Dead Letter Queue |

### Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| DELETE | `/api/scheduled` | Removes scheduled jobs |
| POST | `/api/dlq/reprocess` | Returns DLQ jobs to the queue |

## Web interface

The dashboard uses Bootstrap 5 + Chart.js with:
- Real-time updates via **WebSocket** (push every 1s without polling)
- Automatic fallback: WebSocket → SSE → HTTP polling every 5s
- Connection status indicator in the corner of the screen
- Throughput and error rate charts
- Scheduled job listing with cancellation option
- DLQ job inspection with individual reprocessing

## WebSocket real-time

The `/ws` endpoint accepts WebSocket connections (RFC 6455). The server pushes metrics every 1 second to all connected clients.

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');
ws.onmessage = (e) => {
  const data = JSON.parse(e.data);
  console.log(data.queued, data.workers, data.processed_total);
};
```

The payload sent has the same format as the `/api/overview` endpoint:

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

The dashboard SPA detects WebSocket support automatically. If the connection fails or the browser does not support it, it falls back to SSE (`/api/stream`) and then to HTTP polling every 5s.

## Prometheus Metrics

The `/metrics` endpoint exposes Prometheus-compatible counters:

```
sidekiq4d_jobs_processed_total{queue="default",status="success"}
sidekiq4d_jobs_processed_total{queue="default",status="failed"}
sidekiq4d_workers_active
sidekiq4d_queue_depth{queue="default"}
```

Configure Prometheus to scrape at `http://host:8080/metrics`.

## Jaeger / OTLP with Docker

For complete observability with traces:

```yaml
# docker/docker-compose.yml
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"   # Jaeger UI
      - "4318:4318"     # OTLP HTTP
```

```bash
cd docker && docker-compose up -d jaeger
```

Configure the server to send traces:
```pascal
.Telemetry(THefestoOTLPTraceTelemetry.New(
  'http://localhost:4318',  // OTLP endpoint
  'my-service'              // service name
))
```

Access UI at `http://localhost:16686`.

See complete recipes in [06-recipes/telemetria-otlp.md](../06-recipes/telemetria-otlp.md) and [06-recipes/dashboard-web.md](../06-recipes/dashboard-web.md).
