# Telemetria

## Interface ISidekiqTelemetry

13 eventos que cobrem o ciclo de vida completo do servidor e dos jobs:

| Evento | Quando dispara |
|--------|---------------|
| `ServerStarted` | Servidor inicializado e workers ativos |
| `ServerStopped` | Servidor parado graciosamente |
| `JobStarted` | Job retirado da fila e iniciando execução |
| `JobSucceeded` | Job executado com sucesso |
| `JobFailed` | Job falhou (exception no handler) |
| `JobAcked` | Ack enviado ao broker após sucesso |
| `JobRetried` | Job agendado para retry após falha |
| `JobDiscarded` | Job descartado após esgotar retries |
| `JobDeadLettered` | Job movido para DLQ |
| `FetchFinished` | Ciclo de fetch concluído (com ou sem jobs) |
| `Idle` | Worker sem jobs para processar |
| `PollingStarted` | Início de ciclo de polling do scheduler |
| `PollingFinished` | Fim de ciclo de polling do scheduler |

## Providers disponíveis

| Provider | Unit | O que faz |
|----------|------|-----------|
| `TSidekiqNoopTelemetry` | `Sidekiq4D.Telemetry` | Nenhuma ação (padrão) |
| `TSidekiqConsoleTelemetry` | `Sidekiq4D.Telemetry.Console` | Imprime eventos no console |
| `TSidekiqCompositeTelemetry` | `Sidekiq4D.Telemetry` | Encadeia múltiplos providers |
| `TSidekiqMetricsTelemetry` | `Sidekiq4D.Telemetry.Metrics` | StatsD — contadores e timers |
| `TSidekiqHistoricalMetricsTelemetry` | `Sidekiq4D.Telemetry.Historical` | Buckets de métricas em memória |
| `TSidekiqOTLPTraceTelemetry` | `Sidekiq4D.Telemetry.OTLP` | Traces OpenTelemetry (Jaeger/Tempo/Honeycomb) |

## Composite: múltiplos providers

```pascal
.Telemetry(
  TSidekiqCompositeTelemetry.New([
    TSidekiqConsoleTelemetry.New,
    TSidekiqOTLPTraceTelemetry.New('http://localhost:4318', 'meu-servico'),
    TSidekiqMetricsTelemetry.New('localhost', 8125)  // StatsD
  ])
)
```

A ordem não afeta a semântica — todos os providers recebem cada evento.

## OTLP / OpenTelemetry

```pascal
uses
  Sidekiq4D.Telemetry.OTLP;

.Telemetry(
  TSidekiqOTLPTraceTelemetry.New(
    'http://localhost:4318',  // OTLP HTTP endpoint
    'sidekiq4d-worker'        // service.name nos traces
  )
)
```

Cada job gera um span com:
- `job.action` — action name
- `job.queue` — nome da fila
- `job.id` — ID do job
- Status: OK (sucesso) ou ERROR (falha com exception message)

**Backends suportados:** Jaeger (via OTLP), Grafana Tempo, Honeycomb, qualquer coletor OTLP HTTP.

### Problema comum: timezone

`DateTimeToUnix` em Delphi tem um parâmetro de timezone. Para OTLP, use `False` para indicar que a `TDateTime` é local (converte para UTC internamente):

```pascal
// Correto para OTLP
UnixTimestamp := DateTimeToUnix(Now, False);
```

Se os traces aparecem no Jaeger com timestamp errado, verificar este parâmetro.

## Console Telemetry (desenvolvimento)

```pascal
.Telemetry(TSidekiqConsoleTelemetry.New)
```

Saída exemplo:
```
[Sidekiq4D] ServerStarted workers=4
[Sidekiq4D] JobStarted queue=default action=send_email id=abc123
[Sidekiq4D] JobSucceeded queue=default action=send_email duration=142ms
[Sidekiq4D] Idle queue=default
```

## Métricas históricas (in-memory)

```pascal
uses
  Sidekiq4D.Telemetry.Historical;

var
  LMetrics: TSidekiqHistoricalMetricsTelemetry;
begin
  LMetrics := TSidekiqHistoricalMetricsTelemetry.Create;

  TSidekiqServer.New
    .Telemetry(LMetrics)
    ...

  // Consultar métricas acumuladas
  Writeln('Total processados: ', LMetrics.TotalProcessed);
  Writeln('Total falhos: ', LMetrics.TotalFailed);
end;
```

Ver receita em [06-receitas/telemetria-otlp.md](../06-receitas/telemetria-otlp.md).
