# Telemetria

## Interface IHefestoTelemetry

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
| `THefestoNoopTelemetry` | `Hefesto.Telemetry` | Nenhuma ação (padrão) |
| `THefestoConsoleTelemetry` | `Hefesto.Telemetry.Console` | Imprime eventos no console |
| `THefestoCompositeTelemetry` | `Hefesto.Telemetry` | Encadeia múltiplos providers |
| `THefestoMetricsTelemetry` | `Hefesto.Telemetry.Metrics` | StatsD — contadores e timers |
| `THefestoHistoricalMetricsTelemetry` | `Hefesto.Telemetry.Historical` | Buckets de métricas em memória |
| `THefestoOTLPTraceTelemetry` | `Hefesto.Telemetry.OTLP` | Traces OpenTelemetry (Jaeger/Tempo/Honeycomb) |

## Composite: múltiplos providers

```pascal
.Telemetry(
  THefestoCompositeTelemetry.New([
    THefestoConsoleTelemetry.New,
    THefestoOTLPTraceTelemetry.New('http://localhost:4318', 'meu-servico'),
    THefestoMetricsTelemetry.New('localhost', 8125)  // StatsD
  ])
)
```

A ordem não afeta a semântica — todos os providers recebem cada evento.

## OTLP / OpenTelemetry

```pascal
uses
  Hefesto.Telemetry.OTLP;

.Telemetry(
  THefestoOTLPTraceTelemetry.New(
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
.Telemetry(THefestoConsoleTelemetry.New)
```

Saída exemplo:
```
[Hefesto] ServerStarted workers=4
[Hefesto] JobStarted queue=default action=send_email id=abc123
[Hefesto] JobSucceeded queue=default action=send_email duration=142ms
[Hefesto] Idle queue=default
```

## Métricas históricas (in-memory)

```pascal
uses
  Hefesto.Telemetry.Historical;

var
  LMetrics: THefestoHistoricalMetricsTelemetry;
begin
  LMetrics := THefestoHistoricalMetricsTelemetry.Create;

  THefestoServer.New
    .Telemetry(LMetrics)
    ...

  // Consultar métricas acumuladas
  Writeln('Total processados: ', LMetrics.TotalProcessed);
  Writeln('Total falhos: ', LMetrics.TotalFailed);
end;
```

Ver receita em [06-receitas/telemetria-otlp.md](../06-receitas/telemetria-otlp.md).
