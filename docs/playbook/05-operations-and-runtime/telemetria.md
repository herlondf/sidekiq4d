# Telemetry

## IHefestoTelemetry interface

13 events covering the complete lifecycle of the server and jobs:

| Event | When it fires |
|-------|--------------|
| `ServerStarted` | Server initialized and workers active |
| `ServerStopped` | Server gracefully stopped |
| `JobStarted` | Job dequeued and starting execution |
| `JobSucceeded` | Job executed successfully |
| `JobFailed` | Job failed (exception in handler) |
| `JobAcked` | Ack sent to broker after success |
| `JobRetried` | Job scheduled for retry after failure |
| `JobDiscarded` | Job discarded after exhausting retries |
| `JobDeadLettered` | Job moved to DLQ |
| `FetchFinished` | Fetch cycle completed (with or without jobs) |
| `Idle` | Worker with no jobs to process |
| `PollingStarted` | Start of scheduler polling cycle |
| `PollingFinished` | End of scheduler polling cycle |

## Available providers

| Provider | Unit | What it does |
|----------|------|-------------|
| `THefestoNoopTelemetry` | `Hefesto.Telemetry` | No action (default) |
| `THefestoConsoleTelemetry` | `Hefesto.Telemetry.Console` | Prints events to the console |
| `THefestoCompositeTelemetry` | `Hefesto.Telemetry` | Chains multiple providers |
| `THefestoMetricsTelemetry` | `Hefesto.Telemetry.Metrics` | StatsD — counters and timers |
| `THefestoHistoricalMetricsTelemetry` | `Hefesto.Telemetry.Historical` | In-memory metric buckets |
| `THefestoOTLPTraceTelemetry` | `Hefesto.Telemetry.OTLP` | OpenTelemetry traces (Jaeger/Tempo/Honeycomb) |

## Composite: multiple providers

```pascal
.Telemetry(
  THefestoCompositeTelemetry.New([
    THefestoConsoleTelemetry.New,
    THefestoOTLPTraceTelemetry.New('http://localhost:4318', 'my-service'),
    THefestoMetricsTelemetry.New('localhost', 8125)  // StatsD
  ])
)
```

The order does not affect semantics — all providers receive each event.

## OTLP / OpenTelemetry

```pascal
uses
  Hefesto.Telemetry.OTLP;

.Telemetry(
  THefestoOTLPTraceTelemetry.New(
    'http://localhost:4318',  // OTLP HTTP endpoint
    'sidekiq4d-worker'        // service.name in traces
  )
)
```

Each job generates a span with:
- `job.action` — action name
- `job.queue` — queue name
- `job.id` — job ID
- Status: OK (success) or ERROR (failure with exception message)

**Supported backends:** Jaeger (via OTLP), Grafana Tempo, Honeycomb, any OTLP HTTP collector.

### Common issue: timezone

`DateTimeToUnix` in Delphi has a timezone parameter. For OTLP, use `False` to indicate that `TDateTime` is local (converts to UTC internally):

```pascal
// Correct for OTLP
UnixTimestamp := DateTimeToUnix(Now, False);
```

If traces appear in Jaeger with the wrong timestamp, check this parameter.

## Console Telemetry (development)

```pascal
.Telemetry(THefestoConsoleTelemetry.New)
```

Sample output:
```
[Hefesto] ServerStarted workers=4
[Hefesto] JobStarted queue=default action=send_email id=abc123
[Hefesto] JobSucceeded queue=default action=send_email duration=142ms
[Hefesto] Idle queue=default
```

## Historical metrics (in-memory)

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

  // Query accumulated metrics
  Writeln('Total processed: ', LMetrics.TotalProcessed);
  Writeln('Total failed: ', LMetrics.TotalFailed);
end;
```

See recipe in [06-recipes/telemetria-otlp.md](../06-recipes/telemetria-otlp.md).
