# Recipe: Web Dashboard

Server with web dashboard, Prometheus metrics, and real-time monitoring.

```pascal
program WebDashboard;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry.Console,
  Hefesto.Dashboard;

type
  TSlowHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TFastHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TSlowHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'slow';
end;

procedure TSlowHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Sleep(2000);  // simulate slow job
  Writeln('Slow completed: ', AJob.JobId);
end;

function TFastHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'fast';
end;

procedure TFastHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Sleep(50);
  Writeln('Fast completed: ', AJob.JobId);
end;

var
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
  LDashboard: IHefestoWebDashboard;
  I: Integer;
begin
  Randomize;
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(THefestoInMemoryStateStore.New)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(3, 5, 60))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('slow', TSlowHandler.Create)
    .RegisterHandler('fast', TFastHandler.Create)
    .Run;

  // Start dashboard on port 8080
  LDashboard := THefestoWebDashboard.New
    .Port(8080)
    .Start;

  // Generate continuous load to visualize in the dashboard
  for I := 1 to 20 do
  begin
    LQueue.Enqueue('fast', Format('{"id": %d}', [I]));
    if I mod 5 = 0 then
      LQueue.Enqueue('slow', Format('{"id": %d}', [I]));
  end;

  Writeln('Dashboard: http://localhost:8080');
  Writeln('Health:    http://localhost:8080/health');
  Writeln('Metrics:   http://localhost:8080/metrics');
  Writeln('Workers:   http://localhost:8080/api/workers');
  Writeln('DLQ:       http://localhost:8080/api/dlq');
  Writeln('');
  Writeln('Press Enter to stop...');
  ReadLn;

  LDashboard.Stop;
  LServer.Stop;
end.
```

## Available endpoints

```
GET  /health              → {"status":"ok","workers":4,"queues":["default"]}
GET  /metrics             → Prometheus format
GET  /api/workers         → active workers and running jobs
GET  /api/queues          → depth of each queue
GET  /api/dlq             → jobs in the dead letter queue
DELETE /api/scheduled     → cancel a scheduled job
POST /api/dlq/reprocess   → reprocess job from DLQ
```

## Prometheus integration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'sidekiq4d'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

See [dashboard-web.md](../04-features/dashboard-web.md) for details on SSE and the SPA interface.
