# Receita: Dashboard Web

Servidor com dashboard web, métricas Prometheus e monitoramento em tempo real.

```pascal
program DashboardWeb;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry.Console,
  Sidekiq4D.Dashboard;

type
  TLentoHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

  TRapidoHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TLentoHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'lento';
end;

procedure TLentoHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Sleep(2000);  // simula job lento
  Writeln('Lento concluído: ', AJob.JobId);
end;

function TRapidoHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'rapido';
end;

procedure TRapidoHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Sleep(50);
  Writeln('Rápido concluído: ', AJob.JobId);
end;

var
  LQueue: ISidekiqQueueAdapter;
  LServer: ISidekiqServer;
  LDashboard: ISidekiqWebDashboard;
  I: Integer;
begin
  Randomize;
  LQueue := TSidekiqInMemoryQueueAdapter.New;

  LServer := TSidekiqServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(TSidekiqInMemoryStateStore.New)
    .RetryPolicy(TSidekiqExponentialRetryPolicy.New(3, 5, 60))
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('lento',  TLentoHandler.Create)
    .RegisterHandler('rapido', TRapidoHandler.Create)
    .Run;

  // Iniciar dashboard na porta 8080
  LDashboard := TSidekiqWebDashboard.New
    .Port(8080)
    .Start;

  // Gerar carga contínua para visualizar no dashboard
  for I := 1 to 20 do
  begin
    LQueue.Enqueue('rapido', Format('{"id": %d}', [I]));
    if I mod 5 = 0 then
      LQueue.Enqueue('lento', Format('{"id": %d}', [I]));
  end;

  Writeln('Dashboard: http://localhost:8080');
  Writeln('Health:    http://localhost:8080/health');
  Writeln('Metrics:   http://localhost:8080/metrics');
  Writeln('Workers:   http://localhost:8080/api/workers');
  Writeln('DLQ:       http://localhost:8080/api/dlq');
  Writeln('');
  Writeln('Pressione Enter para parar...');
  ReadLn;

  LDashboard.Stop;
  LServer.Stop;
end.
```

## Endpoints disponíveis

```
GET  /health              → {"status":"ok","workers":4,"queues":["default"]}
GET  /metrics             → formato Prometheus
GET  /api/workers         → workers ativos e jobs em execução
GET  /api/queues          → profundidade de cada fila
GET  /api/dlq             → jobs na dead letter queue
DELETE /api/scheduled     → cancela job agendado
POST /api/dlq/reprocess   → reprocessa job da DLQ
```

## Integração Prometheus

Adicionar ao `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'sidekiq4d'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

Ver [dashboard-web.md](../04-features/dashboard-web.md) para detalhes sobre SSE e a interface SPA.
