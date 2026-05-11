# Receita: Servidor com Redis Streams

Requer Redis local ou remoto. Usa Redis4D via XREADGROUP para consumer groups.

```pascal
program ServidorRedis;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.RedisStreams,
  Sidekiq4D.Store.Redis4D,
  Sidekiq4D.Retry,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Telemetry.Console;

const
  REDIS_URL = 'redis://localhost:6379';

type
  TProcessHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TProcessHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process';
end;

procedure TProcessHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('Processando: ', AJob.Body);
end;

var
  LStore: ISidekiqStateStore;
  LServer: ISidekiqServer;
begin
  LStore := TSidekiqRedis4DStateStore.New
    .ConnectionString(REDIS_URL);

  LServer := TSidekiqServer.New
    .UseQueue(
      TSidekiqRedisStreamsAdapter.New
        .ConnectionString(REDIS_URL)
        .StreamName('sidekiq4d:jobs')
        .ConsumerGroup('workers')
        .ConsumerName('worker-1')   // único por instância
        .BlockMs(2000)              // block timeout no XREADGROUP
    )
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(TSidekiqStateStoreIdempotency.New(LStore))
    .RetryPolicy(TSidekiqExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('process', TProcessHandler.Create)
    .Run;

  Writeln('Aguardando jobs do Redis...');
  ReadLn;
  LServer.Stop;
end.
```

**Notas:**
- `ConsumerName` deve ser único por instância para consumer groups funcionarem corretamente
- `BlockMs` controla o timeout do XREADGROUP — 0 bloqueia indefinidamente
- O `LStore` Redis também persiste idempotência e estado entre restarts
- Para múltiplos workers na mesma máquina, use nomes diferentes: `worker-1`, `worker-2`, etc.

**Iniciando Redis com Docker:**
```bash
docker run -d -p 6379:6379 redis:7-alpine
```
