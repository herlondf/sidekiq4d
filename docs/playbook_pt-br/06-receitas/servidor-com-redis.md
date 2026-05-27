# Receita: Servidor com Redis Streams

Requer Redis local ou remoto. Usa Redis4D via XREADGROUP para consumer groups.

```pascal
program ServidorRedis;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.RedisStreams,
  Hefesto.Store.Redis4D,
  Hefesto.Retry,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console;

const
  REDIS_URL = 'redis://localhost:6379';

type
  TProcessHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TProcessHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process';
end;

procedure TProcessHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processando: ', AJob.Body);
end;

var
  LStore: IHefestoStateStore;
  LServer: IHefestoServer;
begin
  LStore := THefestoRedis4DStateStore.New
    .ConnectionString(REDIS_URL);

  LServer := THefestoServer.New
    .UseQueue(
      THefestoRedisStreamsAdapter.New
        .ConnectionString(REDIS_URL)
        .StreamName('sidekiq4d:jobs')
        .ConsumerGroup('workers')
        .ConsumerName('worker-1')   // único por instância
        .BlockMs(2000)              // block timeout no XREADGROUP
    )
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
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
