# Receita: Idempotência

Prevenir que o mesmo job seja processado mais de uma vez quando o broker entrega duplicatas.

```pascal
program Idempotencia;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Telemetry.Console;

type
  TTransferenciaHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TTransferenciaHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'transferencia';
end;

procedure TTransferenciaHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  // Este código executa no máximo uma vez por JobId
  Writeln('Executando transferência: ', AJob.Body);
  Writeln('JobId: ', AJob.JobId);
end;

var
  LStore: ISidekiqStateStore;
  LQueue: ISidekiqQueueAdapter;
  LServer: ISidekiqServer;
begin
  LStore := TSidekiqInMemoryStateStore.New;
  LQueue := TSidekiqInMemoryQueueAdapter.New;

  LServer := TSidekiqServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(TSidekiqStateStoreIdempotency.New(LStore))
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('transferencia', TTransferenciaHandler.Create)
    .Run;

  // Simular entrega duplicada do broker
  LQueue.Enqueue('transferencia', '{"valor": 100, "jobId": "abc123"}');
  LQueue.Enqueue('transferencia', '{"valor": 100, "jobId": "abc123"}');  // duplicata
  LQueue.Enqueue('transferencia', '{"valor": 100, "jobId": "abc123"}');  // duplicata

  // Apenas o primeiro será processado — os outros são descartados silenciosamente
  Writeln('3 mensagens enfileiradas (2 são duplicatas).');
  Writeln('Apenas 1 deve ser processada. Enter para parar...');
  ReadLn;
  LServer.Stop;
end.
```

**Com TTL (reprocessar após expirar):**

```pascal
// Permite reprocessar o mesmo job após 24 horas
.Idempotency(
  TSidekiqRenewableIdempotency.New(LStore, 86400)  // 86400s = 24h
)
```

**Com Redis para persistência entre restarts:**

```pascal
LStore := TSidekiqRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');

.Idempotency(TSidekiqStateStoreIdempotency.New(LStore))
```

Ver [idempotency.md](../04-features/idempotency.md) para detalhes sobre a interface.
