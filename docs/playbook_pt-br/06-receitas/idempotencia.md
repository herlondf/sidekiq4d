# Receita: Idempotência

Prevenir que o mesmo job seja processado mais de uma vez quando o broker entrega duplicatas.

```pascal
program Idempotencia;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console;

type
  TTransferenciaHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TTransferenciaHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'transferencia';
end;

procedure TTransferenciaHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // Este código executa no máximo uma vez por JobId
  Writeln('Executando transferência: ', AJob.Body);
  Writeln('JobId: ', AJob.JobId);
end;

var
  LStore: IHefestoStateStore;
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))
    .Telemetry(THefestoConsoleTelemetry.New)
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
  THefestoRenewableIdempotency.New(LStore, 86400)  // 86400s = 24h
)
```

**Com Redis para persistência entre restarts:**

```pascal
LStore := THefestoRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');

.Idempotency(THefestoStateStoreIdempotency.New(LStore))
```

Ver [idempotency.md](../04-features/idempotency.md) para detalhes sobre a interface.
