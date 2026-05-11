# Receita: Servidor Básico (InMemory)

Servidor mínimo funcional sem dependências externas.

```pascal
program ServidorBasico;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry.Console;

type
  TSendEmailHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TSendEmailHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'send_email';
end;

procedure TSendEmailHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('Enviando email: ', AJob.Body);
  // Aqui vai a lógica real
end;

var
  LStore: ISidekiqStateStore;
  LQueue: ISidekiqQueueAdapter;
  LServer: ISidekiqServer;

begin
  ReportMemoryLeaksOnShutdown := True;

  LStore := TSidekiqInMemoryStateStore.New;
  LQueue := TSidekiqInMemoryQueueAdapter.New;

  LServer := TSidekiqServer.New
    .UseQueue(LQueue)
    .Concurrency(2)
    .IdleDelayMs(500)
    .StateStore(LStore)
    .RetryPolicy(TSidekiqExponentialRetryPolicy.New(3, 10, 300))
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('send_email', TSendEmailHandler.Create)
    .Run;

  // Enfileirar um job de teste
  LQueue.Enqueue('send_email', '{"to":"user@example.com"}');

  Writeln('Pressione Enter para parar...');
  ReadLn;

  LServer.Stop;
end.
```

**Dependências:** apenas `src/` e `src/adapters/` no library path. Sem serviços externos.
