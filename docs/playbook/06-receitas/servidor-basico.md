# Receita: Servidor Básico (InMemory)

Servidor mínimo funcional sem dependências externas.

```pascal
program ServidorBasico;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

type
  TSendEmailHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TSendEmailHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'send_email';
end;

procedure TSendEmailHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Enviando email: ', AJob.Body);
  // Aqui vai a lógica real
end;

var
  LStore: IHefestoStateStore;
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;

begin
  ReportMemoryLeaksOnShutdown := True;

  LStore := THefestoInMemoryStateStore.New;
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(2)
    .IdleDelayMs(500)
    .StateStore(LStore)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(3, 10, 300))
    .Telemetry(THefestoConsoleTelemetry.New)
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
