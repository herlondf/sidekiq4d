# Recipe: Basic Server (InMemory)

Minimal working server with no external dependencies.

```pascal
program BasicServer;

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
  Writeln('Sending email: ', AJob.Body);
  // Real logic goes here
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

  // Enqueue a test job
  LQueue.Enqueue('send_email', '{"to":"user@example.com"}');

  Writeln('Press Enter to stop...');
  ReadLn;

  LServer.Stop;
end.
```

**Dependencies:** only `src/` and `src/adapters/` in the library path. No external services.
