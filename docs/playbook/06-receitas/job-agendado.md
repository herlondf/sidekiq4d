# Receita: Job Agendado

Agendar um job para executar em data/hora futura.

```pascal
program JobAgendado;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Scheduled,
  Hefesto.Telemetry.Console;

type
  TRelatorioHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TRelatorioHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'gerar_relatorio';
end;

procedure TRelatorioHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Gerando relatório às ', TimeToStr(Now), ': ', AJob.Body);
end;

var
  LStore: IHefestoStateStore;
  LScheduledStore: IHefestoScheduledStore;
  LServer: IHefestoServer;
  LEntry: THefestoScheduledEntry;
begin
  LStore := THefestoInMemoryStateStore.New;
  LScheduledStore := THefestoStateStoreScheduledStore.New(LStore);

  // Agendar para daqui a 5 segundos
  LEntry := MakeScheduledEntry(
    'default',            // queue
    'gerar_relatorio',    // action
    '{"tipo": "mensal"}', // body
    [],                   // atributos extras
    Now + (5 / 86400)     // DueAt: Now + 5 segundos
  );
  LScheduledStore.Schedule(LEntry);
  Writeln('Job agendado para: ', DateTimeToStr(LEntry.DueAt));

  LServer := THefestoServer.New
    .UseQueue(THefestoInMemoryQueueAdapter.New)
    .StateStore(LStore)
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('gerar_relatorio', TRelatorioHandler.Create)
    .Run;

  Writeln('Aguardando execução do job agendado...');
  ReadLn;
  LServer.Stop;
end.
```

**Cancelando um job agendado:**
```pascal
LScheduledStore.Delete('default', 'gerar_relatorio', LEntry.DueAt);
```

**Listando todos os agendados:**
```pascal
var LList := LScheduledStore.List;
for var E in LList do
  Writeln(Format('%s em %s', [E.Action, DateTimeToStr(E.DueAt)]));
```

Ver [scheduled-e-periodic.md](../04-features/scheduled-e-periodic.md) para jobs periódicos (cron).
