# Receita: Job Agendado

Agendar um job para executar em data/hora futura.

```pascal
program JobAgendado;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Telemetry.Console;

type
  TRelatorioHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TRelatorioHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'gerar_relatorio';
end;

procedure TRelatorioHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('Gerando relatório às ', TimeToStr(Now), ': ', AJob.Body);
end;

var
  LStore: ISidekiqStateStore;
  LScheduledStore: ISidekiqScheduledStore;
  LServer: ISidekiqServer;
  LEntry: TSidekiqScheduledEntry;
begin
  LStore := TSidekiqInMemoryStateStore.New;
  LScheduledStore := TSidekiqStateStoreScheduledStore.New(LStore);

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

  LServer := TSidekiqServer.New
    .UseQueue(TSidekiqInMemoryQueueAdapter.New)
    .StateStore(LStore)
    .Telemetry(TSidekiqConsoleTelemetry.New)
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
