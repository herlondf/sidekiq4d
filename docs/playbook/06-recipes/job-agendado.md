# Recipe: Scheduled Job

Schedule a job to execute at a future date/time.

```pascal
program ScheduledJob;

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
  TReportHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TReportHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'generate_report';
end;

procedure TReportHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Generating report at ', TimeToStr(Now), ': ', AJob.Body);
end;

var
  LStore: IHefestoStateStore;
  LScheduledStore: IHefestoScheduledStore;
  LServer: IHefestoServer;
  LEntry: THefestoScheduledEntry;
begin
  LStore := THefestoInMemoryStateStore.New;
  LScheduledStore := THefestoStateStoreScheduledStore.New(LStore);

  // Schedule 5 seconds from now
  LEntry := MakeScheduledEntry(
    'default',             // queue
    'generate_report',     // action
    '{"type": "monthly"}', // body
    [],                    // extra attributes
    Now + (5 / 86400)      // DueAt: Now + 5 seconds
  );
  LScheduledStore.Schedule(LEntry);
  Writeln('Job scheduled for: ', DateTimeToStr(LEntry.DueAt));

  LServer := THefestoServer.New
    .UseQueue(THefestoInMemoryQueueAdapter.New)
    .StateStore(LStore)
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('generate_report', TReportHandler.Create)
    .Run;

  Writeln('Waiting for the scheduled job to execute...');
  ReadLn;
  LServer.Stop;
end.
```

**Cancelling a scheduled job:**
```pascal
LScheduledStore.Delete('default', 'generate_report', LEntry.DueAt);
```

**Listing all scheduled jobs:**
```pascal
var LList := LScheduledStore.List;
for var E in LList do
  Writeln(Format('%s at %s', [E.Action, DateTimeToStr(E.DueAt)]));
```

See [scheduled-e-periodic.md](../04-features/scheduled-e-periodic.md) for periodic (cron) jobs.
