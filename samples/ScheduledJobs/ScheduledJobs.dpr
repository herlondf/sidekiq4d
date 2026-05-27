program ScheduledJobs;

{$APPTYPE CONSOLE}

// Demo: Scheduled Jobs, Periodic Jobs e EnqueueIn/EnqueueAt
//
// Mostra como agendar jobs para execucao futura e registrar jobs periodicos (cron).

uses
  System.SysUtils,
  System.DateUtils,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Scheduled,
  Hefesto.Periodic,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TReportHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TCleanupHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TReportHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'report';
end;

procedure TReportHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [%s] Gerando relatorio: %s',
    [FormatDateTime('hh:nn:ss', Now), AContext.Job.Body]));
end;

function TCleanupHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'cleanup';
end;

procedure TCleanupHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [%s] Executando limpeza periodica',
    [FormatDateTime('hh:nn:ss', Now)]));
end;

var
  Queue: THefestoInMemoryQueueAdapter;
  Server: IHefestoServer;
begin
  try
    WriteLn('Hefesto - Demo Scheduled & Periodic Jobs');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;

    Server := THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(100)
      .MaxCycles(10)
      .ScheduledStore(THefestoInMemoryScheduledStore.New)
      .Telemetry(THefestoConsoleTelemetry.New)
      .RegisterHandler('report', TReportHandler.Create)
      .RegisterHandler('cleanup', TCleanupHandler.Create);

    // --- Scheduled Jobs (EnqueueIn / EnqueueAt) ---
    WriteLn('--- Agendando jobs ---');

    // Job que executa em 2 segundos
    Server.EnqueueIn('default', 'report', '{"tipo":"vendas"}', 2);
    WriteLn('  EnqueueIn: report em 2s');

    // Job que executa em momento especifico
    Server.EnqueueAt('default', 'report', '{"tipo":"estoque"}',
      IncSecond(Now, 3));
    WriteLn('  EnqueueAt: report em 3s');

    // --- Periodic Jobs (Cron) ---
    WriteLn('');
    WriteLn('--- Registrando periodic jobs ---');

    // Limpeza a cada minuto (cron: * * * * *)
    Server.RegisterPeriodic('limpeza-diaria', '* * * * *',
      'default', 'cleanup', '{}');
    WriteLn('  Periodic: cleanup a cada minuto');

    // --- Executar ---
    WriteLn('');
    WriteLn('--- Executando (10 ciclos) ---');

    // Promove scheduled jobs (simula passagem de tempo)
    Server.PromoteScheduled;
    Server.PromotePeriodic;

    Server.Run;

    WriteLn('');
    WriteLn('Demo concluido.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
