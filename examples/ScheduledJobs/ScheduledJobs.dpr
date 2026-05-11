program ScheduledJobs;

{$APPTYPE CONSOLE}

// Demo: Scheduled Jobs, Periodic Jobs e EnqueueIn/EnqueueAt
//
// Mostra como agendar jobs para execucao futura e registrar jobs periodicos (cron).

uses
  System.SysUtils,
  System.DateUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Periodic,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TReportHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TCleanupHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TReportHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'report';
end;

procedure TReportHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [%s] Gerando relatorio: %s',
    [FormatDateTime('hh:nn:ss', Now), AContext.Job.Body]));
end;

function TCleanupHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'cleanup';
end;

procedure TCleanupHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [%s] Executando limpeza periodica',
    [FormatDateTime('hh:nn:ss', Now)]));
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  Server: ISidekiqServer;
begin
  try
    WriteLn('Sidekiq4D - Demo Scheduled & Periodic Jobs');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    Server := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(100)
      .MaxCycles(10)
      .ScheduledStore(TSidekiqInMemoryScheduledStore.New)
      .Telemetry(TSidekiqConsoleTelemetry.New)
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
