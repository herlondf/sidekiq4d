program ReliabilityDemo;

{$APPTYPE CONSOLE}

// Demo: Client Reliability (Outbox) + Leader Election + Queue Pause/Resume
//
// Mostra:
// - Outbox para garantir que jobs nao se percam em caso de crash
// - Leader election para singleton operations (ex: periodic promotion)
// - Pause/resume de filas para controle operacional

uses
  System.SysUtils,
  System.Classes,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.ClientReliability,
  Sidekiq4D.Leader,
  Sidekiq4D.Locking,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TJobHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TJobHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TJobHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [handler] %s: %s', [AContext.Job.Action, AContext.Job.Body]));
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  StateStore: ISidekiqStateStore;
  Outbox: ISidekiqClientOutbox;
  Server: ISidekiqServer;
begin
  try
    WriteLn('Sidekiq4D - Demo Reliability & Leader Election');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;
    StateStore := TSidekiqInMemoryStateStore.New;

    // --- Client Outbox ---
    WriteLn('--- Client Outbox (crash recovery) ---');

    // Outbox em arquivo — sobrevive a crash do processo
    Outbox := TSidekiqFileClientOutbox.New('outbox_demo.json');

    // Simula publicacao via outbox (durable)
    Server := TSidekiqServer.New
      .UseQueue(Queue)
      .ClientOutbox(Outbox);
    Server.Enqueue('default', 'notificar', '{"evento":"pedido_criado"}');
    Server.Enqueue('default', 'notificar', '{"evento":"pagamento_confirmado"}');

    WriteLn(Format('  Outbox entries: %d', [Outbox.Count]));
    WriteLn('  (Em producao, o server faz flush automatico a cada ciclo)');
    WriteLn('');

    // --- Leader Election ---
    WriteLn('--- Leader Election ---');

    Server := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .MaxCycles(3)
      .StateStore(StateStore)
      .LockProvider(TSidekiqInMemoryLockProvider.New(StateStore))
      .ClientOutbox(Outbox)
      .UseLeaderElection(True)
      .LeaderName('worker-cluster-1')
      .LeaderLeaseTtlSeconds(30)
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('notificar', TJobHandler.Create);

    WriteLn(Format('  IsLeader: %s', [BoolToStr(Server.IsLeader, True)]));
    WriteLn('  (Apenas o leader executa periodic promotion e outbox flush)');
    WriteLn('');

    // --- Queue Pause/Resume ---
    WriteLn('--- Queue Pause/Resume ---');
    Queue.Enqueue('notificar', '{"msg":"antes do pause"}');
    Server.PauseQueue('default');
    WriteLn(Format('  Queue "default" pausada: %s',
      [BoolToStr(Server.QueuePaused('default'), True)]));

    // Jobs nao serao processados enquanto pausada
    WriteLn('  RunOnce enquanto pausada...');
    Server.RunOnce;

    Server.ResumeQueue('default');
    WriteLn('  Queue resumida.');
    WriteLn('');

    // --- Executar ---
    WriteLn('--- Processando (flush outbox + jobs) ---');
    Server.Run;

    // Cleanup
    Outbox.Clear;
    DeleteFile('outbox_demo.json');

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
