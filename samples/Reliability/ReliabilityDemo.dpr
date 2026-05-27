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
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Store.InMemory,
  Hefesto.ClientReliability,
  Hefesto.Leader,
  Hefesto.Locking,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TJobHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TJobHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TJobHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [handler] %s: %s', [AContext.Job.Action, AContext.Job.Body]));
end;

var
  Queue: THefestoInMemoryQueueAdapter;
  StateStore: IHefestoStateStore;
  Outbox: IHefestoClientOutbox;
  Server: IHefestoServer;
begin
  try
    WriteLn('Hefesto - Demo Reliability & Leader Election');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;
    StateStore := THefestoInMemoryStateStore.New;

    // --- Client Outbox ---
    WriteLn('--- Client Outbox (crash recovery) ---');

    // Outbox em arquivo — sobrevive a crash do processo
    Outbox := THefestoFileClientOutbox.New('outbox_demo.json');

    // Simula publicacao via outbox (durable)
    Server := THefestoServer.New
      .UseQueue(Queue)
      .ClientOutbox(Outbox);
    Server.Enqueue('default', 'notificar', '{"evento":"pedido_criado"}');
    Server.Enqueue('default', 'notificar', '{"evento":"pagamento_confirmado"}');

    WriteLn(Format('  Outbox entries: %d', [Outbox.Count]));
    WriteLn('  (Em producao, o server faz flush automatico a cada ciclo)');
    WriteLn('');

    // --- Leader Election ---
    WriteLn('--- Leader Election ---');

    Server := THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .MaxCycles(3)
      .StateStore(StateStore)
      .LockProvider(THefestoInMemoryLockProvider.New(StateStore))
      .ClientOutbox(Outbox)
      .UseLeaderElection(True)
      .LeaderName('worker-cluster-1')
      .LeaderLeaseTtlSeconds(30)
      .Telemetry(THefestoConsoleTelemetry.New)
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
