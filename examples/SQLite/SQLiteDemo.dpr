program SQLiteDemo;

{$APPTYPE CONSOLE}

// Demo: State Store com SQLite via FireDAC
//
// Mostra como usar SQLite como backend persistente para o Hefesto.
// Nao requer servidor externo — o banco e um arquivo local.
// Ideal para cenarios single-process, Windows Service ou aplicacoes desktop.

uses
  System.SysUtils,
  FireDAC.Comp.Client,
  FireDAC.Stan.Def,
  FireDAC.Stan.Async,
  FireDAC.DApt,
  FireDAC.Phys.SQLite,
  FireDAC.Phys.SQLiteDef,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Store.Postgres,
  Hefesto.Store.FireDAC,
  Hefesto.Locking,
  Hefesto.Idempotency,
  Hefesto.Scheduled,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TOrderHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TOrderHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'process_order';
end;

procedure TOrderHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [order] Processando pedido: %s', [AContext.Job.Body]));
end;

const
  DB_FILE = 'sidekiq4d_demo.db';

var
  Connection: TFDConnection;
  StateStore: IHefestoStateStore;
  Queue: THefestoInMemoryQueueAdapter;
  Server: IHefestoServer;
begin
  try
    WriteLn('Hefesto - Demo SQLite (FireDAC)');
    WriteLn(Format('Banco: %s', [DB_FILE]));
    WriteLn('');

    // --- Criar conexao SQLite ---
    Connection := THefestoFireDACBackend.NewSQLiteConnection(DB_FILE);
    try
      // --- State Store com backend SQLite ---
      StateStore := THefestoPostgresStateStore.New
        .Backend(THefestoFireDACBackend.New(Connection))
        .TableName('sidekiq_state');

      WriteLn('--- Configuracao ---');
      WriteLn('  Backend: SQLite (arquivo local)');
      WriteLn('  Tabela: sidekiq_state');
      WriteLn('  Locks, idempotency e scheduled persistem entre reinicializacoes');
      WriteLn('');

      Queue := THefestoInMemoryQueueAdapter.New;

      Server := THefestoServer.New
        .UseQueue(Queue)
        .BatchSize(10)
        .IdleDelayMs(0)
        .StopWhenIdle
        .StateStore(StateStore)
        .LockProvider(THefestoInMemoryLockProvider.New(StateStore))
        .Idempotency(THefestoStateStoreIdempotency.New(StateStore))
        .ScheduledStore(THefestoInMemoryScheduledStore.New)
        .Telemetry(THefestoConsoleTelemetry.New)
        .RegisterHandler('process_order', TOrderHandler.Create);

      // --- Enfileirar e processar ---
      WriteLn('--- Processando pedidos ---');
      Queue.Enqueue('process_order', '{"id":"ORD-001","total":150.00}');
      Queue.Enqueue('process_order', '{"id":"ORD-002","total":299.90}');
      Queue.Enqueue('process_order', '{"id":"ORD-003","total":49.90}');

      // Job agendado para 2s
      Server.EnqueueIn('default', 'process_order',
        '{"id":"ORD-004","total":1000.00}', 2);

      Server.Run;

      // --- Demonstrar persistencia ---
      WriteLn('');
      WriteLn('--- Verificando persistencia ---');
      StateStore.Put('demo:counter', '42', 0);
      WriteLn('  Put demo:counter = 42');
      WriteLn(Format('  Get demo:counter = %s', [StateStore.Get('demo:counter')]));
      StateStore.Delete('demo:counter');

      WriteLn('');
      WriteLn('Demo concluido.');
      WriteLn(Format('(Banco SQLite persistido em: %s)', [ExpandFileName(DB_FILE)]));
    finally
      Connection.Free;
    end;

    // Cleanup demo file
    DeleteFile(DB_FILE);
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
