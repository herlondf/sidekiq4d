program SQLiteDemo;

{$APPTYPE CONSOLE}

// Demo: State Store com SQLite via FireDAC
//
// Mostra como usar SQLite como backend persistente para o Sidekiq4D.
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
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.Postgres,
  Sidekiq4D.Store.FireDAC,
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TOrderHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TOrderHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'process_order';
end;

procedure TOrderHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [order] Processando pedido: %s', [AContext.Job.Body]));
end;

const
  DB_FILE = 'sidekiq4d_demo.db';

var
  Connection: TFDConnection;
  StateStore: ISidekiqStateStore;
  Queue: TSidekiqInMemoryQueueAdapter;
  Server: ISidekiqServer;
begin
  try
    WriteLn('Sidekiq4D - Demo SQLite (FireDAC)');
    WriteLn(Format('Banco: %s', [DB_FILE]));
    WriteLn('');

    // --- Criar conexao SQLite ---
    Connection := TSidekiqFireDACBackend.NewSQLiteConnection(DB_FILE);
    try
      // --- State Store com backend SQLite ---
      StateStore := TSidekiqPostgresStateStore.New
        .Backend(TSidekiqFireDACBackend.New(Connection))
        .TableName('sidekiq_state');

      WriteLn('--- Configuracao ---');
      WriteLn('  Backend: SQLite (arquivo local)');
      WriteLn('  Tabela: sidekiq_state');
      WriteLn('  Locks, idempotency e scheduled persistem entre reinicializacoes');
      WriteLn('');

      Queue := TSidekiqInMemoryQueueAdapter.New;

      Server := TSidekiqServer.New
        .UseQueue(Queue)
        .BatchSize(10)
        .IdleDelayMs(0)
        .StopWhenIdle
        .StateStore(StateStore)
        .LockProvider(TSidekiqInMemoryLockProvider.New(StateStore))
        .Idempotency(TSidekiqStateStoreIdempotency.New(StateStore))
        .ScheduledStore(TSidekiqInMemoryScheduledStore.New)
        .Telemetry(TSidekiqConsoleTelemetry.New)
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
