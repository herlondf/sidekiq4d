program PostgresDemo;

{$APPTYPE CONSOLE}

// Demo: State Store com PostgreSQL via FireDAC
//
// Mostra como usar Postgres como backend persistente para o Sidekiq4D.
// Ideal para cenarios multi-processo/multi-servidor onde o estado precisa
// ser compartilhado (locks, idempotency, leader election).
//
// Requer: PostgreSQL acessivel (ex: Docker na WSL)
//   docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16

uses
  System.SysUtils,
  FireDAC.Comp.Client,
  FireDAC.Stan.Def,
  FireDAC.Stan.Async,
  FireDAC.DApt,
  FireDAC.Phys.PG,
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
  Sidekiq4D.Leader,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TInvoiceHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TInvoiceHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'emit_invoice';
end;

procedure TInvoiceHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [invoice] Emitindo NF: %s', [AContext.Job.Body]));
  Sleep(100); // Simula chamada a SEFAZ
end;

const
  PG_HOST = '127.0.0.1';
  PG_PORT = 5433;
  PG_DB   = 'postgres';
  PG_USER = 'postgres';
  PG_PASS = 'postgres';

var
  Connection: TFDConnection;
  StateStore: ISidekiqStateStore;
  Queue: TSidekiqInMemoryQueueAdapter;
  Server: ISidekiqServer;
begin
  try
    WriteLn('Sidekiq4D - Demo PostgreSQL (FireDAC)');
    WriteLn(Format('Conectando em %s:%d/%s...', [PG_HOST, PG_PORT, PG_DB]));
    WriteLn('');

    // --- Criar conexao Postgres ---
    Connection := TSidekiqFireDACBackend.NewPostgresConnection(
      PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS);
    try
      // --- State Store com backend Postgres ---
      StateStore := TSidekiqPostgresStateStore.New
        .Backend(TSidekiqFireDACBackend.New(Connection))
        .TableName('sidekiq_state');

      WriteLn('--- Configuracao ---');
      WriteLn(Format('  Backend: PostgreSQL (%s:%d/%s)', [PG_HOST, PG_PORT, PG_DB]));
      WriteLn('  Tabela: sidekiq_state (criada automaticamente)');
      WriteLn('  Estado distribuido: locks, idempotency, leader election');
      WriteLn('');

      Queue := TSidekiqInMemoryQueueAdapter.New;

      Server := TSidekiqServer.New
        .UseQueue(Queue)
        .BatchSize(10)
        .IdleDelayMs(0)
        .StopWhenIdle

        // Providers com Postgres
        .StateStore(StateStore)
        .LockProvider(TSidekiqInMemoryLockProvider.New(StateStore))
        .Idempotency(TSidekiqStateStoreIdempotency.New(StateStore))

        // Leader election (funciona com multiplos processos apontando pro mesmo Postgres)
        .UseLeaderElection(True)
        .LeaderName('invoice-workers')
        .LeaderLeaseTtlSeconds(30)

        .Telemetry(TSidekiqConsoleTelemetry.New)
        .RegisterHandler('emit_invoice', TInvoiceHandler.Create);

      WriteLn(Format('  IsLeader: %s', [BoolToStr(Server.IsLeader, True)]));
      WriteLn('');

      // --- Enfileirar e processar ---
      WriteLn('--- Processando notas ---');
      Queue.Enqueue('emit_invoice', '{"empresa":"ACME","serie":1,"numero":1001}');
      Queue.Enqueue('emit_invoice', '{"empresa":"ACME","serie":1,"numero":1002}');
      Queue.Enqueue('emit_invoice', '{"empresa":"ACME","serie":1,"numero":1003}');

      Server.Run;

      WriteLn('');
      WriteLn('Demo concluido.');
      WriteLn('(Tabela sidekiq_state criada no Postgres para inspecao)');
    finally
      Connection.Free;
    end;
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
