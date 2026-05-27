program PostgresDemo;

{$APPTYPE CONSOLE}

// Demo: State Store com PostgreSQL via FireDAC
//
// Mostra como usar Postgres como backend persistente para o Hefesto.
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
  Hefesto.Leader,
  Hefesto.Scheduled,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TInvoiceHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TInvoiceHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'emit_invoice';
end;

procedure TInvoiceHandler.Perform(const AContext: IHefestoJobContext);
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
  StateStore: IHefestoStateStore;
  Queue: THefestoInMemoryQueueAdapter;
  Server: IHefestoServer;
begin
  try
    WriteLn('Hefesto - Demo PostgreSQL (FireDAC)');
    WriteLn(Format('Conectando em %s:%d/%s...', [PG_HOST, PG_PORT, PG_DB]));
    WriteLn('');

    // --- Criar conexao Postgres ---
    Connection := THefestoFireDACBackend.NewPostgresConnection(
      PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS);
    try
      // --- State Store com backend Postgres ---
      StateStore := THefestoPostgresStateStore.New
        .Backend(THefestoFireDACBackend.New(Connection))
        .TableName('sidekiq_state');

      WriteLn('--- Configuracao ---');
      WriteLn(Format('  Backend: PostgreSQL (%s:%d/%s)', [PG_HOST, PG_PORT, PG_DB]));
      WriteLn('  Tabela: sidekiq_state (criada automaticamente)');
      WriteLn('  Estado distribuido: locks, idempotency, leader election');
      WriteLn('');

      Queue := THefestoInMemoryQueueAdapter.New;

      Server := THefestoServer.New
        .UseQueue(Queue)
        .BatchSize(10)
        .IdleDelayMs(0)
        .StopWhenIdle

        // Providers com Postgres
        .StateStore(StateStore)
        .LockProvider(THefestoInMemoryLockProvider.New(StateStore))
        .Idempotency(THefestoStateStoreIdempotency.New(StateStore))

        // Leader election (funciona com multiplos processos apontando pro mesmo Postgres)
        .UseLeaderElection(True)
        .LeaderName('invoice-workers')
        .LeaderLeaseTtlSeconds(30)

        .Telemetry(THefestoConsoleTelemetry.New)
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
