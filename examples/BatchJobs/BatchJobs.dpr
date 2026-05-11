program BatchJobs;

{$APPTYPE CONSOLE}

// Demo: Batch Jobs com callbacks de conclusao
//
// Mostra como agrupar jobs em um batch e receber notificacao quando todos
// completam (OnComplete) ou quando todos tem sucesso (OnSuccess).

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Batch,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TImportHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TNotifyHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TImportHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'import';
end;

procedure TImportHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  Importando: %s', [AContext.Job.Body]));
end;

function TNotifyHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'notify';
end;

procedure TNotifyHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  >>> CALLBACK: Batch concluido! Enviando notificacao: %s',
    [AContext.Job.Body]));
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  Server: ISidekiqServer;
  Batch: ISidekiqBatch;
begin
  try
    WriteLn('Sidekiq4D - Demo Batch Jobs');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    Server := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .StateStore(TSidekiqInMemoryStateStore.New)
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('import', TImportHandler.Create)
      .RegisterHandler('notify', TNotifyHandler.Create);

    // --- Criar batch ---
    WriteLn('--- Criando batch de importacao ---');

    Batch := Server.Batch('importacao-lote-42');

    // Adiciona 3 jobs ao batch
    Batch
      .Enqueue('default', 'import', '{"arquivo":"clientes.csv"}')
      .Enqueue('default', 'import', '{"arquivo":"produtos.csv"}')
      .Enqueue('default', 'import', '{"arquivo":"pedidos.csv"}');

    // Registra callback de conclusao
    Batch.OnComplete('default', 'notify',
      '{"msg":"Importacao do lote 42 finalizada"}');

    // Registra callback de sucesso total
    Batch.OnSuccess('default', 'notify',
      '{"msg":"Todos os arquivos importados sem erro"}');

    WriteLn('  Batch criado com 3 jobs');
    WriteLn('  Callbacks: OnComplete + OnSuccess');
    WriteLn('');

    // --- Executar ---
    WriteLn('--- Processando ---');
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
