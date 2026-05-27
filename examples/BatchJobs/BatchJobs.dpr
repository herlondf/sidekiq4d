program BatchJobs;

{$APPTYPE CONSOLE}

// Demo: Batch Jobs com callbacks de conclusao
//
// Mostra como agrupar jobs em um batch e receber notificacao quando todos
// completam (OnComplete) ou quando todos tem sucesso (OnSuccess).

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Batch,
  Hefesto.Store.Interfaces,
  Hefesto.Store.InMemory,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TImportHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TNotifyHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TImportHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'import';
end;

procedure TImportHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  Importando: %s', [AContext.Job.Body]));
end;

function TNotifyHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'notify';
end;

procedure TNotifyHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  >>> CALLBACK: Batch concluido! Enviando notificacao: %s',
    [AContext.Job.Body]));
end;

var
  Queue: THefestoInMemoryQueueAdapter;
  Server: IHefestoServer;
  Batch: IHefestoBatch;
begin
  try
    WriteLn('Hefesto - Demo Batch Jobs');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;

    Server := THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .StateStore(THefestoInMemoryStateStore.New)
      .Telemetry(THefestoConsoleTelemetry.New)
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
