program ConcurrencyControl;

{$APPTYPE CONSOLE}

// Demo: Concurrency Control, Rate Limiting, Unique Jobs, Idempotency e Locking
//
// Mostra como controlar concorrencia global, por fila e por action,
// aplicar rate limiting, prevenir duplicatas e usar locks distribuidos.

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
  Hefesto.Locking,
  Hefesto.Idempotency,
  Hefesto.RateLimit,
  Hefesto.Unique,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TEmissaoHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TConsultaHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TEmissaoHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'emissao';
end;

procedure TEmissaoHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [emissao] Processando nota: %s (thread %d)',
    [AContext.Job.Body, TThread.CurrentThread.ThreadID]));
  Sleep(100); // Simula trabalho
end;

function TConsultaHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'consulta';
end;

procedure TConsultaHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [consulta] Consultando: %s (thread %d)',
    [AContext.Job.Body, TThread.CurrentThread.ThreadID]));
  Sleep(50);
end;

var
  Queue: THefestoInMemoryQueueAdapter;
  StateStore: IHefestoStateStore;
  Server: IHefestoServer;
  I: Integer;
  Attrs: TStringList;
begin
  try
    WriteLn('Hefesto - Demo Concurrency Control');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;
    StateStore := THefestoInMemoryStateStore.New;

    // --- Enfileirar jobs ---
    WriteLn('--- Enfileirando 10 emissoes + 5 consultas ---');
    for I := 1 to 10 do
      Queue.Enqueue('emissao', Format('{"nota":%d}', [I]));
    for I := 1 to 5 do
      Queue.Enqueue('consulta', Format('{"protocolo":%d}', [I]));

    // --- Enfileirar job com unique key (duplicata) ---
    Attrs := TStringList.Create;
    try
      Attrs.Values['unique_key'] := 'nota-123';
      Attrs.Values['unique_strategy'] := 'until_executed';
      Queue.EnqueueWithAttributes('emissao', '{"nota":123}', Attrs);
      Queue.EnqueueWithAttributes('emissao', '{"nota":123}', Attrs); // duplicata
    finally
      Attrs.Free;
    end;
    WriteLn('  + 2 emissoes com mesma unique_key (1 sera ignorada)');

    // --- Configurar servidor ---
    WriteLn('');
    WriteLn('--- Configuracao ---');
    WriteLn('  Concurrency global: 4');
    WriteLn('  Queue "nfse" max: 3 workers');
    WriteLn('  Action "emissao" max: 2 workers');
    WriteLn('  Rate limit: 5 ops/10s para emissao');
    WriteLn('');

    Server := THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(5)
      .IdleDelayMs(0)
      .StopWhenIdle

      // Concurrency control
      .Concurrency(4)                      // max 4 workers globais
      .QueueConcurrency('nfse', 3)         // max 3 na fila nfse
      .ActionConcurrency('emissao', 2)     // max 2 para emissao

      // State & providers
      .StateStore(StateStore)
      .LockProvider(THefestoInMemoryLockProvider.New(StateStore))
      .Idempotency(THefestoStateStoreIdempotency.New(StateStore))
      .RateLimiter(THefestoTokenBucketRateLimiter.New(StateStore))

      // Telemetry
      .Telemetry(THefestoConsoleTelemetry.New)

      // Handlers
      .RegisterHandler('emissao', TEmissaoHandler.Create)
      .RegisterHandler('consulta', TConsultaHandler.Create);

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
