program ConcurrencyControl;

{$APPTYPE CONSOLE}

// Demo: Concurrency Control, Rate Limiting, Unique Jobs, Idempotency e Locking
//
// Mostra como controlar concorrencia global, por fila e por action,
// aplicar rate limiting, prevenir duplicatas e usar locks distribuidos.

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
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency,
  Sidekiq4D.RateLimit,
  Sidekiq4D.Unique,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TEmissaoHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TConsultaHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TEmissaoHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'emissao';
end;

procedure TEmissaoHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [emissao] Processando nota: %s (thread %d)',
    [AContext.Job.Body, TThread.CurrentThread.ThreadID]));
  Sleep(100); // Simula trabalho
end;

function TConsultaHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'consulta';
end;

procedure TConsultaHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [consulta] Consultando: %s (thread %d)',
    [AContext.Job.Body, TThread.CurrentThread.ThreadID]));
  Sleep(50);
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  StateStore: ISidekiqStateStore;
  Server: ISidekiqServer;
  I: Integer;
  Attrs: TStringList;
begin
  try
    WriteLn('Sidekiq4D - Demo Concurrency Control');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;
    StateStore := TSidekiqInMemoryStateStore.New;

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

    Server := TSidekiqServer.New
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
      .LockProvider(TSidekiqInMemoryLockProvider.New(StateStore))
      .Idempotency(TSidekiqStateStoreIdempotency.New(StateStore))
      .RateLimiter(TSidekiqTokenBucketRateLimiter.New(StateStore))

      // Telemetry
      .Telemetry(TSidekiqConsoleTelemetry.New)

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
