program RedisIntegration;

{$APPTYPE CONSOLE}

// Demo: Integracao com Redis via Redis4D
//
// Mostra como usar Redis como backend para:
// - StateStore (locks, idempotency, leader election, rate limiting)
// - ScheduledStore (sorted set + Lua atomico)
// - Outbox distribuido (Redis list)
// - Locking distribuido
//
// Requer: Redis em localhost:6379

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Store.Redis4D,
  Hefesto.Redis.Client,
  Hefesto.Redis4D.Client,
  Hefesto.Locking,
  Hefesto.Locking.Redis4D,
  Hefesto.Idempotency,
  Hefesto.Scheduled,
  Hefesto.Scheduled.Redis4D,
  Hefesto.ClientReliability,
  Hefesto.ClientReliability.Redis4D,
  Hefesto.RateLimit,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TPaymentHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

function TPaymentHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'payment';
end;

procedure TPaymentHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [payment] Processando pagamento: %s', [AContext.Job.Body]));
  Sleep(100);
end;

var
  Queue: THefestoInMemoryQueueAdapter;
  StateStore: IHefestoStateStore;
  Server: IHefestoServer;
begin
  try
    WriteLn('Hefesto - Demo Redis Integration');
    WriteLn('Requer Redis em localhost:6379');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;

    // --- State Store Redis4D ---
    WriteLn('--- Configurando backends Redis4D ---');

    StateStore := THefestoRedis4DStateStore.New
      .RedisClient(TRedis4DClientBridge.NewFromConnectionString('redis://127.0.0.1:6379/0'));
    WriteLn('  StateStore: Redis4D (redis://127.0.0.1:6379/0)');

    // --- Servidor com todos os backends Redis ---
    Server := THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle

      // Backends Redis4D
      .StateStore(StateStore)
      .LockProvider(THefestoRedis4DLockProvider.New
        .ConnectionString('redis://127.0.0.1:6379/0'))
      .ScheduledStore(THefestoRedis4DScheduledStore.New
        .ConnectionString('redis://127.0.0.1:6379/0'))
      .ClientOutbox(THefestoRedis4DClientOutbox.New
        .ConnectionString('redis://127.0.0.1:6379/0'))
      .Idempotency(THefestoStateStoreIdempotency.New(StateStore))
      .RateLimiter(THefestoTokenBucketRateLimiter.New(StateStore))

      // Telemetry
      .Telemetry(THefestoConsoleTelemetry.New)

      // Handler
      .RegisterHandler('payment', TPaymentHandler.Create);

    // --- Enfileirar e processar ---
    WriteLn('');
    WriteLn('--- Enfileirando jobs ---');
    Queue.Enqueue('payment', '{"order_id":"ORD-001","amount":150.00}');
    Queue.Enqueue('payment', '{"order_id":"ORD-002","amount":299.90}');
    Queue.Enqueue('payment', '{"order_id":"ORD-003","amount":49.90}');

    // Job agendado para 5s no futuro via Redis scheduled store
    Server.EnqueueIn('payments', 'payment',
      '{"order_id":"ORD-004","amount":1000.00}', 5);
    WriteLn('  3 jobs imediatos + 1 agendado para 5s');

    WriteLn('');
    WriteLn('--- Processando ---');
    Server.Run;

    WriteLn('');
    WriteLn('Demo concluido.');
    WriteLn('(Dados ficaram no Redis para inspecao: redis-cli KEYS "sidekiq4d:*")');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
