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
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.Redis4D,
  Sidekiq4D.Redis.Client,
  Sidekiq4D.Redis4D.Client,
  Sidekiq4D.Locking,
  Sidekiq4D.Locking.Redis4D,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Scheduled.Redis4D,
  Sidekiq4D.ClientReliability,
  Sidekiq4D.ClientReliability.Redis4D,
  Sidekiq4D.RateLimit,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TPaymentHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function TPaymentHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'payment';
end;

procedure TPaymentHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [payment] Processando pagamento: %s', [AContext.Job.Body]));
  Sleep(100);
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  StateStore: ISidekiqStateStore;
  Server: ISidekiqServer;
begin
  try
    WriteLn('Sidekiq4D - Demo Redis Integration');
    WriteLn('Requer Redis em localhost:6379');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    // --- State Store Redis4D ---
    WriteLn('--- Configurando backends Redis4D ---');

    StateStore := TSidekiqRedis4DStateStore.New
      .RedisClient(TRedis4DClientBridge.NewFromConnectionString('redis://127.0.0.1:6379/0'));
    WriteLn('  StateStore: Redis4D (redis://127.0.0.1:6379/0)');

    // --- Servidor com todos os backends Redis ---
    Server := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle

      // Backends Redis4D
      .StateStore(StateStore)
      .LockProvider(TSidekiqRedis4DLockProvider.New
        .ConnectionString('redis://127.0.0.1:6379/0'))
      .ScheduledStore(TSidekiqRedis4DScheduledStore.New
        .ConnectionString('redis://127.0.0.1:6379/0'))
      .ClientOutbox(TSidekiqRedis4DClientOutbox.New
        .ConnectionString('redis://127.0.0.1:6379/0'))
      .Idempotency(TSidekiqStateStoreIdempotency.New(StateStore))
      .RateLimiter(TSidekiqTokenBucketRateLimiter.New(StateStore))

      // Telemetry
      .Telemetry(TSidekiqConsoleTelemetry.New)

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
