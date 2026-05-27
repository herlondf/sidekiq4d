# Recipe: Outbox Pattern

Publish messages reliably alongside a database operation.

```pascal
program OutboxPattern;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Outbox,
  Hefesto.Telemetry.Console;

type
  TRelayHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FOutbox: IHefestoClientOutbox;
    FQueue: IHefestoQueueAdapter;
  public
    constructor Create(
      const AOutbox: IHefestoClientOutbox;
      const AQueue: IHefestoQueueAdapter);
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TProcessOrderHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

constructor TRelayHandler.Create(
  const AOutbox: IHefestoClientOutbox;
  const AQueue: IHefestoQueueAdapter);
begin
  inherited Create;
  FOutbox := AOutbox;
  FQueue := AQueue;
end;

function TRelayHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'relay_outbox';
end;

procedure TRelayHandler.Execute(const AJob: IHefestoJobEnvelope);
var
  LEntries: TArray<THefestoOutboxEntry>;
  LEntry: THefestoOutboxEntry;
begin
  LEntries := FOutbox.Entries;
  Writeln(Format('[relay] %d pending entries in outbox', [Length(LEntries)]));

  for LEntry in LEntries do
  begin
    try
      FQueue.Enqueue(LEntry.Request.Action, LEntry.Request.Body);
      FOutbox.Remove(LEntry.Id);
      Writeln('[relay] Published: ', LEntry.Request.Action);
    except on E: Exception do
      Writeln('[relay] Failed to publish: ', E.Message);
      // keep in outbox for next execution
    end;
  end;
end;

function TProcessOrderHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process_order';
end;

procedure TProcessOrderHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processing order: ', AJob.Body);
end;

// --- Transactional write simulation ---

procedure CreateOrderWithOutbox(
  const AOutbox: IHefestoClientOutbox;
  const AOrderId: Integer);
var
  LRequest: THefestoPublishRequest;
begin
  // In production: inside a database transaction

  // 1. Write order to database (simulated)
  Writeln(Format('Writing order %d to database...', [AOrderId]));

  // 2. Write message to outbox (same transaction)
  LRequest.Queue := 'default';
  LRequest.Action := 'process_order';
  LRequest.Body := Format('{"order_id": %d}', [AOrderId]);
  AOutbox.Save(LRequest);

  Writeln(Format('Message saved to outbox. Total: %d', [AOutbox.Count]));
  // Transaction commits here — database and outbox are consistent
end;

var
  LStore: IHefestoStateStore;
  LOutbox: IHefestoClientOutbox;
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LOutbox := THefestoStateStoreOutbox.New(LStore);
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(2)
    .StateStore(LStore)
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('relay_outbox',  TRelayHandler.Create(LOutbox, LQueue))
    .RegisterHandler('process_order', TProcessOrderHandler.Create)
    .Run;

  // Simulate order creation
  CreateOrderWithOutbox(LOutbox, 1001);
  CreateOrderWithOutbox(LOutbox, 1002);
  CreateOrderWithOutbox(LOutbox, 1003);

  // Trigger relay manually (in production: periodic job)
  LQueue.Enqueue('relay_outbox', '{}');

  ReadLn;
  LServer.Stop;
end.
```

**Relay as a periodic job (production):**
```pascal
// Register relay to run every 30 seconds
THefestoPeriodicJob.Register(
  'relay_outbox', '*/1 * * * *',  // every minute
  'default', '{}', LScheduledStore
);
```

See [outbox.md](../04-features/outbox.md) for interface details.
