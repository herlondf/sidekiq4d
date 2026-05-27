# Recipe: Batch with Callback

Process multiple jobs as a batch with a notification on completion.

```pascal
program BatchWithCallback;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Batch,
  Hefesto.Telemetry.Console;

type
  TProcessItemHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TProcessItemHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process_item';
end;

procedure TProcessItemHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processing item: ', AJob.Body);
  Sleep(100); // simulate work
end;

var
  LStore: IHefestoStateStore;
  LBatchStore: IHefestoBatchStore;
  LServer: IHefestoServer;
  LBatch: IHefestoBatch;
  I: Integer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LBatchStore := THefestoStateStoreBatchStore.New(LStore);

  LServer := THefestoServer.New
    .UseQueue(THefestoInMemoryQueueAdapter.New)
    .Concurrency(4)
    .StateStore(LStore)
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('process_item', TProcessItemHandler.Create)
    .Run;

  // Create and commit the batch
  LBatch := THefestoBatch.New(LBatchStore)
    .OnComplete(procedure
      begin
        Writeln('--- Batch complete (success or failure) ---');
      end)
    .OnSuccess(procedure
      begin
        Writeln('--- All items processed successfully! ---');
      end);

  for I := 1 to 10 do
    LBatch.Add('process_item', Format('{"id": %d}', [I]));

  LBatch.Commit;
  Writeln('Batch of 10 items committed.');

  Writeln('Waiting for completion... (Enter to stop)');
  ReadLn;
  LServer.Stop;
end.
```

**Batch with dynamic data:**
```pascal
LBatch := THefestoBatch.New(LBatchStore)
  .OnSuccess(procedure begin NotifySystem end);

for var Order in FPendingOrders do
  if Order.NeedsProcessing then
    LBatch.Add('process_order', Order.ToJSON);

if LBatch.Count > 0 then
  LBatch.Commit
else
  Writeln('No pending orders.');
```

See [batch-jobs.md](../04-features/batch-jobs.md) for detailed callback semantics.
