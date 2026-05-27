# Batch Jobs

Processing multiple jobs as a logical unit, with callbacks executed when the batch completes or succeeds.

## Creating a batch

```pascal
uses
  Hefesto.Batch,
  Hefesto.Store.InMemory;

var
  LStore: IHefestoStateStore;
  LBatchStore: IHefestoBatchStore;
begin
  LStore := THefestoInMemoryStateStore.New;
  LBatchStore := THefestoStateStoreBatchStore.New(LStore);

  THefestoBatch.New(LBatchStore)
    .OnComplete(procedure
      begin
        Writeln('Batch complete (success or failure)');
      end)
    .OnSuccess(procedure
      begin
        Writeln('All batch jobs succeeded');
      end)
    .Add('process_item', '{"id": 1}')
    .Add('process_item', '{"id": 2}')
    .Add('process_item', '{"id": 3}')
    .Commit;
end;
```

## Callback semantics

| Callback | When it fires |
|----------|--------------|
| `OnComplete` | When all jobs finish (any status) |
| `OnSuccess` | Only when all jobs finish without failure |

- If any job fails and is moved to DLQ, `OnSuccess` does not fire
- `OnComplete` always fires at the end of the batch, regardless of failures
- Callbacks are executed on the thread of the last worker to complete

## Adding jobs dynamically

```pascal
var
  LBatch: IHefestoBatch;
begin
  LBatch := THefestoBatch.New(LBatchStore)
    .OnComplete(procedure begin ... end);

  // Add jobs conditionally
  for var Item in FItems do
    if Item.NeedsProcessing then
      LBatch.Add('process_item', TJson.Serialize(Item));

  LBatch.Commit;
end;
```

`Commit` finalizes the batch and releases the jobs for execution. Do not add jobs after `Commit`.

## Tracking progress

The batch store maintains counters of pending, completed, and failed jobs. To inspect:

```pascal
var LStatus := LBatchStore.GetStatus(BatchId);
Writeln(Format('Pending: %d, OK: %d, Failed: %d',
  [LStatus.Pending, LStatus.Succeeded, LStatus.Failed]));
```

## Batch with external StateStore (Redis)

For batches that survive process restarts:

```pascal
LStore := THefestoRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');
LBatchStore := THefestoStateStoreBatchStore.New(LStore);
```

See the complete recipe in [06-recipes/batch-com-callback.md](../06-recipes/batch-com-callback.md).
