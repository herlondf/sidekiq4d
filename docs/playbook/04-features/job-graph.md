# Job Graph (DAG)

Executes jobs with declared dependencies, forming a directed acyclic graph (DAG). Nodes with no pending dependencies execute in parallel.

## Concept

```
extract ──► transform ──► load
```

In this example, `transform` only starts after `extract` completes, and `load` only after `transform`. With `.Parallel`, independent nodes execute at the same time.

## API

```pascal
uses
  Hefesto.Graph;

THefestoJobGraph.New
  .Node('extract',   TExtractJob.Create)
  .Node('transform', TTransformJob.Create).DependsOn('extract')
  .Node('load',      TLoadJob.Create).DependsOn('transform')
  .Parallel                    // nodes with no pending dependencies run in parallel
  .Execute(LQueue, LServer);
```

## Example with multiple parallel branches

```
          ┌─► normalize_a ─┐
ingest ───┤                ├──► merge ──► export
          └─► normalize_b ─┘
```

```pascal
THefestoJobGraph.New
  .Node('ingest',      TIngestJob.Create)
  .Node('normalize_a', TNormalizeAJob.Create).DependsOn('ingest')
  .Node('normalize_b', TNormalizeBJob.Create).DependsOn('ingest')
  .Node('merge',       TMergeJob.Create)
    .DependsOn('normalize_a')
    .DependsOn('normalize_b')
  .Node('export',      TExportJob.Create).DependsOn('merge')
  .Parallel
  .Execute(LQueue, LServer);
```

`normalize_a` and `normalize_b` execute in parallel after `ingest`. `merge` waits for both.

## Method reference

| Method | Description |
|--------|-------------|
| `.Node(name, handler)` | Declares a graph node |
| `.DependsOn(nodeName)` | Adds a dependency to the last declared node |
| `.Parallel` | Enables parallel execution of independent nodes |
| `.Execute(queue, server)` | Starts execution of the graph |

## Node handlers

Each node uses a normal `IHefestoJobHandler`. The graph manages orchestration:

```pascal
TExtractJob = class(TInterfacedObject, IHefestoJobHandler)
public
  function CanHandle(const AAction: string): Boolean;
  procedure Execute(const AJob: IHefestoJobEnvelope);
end;

function TExtractJob.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'extract';
end;

procedure TExtractJob.Execute(const AJob: IHefestoJobEnvelope);
begin
  // extract data and can write results for use in subsequent nodes
  // use StateStore to pass data between nodes if needed
end;
```

## Passing data between nodes

The graph does not pass data between nodes automatically. Recommended patterns:

1. **Shared StateStore:** each node reads/writes from the store with keys combining the graph ID
2. **Database:** nodes write results to tables; subsequent nodes query them
3. **Temporary files:** for large data volumes

## Without `.Parallel`

Without `.Parallel`, the graph executes nodes sequentially in declaration order. Dependencies are still respected, but there is no parallelism even when the graph allows it.

See recipe in [06-recipes/job-graph-dag.md](../06-recipes/job-graph-dag.md).
