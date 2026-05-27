# Recipe: Job Graph (DAG)

ETL pipeline with declared dependencies and parallel execution where possible.

```pascal
program JobGraphDAG;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Graph,
  Hefesto.Telemetry.Console;

// --- Handlers ---

type
  TExtractHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TTransformHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TLoadHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TExtractHandler.CanHandle(const AAction: string): Boolean;
begin Result := AAction = 'extract'; end;

procedure TExtractHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('[extract] Extracting data...');
  Sleep(200);
end;

function TTransformHandler.CanHandle(const AAction: string): Boolean;
begin Result := AAction = 'transform'; end;

procedure TTransformHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('[transform] Transforming data...');
  Sleep(300);
end;

function TLoadHandler.CanHandle(const AAction: string): Boolean;
begin Result := AAction = 'load'; end;

procedure TLoadHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('[load] Loading to destination...');
  Sleep(150);
end;

// --- Main ---

var
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(THefestoInMemoryStateStore.New)
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('extract',   TExtractHandler.Create)
    .RegisterHandler('transform', TTransformHandler.Create)
    .RegisterHandler('load',      TLoadHandler.Create)
    .Run;

  // Define and execute the graph
  // extract → transform → load (sequential)
  THefestoJobGraph.New
    .Node('extract',   TExtractHandler.Create)
    .Node('transform', TTransformHandler.Create).DependsOn('extract')
    .Node('load',      TLoadHandler.Create).DependsOn('transform')
    .Execute(LQueue, LServer);

  Writeln('Graph started. Waiting for completion...');
  ReadLn;
  LServer.Stop;
end.
```

**Graph with parallel branches:**

```pascal
// ingest → [normalize_a, normalize_b] → merge → export
THefestoJobGraph.New
  .Node('ingest',      TIngestHandler.Create)
  .Node('normalize_a', TNormalizeAHandler.Create).DependsOn('ingest')
  .Node('normalize_b', TNormalizeBHandler.Create).DependsOn('ingest')
  .Node('merge',       TMergeHandler.Create)
    .DependsOn('normalize_a')
    .DependsOn('normalize_b')
  .Node('export',      TExportHandler.Create).DependsOn('merge')
  .Parallel   // normalize_a and normalize_b execute in parallel
  .Execute(LQueue, LServer);
```

See [job-graph.md](../04-features/job-graph.md) for complete API reference.
