# Receita: Job Graph (DAG)

Pipeline ETL com dependências declaradas e execução paralela onde possível.

```pascal
program JobGraphDAG;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Graph,
  Sidekiq4D.Telemetry.Console;

// --- Handlers ---

type
  TExtractHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

  TTransformHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

  TLoadHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TExtractHandler.CanHandle(const AAction: string): Boolean;
begin Result := AAction = 'extract'; end;

procedure TExtractHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('[extract] Extraindo dados...');
  Sleep(200);
end;

function TTransformHandler.CanHandle(const AAction: string): Boolean;
begin Result := AAction = 'transform'; end;

procedure TTransformHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('[transform] Transformando dados...');
  Sleep(300);
end;

function TLoadHandler.CanHandle(const AAction: string): Boolean;
begin Result := AAction = 'load'; end;

procedure TLoadHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('[load] Carregando no destino...');
  Sleep(150);
end;

// --- Main ---

var
  LQueue: ISidekiqQueueAdapter;
  LServer: ISidekiqServer;
begin
  LQueue := TSidekiqInMemoryQueueAdapter.New;

  LServer := TSidekiqServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(TSidekiqInMemoryStateStore.New)
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('extract',   TExtractHandler.Create)
    .RegisterHandler('transform', TTransformHandler.Create)
    .RegisterHandler('load',      TLoadHandler.Create)
    .Run;

  // Definir e executar o grafo
  // extract → transform → load (sequencial)
  TSidekiqJobGraph.New
    .Node('extract',   TExtractHandler.Create)
    .Node('transform', TTransformHandler.Create).DependsOn('extract')
    .Node('load',      TLoadHandler.Create).DependsOn('transform')
    .Execute(LQueue, LServer);

  Writeln('Grafo iniciado. Aguarde a conclusão...');
  ReadLn;
  LServer.Stop;
end.
```

**Grafo com ramos paralelos:**

```pascal
// ingest → [normalize_a, normalize_b] → merge → export
TSidekiqJobGraph.New
  .Node('ingest',      TIngestHandler.Create)
  .Node('normalize_a', TNormalizeAHandler.Create).DependsOn('ingest')
  .Node('normalize_b', TNormalizeBHandler.Create).DependsOn('ingest')
  .Node('merge',       TMergeHandler.Create)
    .DependsOn('normalize_a')
    .DependsOn('normalize_b')
  .Node('export',      TExportHandler.Create).DependsOn('merge')
  .Parallel   // normalize_a e normalize_b executam em paralelo
  .Execute(LQueue, LServer);
```

Ver [job-graph.md](../04-features/job-graph.md) para referência completa da API.
