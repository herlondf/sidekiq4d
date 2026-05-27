program ETLPipeline;

{$APPTYPE CONSOLE}

// Demo: ETL Pipeline with Sequential Stages
//
// Demonstrates job chaining pattern:
// - Stage 1 (Extract): Reads data source, enqueues transform jobs
// - Stage 2 (Transform): Processes data, enqueues load jobs
// - Stage 3 (Load): Writes to destination
// - Uses EnqueueIn for inter-stage delays (simulating real pipelines)
// - Shows how to build multi-step workflows with Hefesto

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Store.InMemory,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

var
  GServer: IHefestoServer;

type
  TExtractHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TTransformHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TLoadHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

{ TExtractHandler }

function TExtractHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'etl_extract';
end;

procedure TExtractHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [EXTRACT] Reading source: %s', [AContext.Job.Body]));
  Sleep(100); // Simulate reading from source

  // After extraction, enqueue transform jobs with a small delay
  WriteLn('  [EXTRACT] Data extracted. Scheduling transform...');
  GServer.EnqueueIn('default', 'etl_transform',
    Format('{"source":%s,"rows":1000}', [AContext.Job.Body]), 1);
end;

{ TTransformHandler }

function TTransformHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'etl_transform';
end;

procedure TTransformHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [TRANSFORM] Processing: %s', [AContext.Job.Body]));
  Sleep(100); // Simulate data transformation

  // After transformation, enqueue load jobs
  WriteLn('  [TRANSFORM] Data cleaned and normalized. Scheduling load...');
  GServer.EnqueueIn('default', 'etl_load',
    Format('{"transformed":%s,"records":950}', [AContext.Job.Body]), 1);
end;

{ TLoadHandler }

function TLoadHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'etl_load';
end;

procedure TLoadHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [LOAD] Writing to destination: %s', [AContext.Job.Body]));
  Sleep(100); // Simulate writing to destination database
  WriteLn('  [LOAD] Records committed to data warehouse.');
end;

var
  Queue: THefestoInMemoryQueueAdapter;
begin
  try
    WriteLn('===========================================');
    WriteLn(' Hefesto Demo: ETL Pipeline');
    WriteLn('===========================================');
    WriteLn('');
    WriteLn('Pipeline stages:');
    WriteLn('  Extract -> Transform -> Load');
    WriteLn('  (each stage enqueues the next with delay)');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;

    // Seed the pipeline with extract jobs
    Queue.Enqueue('etl_extract', '{"table":"customers","db":"production"}');
    Queue.Enqueue('etl_extract', '{"table":"orders","db":"production"}');
    Queue.Enqueue('etl_extract', '{"table":"products","db":"inventory"}');

    WriteLn('--- Enqueued 3 extract jobs (pipeline seeds) ---');
    WriteLn('');
    WriteLn('--- Processing pipeline ---');
    WriteLn('');

    GServer := THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(100)
      .StopWhenIdle
      .StateStore(THefestoInMemoryStateStore.New)
      .RetryPolicy(THefestoSimpleRetryPolicy.New(2, 3))
      .Telemetry(THefestoConsoleTelemetry.New)
      .RegisterHandler('etl_extract', TExtractHandler.Create)
      .RegisterHandler('etl_transform', TTransformHandler.Create)
      .RegisterHandler('etl_load', TLoadHandler.Create);

    GServer.Run;

    WriteLn('');
    WriteLn('Pipeline complete. Each stage triggered the next,');
    WriteLn('demonstrating job chaining for multi-step workflows.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
