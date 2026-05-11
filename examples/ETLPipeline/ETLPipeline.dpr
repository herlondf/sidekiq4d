program ETLPipeline;

{$APPTYPE CONSOLE}

// Demo: ETL Pipeline with Sequential Stages
//
// Demonstrates job chaining pattern:
// - Stage 1 (Extract): Reads data source, enqueues transform jobs
// - Stage 2 (Transform): Processes data, enqueues load jobs
// - Stage 3 (Load): Writes to destination
// - Uses EnqueueIn for inter-stage delays (simulating real pipelines)
// - Shows how to build multi-step workflows with Sidekiq4D

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

var
  GServer: ISidekiqServer;

type
  TExtractHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TTransformHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TLoadHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TExtractHandler }

function TExtractHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'etl_extract';
end;

procedure TExtractHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [EXTRACT] Reading source: %s', [AContext.Job.Body]));
  Sleep(100); // Simulate reading from source

  // After extraction, enqueue transform jobs with a small delay
  WriteLn('  [EXTRACT] Data extracted. Scheduling transform...');
  GServer.EnqueueIn('default', 'etl_transform',
    Format('{"source":%s,"rows":1000}', [AContext.Job.Body]), 1);
end;

{ TTransformHandler }

function TTransformHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'etl_transform';
end;

procedure TTransformHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [TRANSFORM] Processing: %s', [AContext.Job.Body]));
  Sleep(100); // Simulate data transformation

  // After transformation, enqueue load jobs
  WriteLn('  [TRANSFORM] Data cleaned and normalized. Scheduling load...');
  GServer.EnqueueIn('default', 'etl_load',
    Format('{"transformed":%s,"records":950}', [AContext.Job.Body]), 1);
end;

{ TLoadHandler }

function TLoadHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'etl_load';
end;

procedure TLoadHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [LOAD] Writing to destination: %s', [AContext.Job.Body]));
  Sleep(100); // Simulate writing to destination database
  WriteLn('  [LOAD] Records committed to data warehouse.');
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
begin
  try
    WriteLn('===========================================');
    WriteLn(' Sidekiq4D Demo: ETL Pipeline');
    WriteLn('===========================================');
    WriteLn('');
    WriteLn('Pipeline stages:');
    WriteLn('  Extract -> Transform -> Load');
    WriteLn('  (each stage enqueues the next with delay)');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    // Seed the pipeline with extract jobs
    Queue.Enqueue('etl_extract', '{"table":"customers","db":"production"}');
    Queue.Enqueue('etl_extract', '{"table":"orders","db":"production"}');
    Queue.Enqueue('etl_extract', '{"table":"products","db":"inventory"}');

    WriteLn('--- Enqueued 3 extract jobs (pipeline seeds) ---');
    WriteLn('');
    WriteLn('--- Processing pipeline ---');
    WriteLn('');

    GServer := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(100)
      .StopWhenIdle
      .StateStore(TSidekiqInMemoryStateStore.New)
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(2, 3))
      .Telemetry(TSidekiqConsoleTelemetry.New)
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
