program SQSBenchmarkWithSidekiq;

{$APPTYPE CONSOLE}

// Benchmark: Consumo SQS COM Sidekiq4D
//
// Configuracao otimizada:
// - BatchSize=10 (maximo SQS)
// - Concurrency=4 (4 workers paralelos)
// - WaitTimeSeconds=5 (long-polling)
// - StopWhenIdle (para quando fila esvazia)
//
// Execute o Seeder antes: SQSBenchmark.Seeder.exe 200

uses
  System.SysUtils,
  System.Diagnostics,
  System.SyncObjs,
  Winapi.ActiveX,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.SQS,
  Sidekiq4D.Retry,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

const
  QUEUE_URL = 'http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/benchmark-queue';
  ACCESS_KEY = 'test';
  SECRET_KEY = 'test';
  REGION = 'us-east-1';
  WORK_SIMULATION_MS = 50;

type
  TBenchmarkHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

var
  GTotalProcessed: Integer = 0;
  GWatch: TStopwatch;

function TBenchmarkHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TBenchmarkHandler.Perform(const AContext: ISidekiqJobContext);
var
  LCount: Integer;
begin
  // Mesma simulacao de trabalho do benchmark sem Sidekiq4D
  Sleep(WORK_SIMULATION_MS);
  LCount := TInterlocked.Increment(GTotalProcessed);

  if (LCount mod 20) = 0 then
    WriteLn(Format('  Processadas: %d (%.1f msgs/s)',
      [LCount, LCount / GWatch.Elapsed.TotalSeconds]));
end;

var
  Adapter: TSidekiqSqsQueueAdapter;
begin
  CoInitialize(nil);
  try
    WriteLn('=== SQS Benchmark: COM Sidekiq4D ===');
    WriteLn('');
    WriteLn('Config: BatchSize=10, Concurrency=4, WaitTime=5s, LongPolling');
    WriteLn(Format('Queue: %s', [QUEUE_URL]));
    WriteLn(Format('Work simulation: %dms por mensagem', [WORK_SIMULATION_MS]));
    WriteLn('');
    WriteLn('--- Processando ---');

    Adapter := TSidekiqSqsQueueAdapter.New
      .QueueUrl(QUEUE_URL)
      .AccessKey(ACCESS_KEY)
      .SecretKey(SECRET_KEY)
      .Region(REGION);

    GWatch := TStopwatch.StartNew;

    TSidekiqServer.New
      .UseQueue(Adapter)
      .Concurrency(4)
      .BatchSize(10)
      .WaitTimeSeconds(5)
      .VisibilityTimeout(60)
      .IdleDelayMs(500)
      .StopWhenIdle
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(3, 5))
      .RegisterHandler('process', TBenchmarkHandler.Create)
      .Run;

    GWatch.Stop;

    WriteLn('');
    WriteLn('=== Resultado COM Sidekiq4D ===');
    WriteLn(Format('  Mensagens processadas: %d', [GTotalProcessed]));
    WriteLn(Format('  Tempo total: %.1fs', [GWatch.Elapsed.TotalSeconds]));
    if GWatch.Elapsed.TotalSeconds > 0 then
      WriteLn(Format('  Throughput: %.1f msgs/s', [GTotalProcessed / GWatch.Elapsed.TotalSeconds]));
    WriteLn('  Config: 10 msgs/fetch, 4 workers, long-poll 5s');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
