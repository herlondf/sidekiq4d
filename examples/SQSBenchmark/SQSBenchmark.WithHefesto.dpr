program SQSBenchmarkWithHefesto;

{$APPTYPE CONSOLE}

// Benchmark: Consumo SQS COM Hefesto
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
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.SQS,
  Hefesto.Retry,
  Hefesto.Dispatcher,
  Hefesto.Telemetry,
  Hefesto.Server;

const
  QUEUE_URL = 'http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/benchmark-queue';
  ACCESS_KEY = 'test';
  SECRET_KEY = 'test';
  REGION = 'us-east-1';
  WORK_SIMULATION_MS = 50;

type
  TBenchmarkHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

var
  GTotalProcessed: Integer = 0;
  GWatch: TStopwatch;

function TBenchmarkHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TBenchmarkHandler.Perform(const AContext: IHefestoJobContext);
var
  LCount: Integer;
begin
  // Mesma simulacao de trabalho do benchmark sem Hefesto
  Sleep(WORK_SIMULATION_MS);
  LCount := TInterlocked.Increment(GTotalProcessed);

  if (LCount mod 20) = 0 then
    WriteLn(Format('  Processadas: %d (%.1f msgs/s)',
      [LCount, LCount / GWatch.Elapsed.TotalSeconds]));
end;

var
  Adapter: THefestoSqsQueueAdapter;
begin
  CoInitialize(nil);
  try
    WriteLn('=== SQS Benchmark: COM Hefesto ===');
    WriteLn('');
    WriteLn('Config: BatchSize=10, Concurrency=4, WaitTime=5s, LongPolling');
    WriteLn(Format('Queue: %s', [QUEUE_URL]));
    WriteLn(Format('Work simulation: %dms por mensagem', [WORK_SIMULATION_MS]));
    WriteLn('');
    WriteLn('--- Processando ---');

    Adapter := THefestoSqsQueueAdapter.New
      .QueueUrl(QUEUE_URL)
      .AccessKey(ACCESS_KEY)
      .SecretKey(SECRET_KEY)
      .Region(REGION);

    GWatch := TStopwatch.StartNew;

    THefestoServer.New
      .UseQueue(Adapter)
      .Concurrency(4)
      .BatchSize(10)
      .WaitTimeSeconds(5)
      .VisibilityTimeout(60)
      .IdleDelayMs(500)
      .StopWhenIdle
      .RetryPolicy(THefestoSimpleRetryPolicy.New(3, 5))
      .RegisterHandler('process', TBenchmarkHandler.Create)
      .Run;

    GWatch.Stop;

    WriteLn('');
    WriteLn('=== Resultado COM Hefesto ===');
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
