program SQSBenchmarkSeeder;

{$APPTYPE CONSOLE}

// Seeder: Popula a fila SQS com N mensagens para benchmark.
// Uso: SQSBenchmark.Seeder.exe [quantidade]  (default: 200)

uses
  System.SysUtils,
  System.Diagnostics,
  System.Generics.Collections,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.SQS;

const
  QUEUE_URL = 'http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/benchmark-queue';
  ACCESS_KEY = 'test';
  SECRET_KEY = 'test';
  REGION = 'us-east-1';

var
  Adapter: THefestoSqsQueueAdapter;
  Watch: TStopwatch;
  Count: Integer;
  I: Integer;
  Request: THefestoPublishRequest;
begin
  try
    Count := 200;
    if ParamCount >= 1 then
      Count := StrToIntDef(ParamStr(1), 200);

    WriteLn('=== SQS Benchmark Seeder ===');
    WriteLn(Format('Queue: %s', [QUEUE_URL]));
    WriteLn(Format('Mensagens a publicar: %d', [Count]));
    WriteLn('');

    Adapter := THefestoSqsQueueAdapter.New
      .QueueUrl(QUEUE_URL)
      .AccessKey(ACCESS_KEY)
      .SecretKey(SECRET_KEY)
      .Region(REGION);

    Watch := TStopwatch.StartNew;

    for I := 1 to Count do
    begin
      Request.QueueName := 'benchmark-queue';
      Request.Action := 'process';
      Request.Body := Format('{"id":%d,"payload":"benchmark-data-%d","timestamp":"%s"}',
        [I, I, FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now)]);
      Request.DelaySeconds := 0;
      SetLength(Request.Attributes, 0);

      (Adapter as IHefestoQueuePublisher).Publish(Request);

      if (I mod 50) = 0 then
        Write(Format('  %d/%d publicadas...'#13, [I, Count]));
    end;

    Watch.Stop;
    WriteLn(Format('  %d/%d publicadas.        ', [Count, Count]));
    WriteLn('');
    WriteLn(Format('Tempo total: %.1fms', [Watch.Elapsed.TotalMilliseconds]));
    WriteLn(Format('Throughput: %.0f msgs/s', [Count / Watch.Elapsed.TotalSeconds]));
    WriteLn('');
    WriteLn('Fila pronta para benchmark.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
