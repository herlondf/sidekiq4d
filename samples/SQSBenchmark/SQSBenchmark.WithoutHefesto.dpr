program SQSBenchmarkWithoutHefesto;

{$APPTYPE CONSOLE}

// Benchmark: Consumo SQS SEM Hefesto (loop manual - padrao legado)
//
// Simula o padrao atual dos workers DocFiscAll:
// - ReceiveMessage com MaxMessages=1
// - Processamento sincrono (1 por vez)
// - Sleep(15000) quando fila vazia
// - DeleteMessage apos sucesso
//
// Execute o Seeder antes: SQSBenchmark.Seeder.exe 200

uses
  System.SysUtils,
  System.Diagnostics,
  System.Generics.Collections,
  Winapi.ActiveX,
  Hefesto.SQS.Client;

const
  QUEUE_URL = 'http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/benchmark-queue';
  ACCESS_KEY = 'test';
  SECRET_KEY = 'test';
  REGION = 'us-east-1';
  WORK_SIMULATION_MS = 50;

var
  SQS: THefestoSqsClient;
  Watch: TStopwatch;
  TotalProcessed: Integer;
  EmptyCycles: Integer;
  Options: THefestoSqsReceiveOptions;
  Messages: TObjectList<THefestoSqsMessage>;
  Msg: THefestoSqsMessage;
begin
  CoInitialize(nil);
  try
    WriteLn('=== SQS Benchmark: SEM Hefesto (loop manual) ===');
    WriteLn('');
    WriteLn('Padrao: Get(1) + Process + Delete + Sleep(15s) se vazio');
    WriteLn(Format('Queue: %s', [QUEUE_URL]));
    WriteLn(Format('Work simulation: %dms por mensagem', [WORK_SIMULATION_MS]));
    WriteLn('');
    WriteLn('--- Processando ---');

    SQS := THefestoSqsClient.New
      .QueueUrl(QUEUE_URL)
      .AccessKey(ACCESS_KEY)
      .SecretKey(SECRET_KEY)
      .Region(REGION);
    try
      TotalProcessed := 0;
      EmptyCycles := 0;
      Watch := TStopwatch.StartNew;

      Options.BatchSize := 1;        // Padrao legado: 1 mensagem por vez
      Options.WaitTimeSeconds := 0;  // Sem long-polling (padrao legado)
      Options.VisibilityTimeout := 30;

      while EmptyCycles < 2 do
      begin
        Messages := SQS.ReceiveMessages(Options);
        try
          if (Messages = nil) or (Messages.Count = 0) then
          begin
            Inc(EmptyCycles);
            if EmptyCycles < 2 then
            begin
              WriteLn(Format('  [idle] Fila vazia - dormindo 15s... (ciclo %d)',
                [EmptyCycles]));
              Sleep(15000); // Padrao legado: 15 segundos de espera fixa
            end;
            Continue;
          end;

          EmptyCycles := 0;

          for Msg in Messages do
          begin
            // Processa (simula trabalho)
            Sleep(WORK_SIMULATION_MS);
            Inc(TotalProcessed);

            // Delete (ack)
            SQS.DeleteMessage(Msg.ReceiptHandle);

            if (TotalProcessed mod 20) = 0 then
              WriteLn(Format('  Processadas: %d (%.1f msgs/s)',
                [TotalProcessed, TotalProcessed / Watch.Elapsed.TotalSeconds]));
          end;
        finally
          Messages.Free;
        end;
      end;

      Watch.Stop;

      WriteLn('');
      WriteLn('=== Resultado SEM Hefesto ===');
      WriteLn(Format('  Mensagens processadas: %d', [TotalProcessed]));
      WriteLn(Format('  Tempo total: %.1fs', [Watch.Elapsed.TotalSeconds]));
      if Watch.Elapsed.TotalSeconds > 0 then
        WriteLn(Format('  Throughput: %.1f msgs/s',
          [TotalProcessed / Watch.Elapsed.TotalSeconds]));
      WriteLn('  Padrao: 1 msg/fetch, sequencial, sleep 15s');
    finally
      SQS.Free;
    end;
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
