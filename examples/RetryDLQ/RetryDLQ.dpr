program RetryDLQ;

{$APPTYPE CONSOLE}

// Demo: Retry com Backoff e Dead-Letter Queue
//
// Mostra o ciclo completo de falha de um job:
// 1. Job falha na primeira execucao
// 2. Retry policy decide: tentar novamente com delay (backoff)
// 3. Job falha de novo apos N tentativas
// 4. Retry policy decide: mover para Dead-Letter Queue
// 5. Telemetry registra cada evento do ciclo
//
// THefestoExponentialRetryPolicy (Hefesto.Retry): delay = base x tentativa^2
// THefestoSimpleRetryPolicy (Hefesto.Retry):      delay fixo configuravel

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Ack,
  Hefesto.Retry,
  Hefesto.Dispatcher,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  // Handler que falha nas primeiras tentativas e sucede na 3a
  TFlakyHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FCallCount: Integer;
  public
    constructor Create;
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  // Handler que sempre falha (vai para DLQ)
  TAlwaysFailHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

{ TFlakyHandler }

constructor TFlakyHandler.Create;
begin
  inherited;
  FCallCount := 0;
end;

function TFlakyHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'flaky_job';
end;

procedure TFlakyHandler.Perform(const AContext: IHefestoJobContext);
begin
  Inc(FCallCount);
  if FCallCount < 3 then
  begin
    WriteLn(Format('  [flaky] Tentativa %d - FALHOU (simulando erro temporario)',
      [FCallCount]));
    raise Exception.Create('Erro temporario: servico indisponivel');
  end;
  WriteLn(Format('  [flaky] Tentativa %d - SUCESSO!', [FCallCount]));
end;

{ TAlwaysFailHandler }

function TAlwaysFailHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'doomed_job';
end;

procedure TAlwaysFailHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn('  [doomed] Falha permanente!');
  raise Exception.Create('Erro fatal: dados invalidos');
end;

var
  Queue: THefestoInMemoryQueueAdapter;
begin
  try
    WriteLn('Hefesto - Demo Retry & Dead-Letter Queue');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;

    // --- Cenario 1: Job flaky que eventualmente sucede ---
    WriteLn('=== Cenario 1: Job flaky (sucede na 3a tentativa) ===');
    WriteLn('  Retry policy: max 5 tentativas, backoff exponencial (2s base)');
    WriteLn('');
    Queue.Enqueue('flaky_job', '{"service":"payment-gateway"}');

    THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 0, 3600)) // delay = base * n^2
      .Telemetry(THefestoConsoleTelemetry.New)
      .RegisterHandler('flaky_job', TFlakyHandler.Create)
      .Run;

    WriteLn('');

    // --- Cenario 2: Job que sempre falha (vai para DLQ) ---
    WriteLn('=== Cenario 2: Job condenado (vai para Dead-Letter) ===');
    WriteLn('  Retry policy: max 3 tentativas, delay fixo 0s');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;
    Queue.Enqueue('doomed_job', '{"payload":"dados_corrompidos"}');

    THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .RetryPolicy(THefestoSimpleRetryPolicy.New(3, 0)) // 3 tentativas, 0s delay
      .Telemetry(THefestoConsoleTelemetry.New)
      .RegisterHandler('doomed_job', TAlwaysFailHandler.Create)
      .Run;

    WriteLn('');
    WriteLn('=== Resumo ===');
    WriteLn('  - THefestoSimpleRetryPolicy: delay fixo, max tentativas');
    WriteLn('  - THefestoExponentialRetryPolicy: delay = base x n^2, max tentativas');
    WriteLn('  - Apos max: DeadLetter (telemetry.JobDeadLettered) ou Discard');
    WriteLn('  - Em producao com SQS: DLQ e a fila nativa do AWS');
    WriteLn('');
    WriteLn('Demo concluido.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
