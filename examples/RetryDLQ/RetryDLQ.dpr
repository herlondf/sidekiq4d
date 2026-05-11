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
// TSidekiqExponentialRetryPolicy (Sidekiq4D.Retry): delay = base x tentativa^2
// TSidekiqSimpleRetryPolicy (Sidekiq4D.Retry):      delay fixo configuravel

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Ack,
  Sidekiq4D.Retry,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  // Handler que falha nas primeiras tentativas e sucede na 3a
  TFlakyHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    FCallCount: Integer;
  public
    constructor Create;
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  // Handler que sempre falha (vai para DLQ)
  TAlwaysFailHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TFlakyHandler }

constructor TFlakyHandler.Create;
begin
  inherited;
  FCallCount := 0;
end;

function TFlakyHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'flaky_job';
end;

procedure TFlakyHandler.Perform(const AContext: ISidekiqJobContext);
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

function TAlwaysFailHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'doomed_job';
end;

procedure TAlwaysFailHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn('  [doomed] Falha permanente!');
  raise Exception.Create('Erro fatal: dados invalidos');
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
begin
  try
    WriteLn('Sidekiq4D - Demo Retry & Dead-Letter Queue');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    // --- Cenario 1: Job flaky que eventualmente sucede ---
    WriteLn('=== Cenario 1: Job flaky (sucede na 3a tentativa) ===');
    WriteLn('  Retry policy: max 5 tentativas, backoff exponencial (2s base)');
    WriteLn('');
    Queue.Enqueue('flaky_job', '{"service":"payment-gateway"}');

    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .RetryPolicy(TSidekiqExponentialRetryPolicy.New(5, 0, 3600)) // delay = base * n^2
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('flaky_job', TFlakyHandler.Create)
      .Run;

    WriteLn('');

    // --- Cenario 2: Job que sempre falha (vai para DLQ) ---
    WriteLn('=== Cenario 2: Job condenado (vai para Dead-Letter) ===');
    WriteLn('  Retry policy: max 3 tentativas, delay fixo 0s');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;
    Queue.Enqueue('doomed_job', '{"payload":"dados_corrompidos"}');

    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(3, 0)) // 3 tentativas, 0s delay
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('doomed_job', TAlwaysFailHandler.Create)
      .Run;

    WriteLn('');
    WriteLn('=== Resumo ===');
    WriteLn('  - TSidekiqSimpleRetryPolicy: delay fixo, max tentativas');
    WriteLn('  - TSidekiqExponentialRetryPolicy: delay = base x n^2, max tentativas');
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
