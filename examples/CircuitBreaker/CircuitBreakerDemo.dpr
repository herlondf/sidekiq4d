program CircuitBreakerDemo;

{$APPTYPE CONSOLE}

// Demo: Circuit Breaker Middleware
//
// Mostra como proteger providers externos com circuit breaker:
// - Apos N falhas consecutivas, o circuito abre
// - Jobs para esse provider sao rejeitados (nack) durante o cooldown
// - Apos cooldown, 1 tentativa e permitida (half-open)
// - Se sucede, circuito fecha; se falha, reabre
//
// Cenario simulado:
// - Provider "unstable" falha nas 5 primeiras tentativas
// - Circuit breaker abre apos 3 falhas
// - Jobs subsequentes sao rejeitados ate cooldown expirar

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Middleware,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Server,
  Sidekiq4D.Middleware.CircuitBreaker;

type
  TUnstableHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    FCallCount: Integer;
    FFailUntil: Integer;
  public
    constructor Create(AFailUntil: Integer);
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TStableHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

constructor TUnstableHandler.Create(AFailUntil: Integer);
begin
  inherited Create;
  FCallCount := 0;
  FFailUntil := AFailUntil;
end;

function TUnstableHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'unstable';
end;

procedure TUnstableHandler.Perform(const AContext: ISidekiqJobContext);
begin
  Inc(FCallCount);
  if FCallCount <= FFailUntil then
  begin
    WriteLn(Format('  [unstable] Tentativa %d - FALHA (provider indisponivel)',
      [FCallCount]));
    raise Exception.Create('Provider timeout');
  end;
  WriteLn(Format('  [unstable] Tentativa %d - SUCESSO (provider recuperou)',
    [FCallCount]));
end;

function TStableHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'stable';
end;

procedure TStableHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn('  [stable] Processado com sucesso');
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  CB: TSidekiqCircuitBreakerMiddleware;
  I: Integer;
begin
  try
    WriteLn('Sidekiq4D - Demo Circuit Breaker');
    WriteLn('');
    WriteLn('Configuracao:');
    WriteLn('  Failure threshold: 3 (abre apos 3 falhas consecutivas)');
    WriteLn('  Cooldown: 2s (tempo que fica aberto)');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    // Enfileira 8 jobs para provider instavel + 2 para estavel
    for I := 1 to 8 do
      Queue.Enqueue('unstable', Format('{"attempt":%d}', [I]));
    Queue.Enqueue('stable', '{"ok":true}');
    Queue.Enqueue('stable', '{"ok":true}');

    CB := TSidekiqCircuitBreakerMiddleware.New
      .FailureThreshold(3)
      .CooldownSeconds(2);

    WriteLn('--- Processando ---');
    WriteLn('');

    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .UseServerMiddleware(CB)
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(2, 0))
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('unstable', TUnstableHandler.Create(5))
      .RegisterHandler('stable', TStableHandler.Create)
      .Run;

    WriteLn('');
    WriteLn('--- Estado final dos circuitos ---');
    WriteLn(Format('  unstable: state=%d failures=%d',
      [Ord(CB.CircuitStateFor('unstable')), CB.FailureCountFor('unstable')]));
    WriteLn(Format('  stable:   state=%d failures=%d',
      [Ord(CB.CircuitStateFor('stable')), CB.FailureCountFor('stable')]));
    WriteLn('  (0=closed, 1=open, 2=half-open)');
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
