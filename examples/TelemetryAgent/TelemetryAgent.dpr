program TelemetryAgent;

{$APPTYPE CONSOLE}

// Demo: Agente de Telemetria construido sobre Sidekiq4D
//
// Substitui o AgenteTelemetria usando a infraestrutura do Sidekiq4D:
// - HTTP Ingress adapter (escuta POST /events e /batch)
// - Circuit breaker middleware (por provider)
// - Provider handlers (Elasticsearch, Datadog, OTLP)
// - Retry com backoff exponencial
// - State store SQLite para persistencia local
//
// Arquitetura:
//   HTTP POST /events -> [HTTP Ingress Adapter]
//     -> 1 job por (evento x target)
//     -> [Circuit Breaker Middleware]
//     -> [Provider Handler] -> HTTP POST para destino
//     -> Ack ou Retry com backoff
//
// Execucao:
//   1. Inicie este programa
//   2. Envie eventos: curl -X POST http://localhost:9090/events -d '{"event_type":"log",...}'
//   3. O agente entrega para os providers configurados

uses
  System.SysUtils,
  System.Math,
  FireDAC.Comp.Client,
  FireDAC.Stan.Def,
  FireDAC.Stan.Async,
  FireDAC.DApt,
  FireDAC.Phys.SQLite,
  FireDAC.Phys.SQLiteDef,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.Postgres,
  Sidekiq4D.Store.FireDAC,
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Retry,
  Sidekiq4D.Middleware,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server,
  Sidekiq4D.Ingress.HTTP,
  Sidekiq4D.Ingress.HTTP.Indy,
  Sidekiq4D.Middleware.CircuitBreaker,
  Sidekiq4D.Telemetry.Provider;

type
  // Retry policy com backoff exponencial e jitter
  TAgentRetryPolicy = class(TInterfacedObject, ISidekiqRetryPolicy)
  private
    FMaxAttempts: Integer;
  public
    constructor Create(AMaxAttempts: Integer = 5);
    function Decide(const AJob: ISidekiqJobEnvelope;
      const AError: Exception): TSidekiqRetryDecision;
  end;

constructor TAgentRetryPolicy.Create(AMaxAttempts: Integer);
begin
  inherited Create;
  FMaxAttempts := AMaxAttempts;
end;

function TAgentRetryPolicy.Decide(const AJob: ISidekiqJobEnvelope;
  const AError: Exception): TSidekiqRetryDecision;
var
  LDelay: Integer;
begin
  if AJob.Attempts < FMaxAttempts then
  begin
    // Backoff: 5s, 10s, 20s, 40s, 80s + jitter
    LDelay := 5 * Round(Power(2, AJob.Attempts)) + Random(5);
    Result := TSidekiqRetryDecision.Retry(LDelay, AError.Message);
  end
  else
    Result := TSidekiqRetryDecision.DeadLetter(
      Format('Quarentena apos %d tentativas: %s', [FMaxAttempts, AError.Message]));
end;

const
  LISTEN_HOST = '127.0.0.1';
  LISTEN_PORT = 9090;
  DB_FILE     = 'telemetry_agent.db';

var
  Ingress: TSidekiqHTTPIngressAdapter;
  CircuitBreaker: TSidekiqCircuitBreakerMiddleware;
  Connection: TFDConnection;
  StateStore: ISidekiqStateStore;
begin
  try
    WriteLn('=================================================');
    WriteLn('  Sidekiq4D Telemetry Agent');
    WriteLn('  Escutando em http://' + LISTEN_HOST + ':' + IntToStr(LISTEN_PORT));
    WriteLn('=================================================');
    WriteLn('');
    WriteLn('Endpoints:');
    WriteLn('  POST /events  -> enfileira 1 evento');
    WriteLn('  POST /batch   -> enfileira N eventos (JSON array)');
    WriteLn('  GET  /health  -> status do agente');
    WriteLn('');
    WriteLn('Providers configurados:');
    WriteLn('  - elasticsearch (http://localhost:9200)');
    WriteLn('  - datadog (logs API)');
    WriteLn('  - otlp (http://localhost:4318)');
    WriteLn('');
    WriteLn('Circuit breaker: 5 falhas consecutivas -> open por 60s');
    WriteLn('Retry: backoff exponencial, max 5 tentativas');
    WriteLn('Persistencia: SQLite local (' + DB_FILE + ')');
    WriteLn('');
    WriteLn('Pressione Ctrl+C para encerrar.');
    WriteLn('-------------------------------------------------');

    // --- HTTP Ingress ---
    Ingress := TSidekiqHTTPIngressAdapter.New
      .Host(LISTEN_HOST)
      .Port(LISTEN_PORT)
      .Targets(['elasticsearch', 'datadog', 'otlp'])
      .UseHTTPServer(TSidekiqIndyHTTPServer.New);

    // --- Circuit Breaker ---
    CircuitBreaker := TSidekiqCircuitBreakerMiddleware.New
      .FailureThreshold(5)
      .CooldownSeconds(60);

    // --- State Store SQLite ---
    Connection := TSidekiqFireDACBackend.NewSQLiteConnection(DB_FILE);

    StateStore := TSidekiqPostgresStateStore.New
      .Backend(TSidekiqFireDACBackend.New(Connection, True))
      .TableName('agent_state');

    // --- Servidor ---
    TSidekiqServer.New
      .UseQueue(Ingress)
      .Concurrency(4)
      .BatchSize(20)
      .IdleDelayMs(500)

      // Providers
      .StateStore(StateStore)
      .LockProvider(TSidekiqInMemoryLockProvider.New(StateStore))
      .Idempotency(TSidekiqStateStoreIdempotency.New(StateStore))

      // Circuit breaker como middleware
      .UseServerMiddleware(CircuitBreaker)

      // Retry com backoff
      .RetryPolicy(TAgentRetryPolicy.Create(5))

      // Telemetry
      .Telemetry(TSidekiqConsoleTelemetry.New)

      // Provider handlers (cada um processa sua action)
      .RegisterHandler('elasticsearch',
        TSidekiqElasticsearchHandler.New('http://localhost:9200', 'telemetry-events'))
      .RegisterHandler('datadog',
        TSidekiqDatadogHandler.New('https://http-intake.logs.datadoghq.com', 'YOUR_DD_API_KEY'))
      .RegisterHandler('otlp',
        TSidekiqOTLPHandler.New('http://localhost:4318'))

      .Run;

  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
