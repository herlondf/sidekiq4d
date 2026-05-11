program HTTPIngressDemo;

{$APPTYPE CONSOLE}

// Demo: HTTP Ingress Queue Adapter
//
// Mostra como usar o Sidekiq4D como receptor HTTP que processa jobs
// recebidos via REST. Ideal para:
// - Agentes de telemetria
// - Webhooks receivers
// - API gateways com processamento assincrono
// - Buffering de eventos com retry
//
// Endpoints:
//   POST /events -> enfileira 1 job (replicado por target)
//   POST /batch  -> enfileira N jobs (JSON array)
//   GET  /health -> status do servidor
//
// Teste com curl:
//   curl -X POST http://localhost:9090/events \
//     -H "Content-Type: application/json" \
//     -d '{"type":"user.created","user_id":"42"}'
//
//   curl http://localhost:9090/health

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Server,
  Sidekiq4D.Ingress.HTTP,
  Sidekiq4D.Ingress.HTTP.Indy;

type
  // Handler que simplesmente loga os eventos recebidos
  TLogHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    FProviderName: string;
  public
    constructor Create(const AName: string);
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

constructor TLogHandler.Create(const AName: string);
begin
  inherited Create;
  FProviderName := AName;
end;

function TLogHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, FProviderName);
end;

procedure TLogHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [%s] Entregando evento: %s',
    [FProviderName, Copy(AContext.Job.Body, 1, 80)]));
end;

const
  HOST = '127.0.0.1';
  PORT = 9090;

var
  Ingress: TSidekiqHTTPIngressAdapter;
begin
  try
    WriteLn('Sidekiq4D - Demo HTTP Ingress');
    WriteLn(Format('Escutando em http://%s:%d', [HOST, PORT]));
    WriteLn('');
    WriteLn('Endpoints:');
    WriteLn('  POST /events  -> enfileira evento');
    WriteLn('  POST /batch   -> enfileira lote (JSON array)');
    WriteLn('  GET  /health  -> status');
    WriteLn('');
    WriteLn('Targets: console-log, file-log');
    WriteLn('');
    WriteLn('Teste:');
    WriteLn('  curl -X POST http://localhost:9090/events -d "{\"msg\":\"hello\"}"');
    WriteLn('');
    WriteLn('Pressione Ctrl+C para encerrar.');
    WriteLn('---');

    // Configura o ingress com 2 targets (cada evento gera 2 jobs)
    Ingress := TSidekiqHTTPIngressAdapter.New
      .Host(HOST)
      .Port(PORT)
      .Targets(['console-log', 'file-log'])
      .UseHTTPServer(TSidekiqIndyHTTPServer.New);

    TSidekiqServer.New
      .UseQueue(Ingress)
      .Concurrency(2)
      .BatchSize(10)
      .IdleDelayMs(200)
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(3, 5))
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('console-log', TLogHandler.Create('console-log'))
      .RegisterHandler('file-log', TLogHandler.Create('file-log'))
      .Run;
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
