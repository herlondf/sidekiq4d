program HTTPIngressDemo;

{$APPTYPE CONSOLE}

// Demo: HTTP Ingress Queue Adapter
//
// Mostra como usar o Hefesto como receptor HTTP que processa jobs
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
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Dispatcher,
  Hefesto.Server,
  Hefesto.Ingress.HTTP,
  Hefesto.Ingress.HTTP.Indy;

type
  // Handler que simplesmente loga os eventos recebidos
  TLogHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FProviderName: string;
  public
    constructor Create(const AName: string);
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

constructor TLogHandler.Create(const AName: string);
begin
  inherited Create;
  FProviderName := AName;
end;

function TLogHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, FProviderName);
end;

procedure TLogHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [%s] Entregando evento: %s',
    [FProviderName, Copy(AContext.Job.Body, 1, 80)]));
end;

const
  HOST = '127.0.0.1';
  PORT = 9090;

var
  Ingress: THefestoHTTPIngressAdapter;
begin
  try
    WriteLn('Hefesto - Demo HTTP Ingress');
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
    Ingress := THefestoHTTPIngressAdapter.New
      .Host(HOST)
      .Port(PORT)
      .Targets(['console-log', 'file-log'])
      .UseHTTPServer(THefestoIndyHTTPServer.New);

    THefestoServer.New
      .UseQueue(Ingress)
      .Concurrency(2)
      .BatchSize(10)
      .IdleDelayMs(200)
      .RetryPolicy(THefestoSimpleRetryPolicy.New(3, 5))
      .Telemetry(THefestoConsoleTelemetry.New)
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
