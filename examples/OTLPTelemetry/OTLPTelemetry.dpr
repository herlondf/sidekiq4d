program OTLPTelemetry;

{$APPTYPE CONSOLE}

{
  Exemplo: Sidekiq4D.Telemetry.OTLP — OpenTelemetry Trace Exporter

  Demonstra como enviar traces de jobs para um coletor OTLP/HTTP.
  Compativel com: Jaeger v2+, Grafana Tempo, Honeycomb, OTel Collector.

  Para testar localmente com Jaeger all-in-one:
    docker run -d --name jaeger \
      -p 4318:4318 \
      -p 16686:16686 \
      jaegertracing/all-in-one:latest

  Acesse o dashboard em: http://localhost:16686

  O exporter envia para http://localhost:4318/v1/traces.
  Cada job gera um span com atributos job.id, job.queue, job.action, job.attempts.
}

uses
  System.SysUtils,
  Sidekiq4D.Telemetry        in '..\..\src\Sidekiq4D.Telemetry.pas',
  Sidekiq4D.Telemetry.OTLP   in '..\..\src\adapters\Sidekiq4D.Telemetry.OTLP.pas',
  Sidekiq4D.Job              in '..\..\src\Sidekiq4D.Job.pas';

procedure SimulateJobLifecycle(
  const ATelemetry: ISidekiqTelemetry;
  const AId, AAction, AQueue: string;
  const AShouldFail: Boolean);
var
  LJob: ISidekiqJobEnvelope;
begin
  LJob := TSidekiqJobEnvelope.New(AId, AQueue, '', '{}', AAction, 1);

  WriteLn(Format('[%s] iniciando job "%s" na fila "%s"...', [AAction, AId, AQueue]));
  ATelemetry.JobStarted(LJob);

  Sleep(Random(300) + 50); // simula processamento

  if AShouldFail then
  begin
    try
      raise Exception.Create('simulated processing error');
    except
      on E: Exception do
      begin
        ATelemetry.JobFailed(LJob, E, 150);
        WriteLn(Format('[%s] falhou: %s (span exportado com status ERROR)', [AAction, E.Message]));
      end;
    end;
  end
  else
  begin
    ATelemetry.JobSucceeded(LJob, 150);
    WriteLn(Format('[%s] concluido com sucesso (span exportado com status OK)', [AAction]));
  end;
end;

var
  LTelemetry: ISidekiqTelemetry;
  LOTLPEndpoint: string;
begin
  Randomize;

  LOTLPEndpoint := 'http://localhost:4318';
  if ParamCount > 0 then
    LOTLPEndpoint := ParamStr(1);

  WriteLn('====================================================');
  WriteLn('  Sidekiq4D — OTLP Trace Exporter Example');
  WriteLn('====================================================');
  WriteLn('Endpoint: ', LOTLPEndpoint);
  WriteLn;

  LTelemetry := TSidekiqCompositeTelemetry.New([
    TSidekiqConsoleTelemetry.New,
    TSidekiqOTLPTraceTelemetry.New(LOTLPEndpoint, 'sidekiq4d-demo', 'console-01')
  ]);

  LTelemetry.ServerStarted;

  WriteLn('--- Simulando 4 jobs ---');
  WriteLn;

  SimulateJobLifecycle(LTelemetry, 'job-001', 'SendEmail',      'emails',  False);
  SimulateJobLifecycle(LTelemetry, 'job-002', 'ProcessPayment', 'payments', False);
  SimulateJobLifecycle(LTelemetry, 'job-003', 'GenerateReport', 'reports',  True);
  SimulateJobLifecycle(LTelemetry, 'job-004', 'SendNotification','notify',  False);

  WriteLn;
  LTelemetry.ServerStopped;

  WriteLn('====================================================');
  WriteLn('4 spans exportados para: ', LOTLPEndpoint, '/v1/traces');
  WriteLn;
  WriteLn('Verifique no Jaeger: http://localhost:16686');
  WriteLn('  Service: sidekiq4d-demo');
  WriteLn('  3 spans OK, 1 span ERROR (GenerateReport)');
  WriteLn;
  Write('Pressione Enter para sair... ');
  ReadLn;
end.
