# Receita: Telemetria OTLP com Jaeger

Enviar traces OpenTelemetry para Jaeger para observabilidade completa dos jobs.

## Pré-requisitos

Iniciar Jaeger via Docker:

```bash
cd docker
docker-compose up -d jaeger
```

Ou diretamente:
```bash
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

- UI Jaeger: `http://localhost:16686`
- OTLP HTTP: `http://localhost:4318`

## Código

```pascal
program TelemetriaOTLP;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Telemetry.Console,
  Sidekiq4D.Telemetry.OTLP;

type
  TProcessHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

  TFalhaHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TProcessHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process';
end;

procedure TProcessHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Sleep(Random(200) + 50); // simula latência variável
  Writeln('Processado: ', AJob.JobId);
end;

function TFalhaHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'falha';
end;

procedure TFalhaHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  raise Exception.Create('Erro intencional para trace de falha');
end;

var
  LQueue: ISidekiqQueueAdapter;
  LServer: ISidekiqServer;
  I: Integer;
begin
  Randomize;
  LQueue := TSidekiqInMemoryQueueAdapter.New;

  LServer := TSidekiqServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(TSidekiqInMemoryStateStore.New)
    .RetryPolicy(TSidekiqExponentialRetryPolicy.New(3, 5, 60))
    .Telemetry(
      // Encadear console + OTLP
      TSidekiqCompositeTelemetry.New([
        TSidekiqConsoleTelemetry.New,
        TSidekiqOTLPTraceTelemetry.New(
          'http://localhost:4318',  // OTLP HTTP endpoint
          'sidekiq4d-demo'         // service.name
        )
      ])
    )
    .RegisterHandler('process', TProcessHandler.Create)
    .RegisterHandler('falha',   TFalhaHandler.Create)
    .Run;

  // Enfileirar jobs de sucesso e falha para gerar traces variados
  for I := 1 to 10 do
    LQueue.Enqueue('process', Format('{"id": %d}', [I]));

  for I := 1 to 3 do
    LQueue.Enqueue('falha', Format('{"id": %d}', [I]));

  Writeln('Jobs enfileirados. Acesse http://localhost:16686 para ver os traces.');
  ReadLn;
  LServer.Stop;
end.
```

## Verificando no Jaeger

1. Acesse `http://localhost:16686`
2. Selecione `sidekiq4d-demo` em "Service"
3. Clique em "Find Traces"
4. Cada job aparece como um span com `job.action`, `job.queue`, `job.id`
5. Jobs com falha aparecem com status ERROR e a mensagem da exception

## Troubleshooting

**Traces não aparecem:**
- Verificar se Jaeger está rodando: `docker ps | grep jaeger`
- Verificar o endpoint: `curl -X POST http://localhost:4318/v1/traces`
- Verificar Content-Type: deve ser `application/json`
- Verificar timezone: `DateTimeToUnix` com segundo parâmetro `False`

Ver [troubleshooting.md](../05-operacao-e-runtime/troubleshooting.md) para mais detalhes.
