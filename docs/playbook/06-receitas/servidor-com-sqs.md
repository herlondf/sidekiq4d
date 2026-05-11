# Receita: Servidor com AWS SQS

Requer conta AWS com fila SQS criada e credenciais configuradas.

```pascal
program ServidorSQS;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.SQS,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry.Console;

type
  TProcessOrderHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TProcessOrderHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process_order';
end;

procedure TProcessOrderHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('Processando pedido: ', AJob.Body);
end;

var
  LServer: ISidekiqServer;
begin
  LServer := TSidekiqServer.New
    .UseQueue(
      TSidekiqSQSQueueAdapter.New
        .QueueUrl('https://sqs.us-east-1.amazonaws.com/123456789/minha-fila')
        .Region('us-east-1')
        .AccessKeyId(GetEnvironmentVariable('AWS_ACCESS_KEY_ID'))
        .SecretAccessKey(GetEnvironmentVariable('AWS_SECRET_ACCESS_KEY'))
        .WaitTimeSeconds(20)    // long polling
        .MaxMessages(10)        // batch de fetch
    )
    .Concurrency(4)
    .StateStore(TSidekiqInMemoryStateStore.New)
    .RetryPolicy(TSidekiqExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('process_order', TProcessOrderHandler.Create)
    .Run;

  Writeln('Aguardando jobs do SQS...');
  ReadLn;
  LServer.Stop;
end.
```

**Notas:**
- `WaitTimeSeconds(20)` habilita long polling — reduz custos e latência
- Credenciais via variáveis de ambiente (não hardcode)
- SQS gerencia DLQ nativamente se configurada no console AWS
- Para múltiplas filas, criar múltiplos adapters e servidores separados
