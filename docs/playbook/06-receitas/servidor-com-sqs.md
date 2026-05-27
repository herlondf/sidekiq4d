# Receita: Servidor com AWS SQS

Requer conta AWS com fila SQS criada e credenciais configuradas.

```pascal
program ServidorSQS;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.SQS,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

type
  TProcessOrderHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TProcessOrderHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process_order';
end;

procedure TProcessOrderHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processando pedido: ', AJob.Body);
end;

var
  LServer: IHefestoServer;
begin
  LServer := THefestoServer.New
    .UseQueue(
      THefestoSQSQueueAdapter.New
        .QueueUrl('https://sqs.us-east-1.amazonaws.com/123456789/minha-fila')
        .Region('us-east-1')
        .AccessKeyId(GetEnvironmentVariable('AWS_ACCESS_KEY_ID'))
        .SecretAccessKey(GetEnvironmentVariable('AWS_SECRET_ACCESS_KEY'))
        .WaitTimeSeconds(20)    // long polling
        .MaxMessages(10)        // batch de fetch
    )
    .Concurrency(4)
    .StateStore(THefestoInMemoryStateStore.New)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
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
