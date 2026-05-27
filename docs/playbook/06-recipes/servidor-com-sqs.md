# Recipe: Server with AWS SQS

Requires an AWS account with an SQS queue created and credentials configured.

```pascal
program SQSServer;

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
  Writeln('Processing order: ', AJob.Body);
end;

var
  LServer: IHefestoServer;
begin
  LServer := THefestoServer.New
    .UseQueue(
      THefestoSQSQueueAdapter.New
        .QueueUrl('https://sqs.us-east-1.amazonaws.com/123456789/my-queue')
        .Region('us-east-1')
        .AccessKeyId(GetEnvironmentVariable('AWS_ACCESS_KEY_ID'))
        .SecretAccessKey(GetEnvironmentVariable('AWS_SECRET_ACCESS_KEY'))
        .WaitTimeSeconds(20)    // long polling
        .MaxMessages(10)        // fetch batch size
    )
    .Concurrency(4)
    .StateStore(THefestoInMemoryStateStore.New)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('process_order', TProcessOrderHandler.Create)
    .Run;

  Writeln('Waiting for jobs from SQS...');
  ReadLn;
  LServer.Stop;
end.
```

**Notes:**
- `WaitTimeSeconds(20)` enables long polling — reduces costs and latency
- Credentials via environment variables (do not hardcode)
- SQS manages DLQ natively if configured in the AWS console
- For multiple queues, create multiple separate adapters and servers
