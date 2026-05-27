# Recipe: OTLP Telemetry with Jaeger

Send OpenTelemetry traces to Jaeger for complete job observability.

## Prerequisites

Start Jaeger via Docker:

```bash
cd docker
docker-compose up -d jaeger
```

Or directly:
```bash
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

- Jaeger UI: `http://localhost:16686`
- OTLP HTTP: `http://localhost:4318`

## Code

```pascal
program OTLPTelemetry;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Telemetry.Console,
  Hefesto.Telemetry.OTLP;

type
  TProcessHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TFailHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TProcessHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process';
end;

procedure TProcessHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Sleep(Random(200) + 50); // simulate variable latency
  Writeln('Processed: ', AJob.JobId);
end;

function TFailHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'fail';
end;

procedure TFailHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  raise Exception.Create('Intentional error for failure trace');
end;

var
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
  I: Integer;
begin
  Randomize;
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(4)
    .StateStore(THefestoInMemoryStateStore.New)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(3, 5, 60))
    .Telemetry(
      // Chain console + OTLP
      THefestoCompositeTelemetry.New([
        THefestoConsoleTelemetry.New,
        THefestoOTLPTraceTelemetry.New(
          'http://localhost:4318',  // OTLP HTTP endpoint
          'sidekiq4d-demo'          // service.name
        )
      ])
    )
    .RegisterHandler('process', TProcessHandler.Create)
    .RegisterHandler('fail',    TFailHandler.Create)
    .Run;

  // Enqueue success and failure jobs to generate varied traces
  for I := 1 to 10 do
    LQueue.Enqueue('process', Format('{"id": %d}', [I]));

  for I := 1 to 3 do
    LQueue.Enqueue('fail', Format('{"id": %d}', [I]));

  Writeln('Jobs enqueued. Access http://localhost:16686 to see traces.');
  ReadLn;
  LServer.Stop;
end.
```

## Verifying in Jaeger

1. Access `http://localhost:16686`
2. Select `sidekiq4d-demo` under "Service"
3. Click "Find Traces"
4. Each job appears as a span with `job.action`, `job.queue`, `job.id`
5. Failed jobs appear with ERROR status and the exception message

## Troubleshooting

**Traces do not appear:**
- Check if Jaeger is running: `docker ps | grep jaeger`
- Check the endpoint: `curl -X POST http://localhost:4318/v1/traces`
- Check Content-Type: must be `application/json`
- Check timezone: `DateTimeToUnix` with second parameter `False`

See [troubleshooting.md](../05-operations-and-runtime/troubleshooting.md) for more details.
