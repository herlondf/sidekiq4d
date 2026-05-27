# Recipe: Rate Limiting

Control the rate of calls to an external API using a token bucket.

```pascal
program RateLimiting;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.RateLimit,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

type
  TAPICallHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FRateLimiter: IHefestoRateLimiter;
  public
    constructor Create(const ARateLimiter: IHefestoRateLimiter);
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

constructor TAPICallHandler.Create(const ARateLimiter: IHefestoRateLimiter);
begin
  inherited Create;
  FRateLimiter := ARateLimiter;
end;

function TAPICallHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'api_call';
end;

procedure TAPICallHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // Try to acquire 1 token from the 'external_api' bucket
  if not FRateLimiter.TryAcquire('external_api', 1) then
    raise Exception.Create('Rate limit reached — job will be retried');

  // If we get here, we have permission to call the API
  Writeln('Calling external API: ', AJob.Body);
  // ... real HTTP call here ...
end;

var
  LStore: IHefestoStateStore;
  LRateLimiter: IHefestoRateLimiter;
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
  I: Integer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LQueue := THefestoInMemoryQueueAdapter.New;

  // Bucket: capacity 10, refills 2 tokens/second
  // Maximum: 10 calls in burst, 2 calls/second in steady state
  LRateLimiter := THefestoTokenBucketRateLimiter.New(LStore, 10, 2);

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(8)  // many workers, but rate limiter controls the rate
    .StateStore(LStore)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 10, 300))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('api_call', TAPICallHandler.Create(LRateLimiter))
    .Run;

  // Enqueue 20 jobs (only ~10 initial ones will pass immediately)
  for I := 1 to 20 do
    LQueue.Enqueue('api_call', Format('{"request_id": %d}', [I]));

  Writeln('20 jobs enqueued. Rate limit: 10 burst, 2/s.');
  ReadLn;
  LServer.Stop;
end.
```

**Rate limit per user (dynamic keys):**

```pascal
procedure TAPICallHandler.Execute(const AJob: IHefestoJobEnvelope);
var
  LUserId: string;
begin
  LUserId := ExtractUserId(AJob.Body);

  // Separate bucket per user: 5 requests/second per user
  if not FRateLimiter.TryAcquire('user:' + LUserId, 1) then
    raise Exception.Create('Per-user rate limit reached');

  ProcessRequest(AJob.Body);
end;
```

See [rate-limiting.md](../04-features/rate-limiting.md) for interface reference.
