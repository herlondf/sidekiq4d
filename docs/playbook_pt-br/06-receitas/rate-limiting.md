# Receita: Rate Limiting

Controlar a taxa de chamadas a uma API externa usando token bucket.

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
  // Tentar adquirir 1 token do bucket 'external_api'
  if not FRateLimiter.TryAcquire('external_api', 1) then
    raise Exception.Create('Rate limit atingido — job será retentado');

  // Se chegou aqui, temos permissão para chamar a API
  Writeln('Chamando API externa: ', AJob.Body);
  // ... chamada HTTP real aqui ...
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

  // Bucket: capacidade 10, reabastece 2 tokens/segundo
  // Máximo: 10 chamadas em burst, 2 chamadas/segundo em regime permanente
  LRateLimiter := THefestoTokenBucketRateLimiter.New(LStore, 10, 2);

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(8)  // muitos workers, mas rate limiter controla a taxa
    .StateStore(LStore)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 10, 300))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('api_call', TAPICallHandler.Create(LRateLimiter))
    .Run;

  // Enfileirar 20 jobs (apenas ~10 iniciais passarão imediatamente)
  for I := 1 to 20 do
    LQueue.Enqueue('api_call', Format('{"request_id": %d}', [I]));

  Writeln('20 jobs enfileirados. Rate limit: 10 burst, 2/s.');
  ReadLn;
  LServer.Stop;
end.
```

**Rate limit por usuário (chaves dinâmicas):**

```pascal
procedure TAPICallHandler.Execute(const AJob: IHefestoJobEnvelope);
var
  LUserId: string;
begin
  LUserId := ExtractUserId(AJob.Body);

  // Bucket separado por usuário: 5 requisições/segundo por usuário
  if not FRateLimiter.TryAcquire('user:' + LUserId, 1) then
    raise Exception.Create('Rate limit por usuário atingido');

  ProcessarRequisicao(AJob.Body);
end;
```

Ver [rate-limiting.md](../04-features/rate-limiting.md) para referência da interface.
