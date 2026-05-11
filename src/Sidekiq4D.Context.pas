unit Sidekiq4D.Context;

interface

uses
  Sidekiq4D.Job,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency,
  Sidekiq4D.RateLimit;

type
  ISidekiqJobContext = interface
    ['{C8E113A2-84B0-4E17-9015-C1925C33326B}']
    function Job: ISidekiqJobEnvelope;
    function StateStore: ISidekiqStateStore;
    function LockProvider: ISidekiqLockProvider;
    function Idempotency: ISidekiqIdempotency;
    function RateLimiter: ISidekiqRateLimiter;
  end;

  TSidekiqJobContext = class(TInterfacedObject, ISidekiqJobContext)
  private
    FJob: ISidekiqJobEnvelope;
    FStateStore: ISidekiqStateStore;
    FLockProvider: ISidekiqLockProvider;
    FIdempotency: ISidekiqIdempotency;
    FRateLimiter: ISidekiqRateLimiter;
  public
    constructor Create(
      const AJob: ISidekiqJobEnvelope;
      const AStateStore: ISidekiqStateStore = nil;
      const ALockProvider: ISidekiqLockProvider = nil;
      const AIdempotency: ISidekiqIdempotency = nil;
      const ARateLimiter: ISidekiqRateLimiter = nil);
    class function New(
      const AJob: ISidekiqJobEnvelope;
      const AStateStore: ISidekiqStateStore = nil;
      const ALockProvider: ISidekiqLockProvider = nil;
      const AIdempotency: ISidekiqIdempotency = nil;
      const ARateLimiter: ISidekiqRateLimiter = nil): ISidekiqJobContext;

    function Job: ISidekiqJobEnvelope;
    function StateStore: ISidekiqStateStore;
    function LockProvider: ISidekiqLockProvider;
    function Idempotency: ISidekiqIdempotency;
    function RateLimiter: ISidekiqRateLimiter;
  end;

implementation

{ TSidekiqJobContext }

constructor TSidekiqJobContext.Create(
  const AJob: ISidekiqJobEnvelope;
  const AStateStore: ISidekiqStateStore;
  const ALockProvider: ISidekiqLockProvider;
  const AIdempotency: ISidekiqIdempotency;
  const ARateLimiter: ISidekiqRateLimiter);
begin
  inherited Create;
  FJob := AJob;
  FStateStore := AStateStore;
  FLockProvider := ALockProvider;
  FIdempotency := AIdempotency;
  FRateLimiter := ARateLimiter;
end;

function TSidekiqJobContext.Job: ISidekiqJobEnvelope;
begin
  Result := FJob;
end;

class function TSidekiqJobContext.New(
  const AJob: ISidekiqJobEnvelope;
  const AStateStore: ISidekiqStateStore;
  const ALockProvider: ISidekiqLockProvider;
  const AIdempotency: ISidekiqIdempotency;
  const ARateLimiter: ISidekiqRateLimiter): ISidekiqJobContext;
begin
  Result := TSidekiqJobContext.Create(
    AJob,
    AStateStore,
    ALockProvider,
    AIdempotency,
    ARateLimiter);
end;

function TSidekiqJobContext.StateStore: ISidekiqStateStore;
begin
  Result := FStateStore;
end;

function TSidekiqJobContext.LockProvider: ISidekiqLockProvider;
begin
  Result := FLockProvider;
end;

function TSidekiqJobContext.Idempotency: ISidekiqIdempotency;
begin
  Result := FIdempotency;
end;

function TSidekiqJobContext.RateLimiter: ISidekiqRateLimiter;
begin
  Result := FRateLimiter;
end;

end.
