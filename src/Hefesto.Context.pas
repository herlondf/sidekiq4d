unit Hefesto.Context;

interface

uses
  Hefesto.Job,
  Hefesto.Store.Interfaces,
  Hefesto.Locking,
  Hefesto.Idempotency,
  Hefesto.RateLimit;

type
  IHefestoJobContext = interface
    ['{C8E113A2-84B0-4E17-9015-C1925C33326B}']
    function Job: IHefestoJobEnvelope;
    function StateStore: IHefestoStateStore;
    function LockProvider: IHefestoLockProvider;
    function Idempotency: IHefestoIdempotency;
    function RateLimiter: IHefestoRateLimiter;
  end;

  THefestoJobContext = class(TInterfacedObject, IHefestoJobContext)
  private
    FJob: IHefestoJobEnvelope;
    FStateStore: IHefestoStateStore;
    FLockProvider: IHefestoLockProvider;
    FIdempotency: IHefestoIdempotency;
    FRateLimiter: IHefestoRateLimiter;
  public
    constructor Create(
      const AJob: IHefestoJobEnvelope;
      const AStateStore: IHefestoStateStore = nil;
      const ALockProvider: IHefestoLockProvider = nil;
      const AIdempotency: IHefestoIdempotency = nil;
      const ARateLimiter: IHefestoRateLimiter = nil);
    class function New(
      const AJob: IHefestoJobEnvelope;
      const AStateStore: IHefestoStateStore = nil;
      const ALockProvider: IHefestoLockProvider = nil;
      const AIdempotency: IHefestoIdempotency = nil;
      const ARateLimiter: IHefestoRateLimiter = nil): IHefestoJobContext;

    function Job: IHefestoJobEnvelope;
    function StateStore: IHefestoStateStore;
    function LockProvider: IHefestoLockProvider;
    function Idempotency: IHefestoIdempotency;
    function RateLimiter: IHefestoRateLimiter;
  end;

implementation

{ THefestoJobContext }

constructor THefestoJobContext.Create(
  const AJob: IHefestoJobEnvelope;
  const AStateStore: IHefestoStateStore;
  const ALockProvider: IHefestoLockProvider;
  const AIdempotency: IHefestoIdempotency;
  const ARateLimiter: IHefestoRateLimiter);
begin
  inherited Create;
  FJob := AJob;
  FStateStore := AStateStore;
  FLockProvider := ALockProvider;
  FIdempotency := AIdempotency;
  FRateLimiter := ARateLimiter;
end;

function THefestoJobContext.Job: IHefestoJobEnvelope;
begin
  Result := FJob;
end;

class function THefestoJobContext.New(
  const AJob: IHefestoJobEnvelope;
  const AStateStore: IHefestoStateStore;
  const ALockProvider: IHefestoLockProvider;
  const AIdempotency: IHefestoIdempotency;
  const ARateLimiter: IHefestoRateLimiter): IHefestoJobContext;
begin
  Result := THefestoJobContext.Create(
    AJob,
    AStateStore,
    ALockProvider,
    AIdempotency,
    ARateLimiter);
end;

function THefestoJobContext.StateStore: IHefestoStateStore;
begin
  Result := FStateStore;
end;

function THefestoJobContext.LockProvider: IHefestoLockProvider;
begin
  Result := FLockProvider;
end;

function THefestoJobContext.Idempotency: IHefestoIdempotency;
begin
  Result := FIdempotency;
end;

function THefestoJobContext.RateLimiter: IHefestoRateLimiter;
begin
  Result := FRateLimiter;
end;

end.
