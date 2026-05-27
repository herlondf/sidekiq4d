unit Hefesto.Middleware.Deduplication;

interface

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Store.Interfaces,
  Hefesto.Middleware;

type
  THefestoDeduplicationMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
  private
    FStateStore: IHefestoStateStore;
    // Configuration fields: set once before first use. Thread-safe after init.
    FTtlSeconds: Integer;
    FKeyAttribute: string;
  public
    constructor Create(const AStateStore: IHefestoStateStore);
    destructor Destroy; override;

    class function New(const AStateStore: IHefestoStateStore): THefestoDeduplicationMiddleware;

    function TtlSeconds(AValue: Integer): THefestoDeduplicationMiddleware;
    function KeyAttribute(const AValue: string): THefestoDeduplicationMiddleware;

    // IHefestoServerMiddleware
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
  end;

implementation

{ THefestoDeduplicationMiddleware }

constructor THefestoDeduplicationMiddleware.Create(
  const AStateStore: IHefestoStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
  FTtlSeconds := 86400;
  FKeyAttribute := 'idempotency_key';
end;

destructor THefestoDeduplicationMiddleware.Destroy;
begin
  inherited;
end;

class function THefestoDeduplicationMiddleware.New(
  const AStateStore: IHefestoStateStore): THefestoDeduplicationMiddleware;
begin
  Result := THefestoDeduplicationMiddleware.Create(AStateStore);
end;

function THefestoDeduplicationMiddleware.TtlSeconds(
  AValue: Integer): THefestoDeduplicationMiddleware;
begin
  Result := Self;
  FTtlSeconds := AValue;
end;

function THefestoDeduplicationMiddleware.KeyAttribute(
  const AValue: string): THefestoDeduplicationMiddleware;
begin
  Result := Self;
  FKeyAttribute := AValue;
end;

procedure THefestoDeduplicationMiddleware.Call(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
var
  LKey: string;
  LStoreKey: string;
begin
  LKey := AJob.Attribute(FKeyAttribute);

  if LKey = '' then
  begin
    // No idempotency key present, proceed normally
    ANext;
    Exit;
  end;

  LStoreKey := 'dedup:' + AJob.Action + ':' + LKey;

  if not FStateStore.TryPutIfAbsent(LStoreKey, AJob.Id, FTtlSeconds) then
  begin
    // Key already exists - duplicate job, skip execution
    WriteLn(Format('[Deduplication] Skipping duplicate job %s (key=%s)',
      [AJob.Id, LKey]));
    Exit;
  end;

  ANext;
end;

end.
