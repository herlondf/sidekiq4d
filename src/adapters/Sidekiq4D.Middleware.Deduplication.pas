unit Sidekiq4D.Middleware.Deduplication;

interface

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Middleware;

type
  TSidekiqDeduplicationMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
  private
    FStateStore: ISidekiqStateStore;
    // Configuration fields: set once before first use. Thread-safe after init.
    FTtlSeconds: Integer;
    FKeyAttribute: string;
  public
    constructor Create(const AStateStore: ISidekiqStateStore);
    destructor Destroy; override;

    class function New(const AStateStore: ISidekiqStateStore): TSidekiqDeduplicationMiddleware;

    function TtlSeconds(AValue: Integer): TSidekiqDeduplicationMiddleware;
    function KeyAttribute(const AValue: string): TSidekiqDeduplicationMiddleware;

    // ISidekiqServerMiddleware
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
  end;

implementation

{ TSidekiqDeduplicationMiddleware }

constructor TSidekiqDeduplicationMiddleware.Create(
  const AStateStore: ISidekiqStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
  FTtlSeconds := 86400;
  FKeyAttribute := 'idempotency_key';
end;

destructor TSidekiqDeduplicationMiddleware.Destroy;
begin
  inherited;
end;

class function TSidekiqDeduplicationMiddleware.New(
  const AStateStore: ISidekiqStateStore): TSidekiqDeduplicationMiddleware;
begin
  Result := TSidekiqDeduplicationMiddleware.Create(AStateStore);
end;

function TSidekiqDeduplicationMiddleware.TtlSeconds(
  AValue: Integer): TSidekiqDeduplicationMiddleware;
begin
  Result := Self;
  FTtlSeconds := AValue;
end;

function TSidekiqDeduplicationMiddleware.KeyAttribute(
  const AValue: string): TSidekiqDeduplicationMiddleware;
begin
  Result := Self;
  FKeyAttribute := AValue;
end;

procedure TSidekiqDeduplicationMiddleware.Call(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
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
