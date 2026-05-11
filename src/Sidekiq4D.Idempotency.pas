unit Sidekiq4D.Idempotency;

interface

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Metadata,
  Sidekiq4D.Store.Interfaces;

type
  ISidekiqIdempotency = interface
    ['{D903DA58-9A62-4E2E-8477-53D20E8A0E5B}']
    function KeyForJob(const AJob: ISidekiqJobEnvelope): string;
    function IsCompleted(const AKey: string): Boolean;
    function TryBegin(const AKey: string; const ATtlSeconds: Integer = 300): Boolean;
    procedure MarkCompleted(const AKey: string; const ATtlSeconds: Integer = 86400);
    procedure Clear(const AKey: string);
  end;

  ISidekiqRenewableIdempotency = interface(ISidekiqIdempotency)
    ['{F8A27149-D028-4A8B-AF5A-8804C4A8227A}']
    procedure Renew(const AKey: string; const ATtlSeconds: Integer = 300);
  end;

  TSidekiqStateStoreIdempotency = class(
    TInterfacedObject,
    ISidekiqIdempotency,
    ISidekiqRenewableIdempotency)
  private
    FStateStore: ISidekiqStateStore;
    class function StateKey(const AKey: string): string; static;
  public
    constructor Create(const AStateStore: ISidekiqStateStore);

    class function New(const AStateStore: ISidekiqStateStore): ISidekiqIdempotency;

    function KeyForJob(const AJob: ISidekiqJobEnvelope): string;
    function IsCompleted(const AKey: string): Boolean;
    function TryBegin(const AKey: string; const ATtlSeconds: Integer = 300): Boolean;
    procedure MarkCompleted(const AKey: string; const ATtlSeconds: Integer = 86400);
    procedure Clear(const AKey: string);
    procedure Renew(const AKey: string; const ATtlSeconds: Integer = 300);
  end;

implementation

{ TSidekiqStateStoreIdempotency }

procedure TSidekiqStateStoreIdempotency.Clear(const AKey: string);
begin
  if Assigned(FStateStore) and not AKey.Trim.IsEmpty then
    FStateStore.Delete(StateKey(AKey));
end;

constructor TSidekiqStateStoreIdempotency.Create(
  const AStateStore: ISidekiqStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
end;

function TSidekiqStateStoreIdempotency.IsCompleted(const AKey: string): Boolean;
begin
  Result := Assigned(FStateStore) and SameText(FStateStore.Get(StateKey(AKey)), 'completed');
end;

function TSidekiqStateStoreIdempotency.KeyForJob(
  const AJob: ISidekiqJobEnvelope): string;
begin
  Result := AJob.Attribute(TSidekiqJobAttribute.IdempotencyKey);
end;

procedure TSidekiqStateStoreIdempotency.MarkCompleted(
  const AKey: string;
  const ATtlSeconds: Integer);
begin
  if Assigned(FStateStore) and not AKey.Trim.IsEmpty then
    FStateStore.Put(StateKey(AKey), 'completed', ATtlSeconds);
end;

procedure TSidekiqStateStoreIdempotency.Renew(
  const AKey: string;
  const ATtlSeconds: Integer);
begin
  if not Assigned(FStateStore) or AKey.Trim.IsEmpty or (ATtlSeconds <= 0) then
    Exit;

  if SameText(FStateStore.Get(StateKey(AKey)), 'processing') then
    FStateStore.Put(StateKey(AKey), 'processing', ATtlSeconds);
end;

class function TSidekiqStateStoreIdempotency.New(
  const AStateStore: ISidekiqStateStore): ISidekiqIdempotency;
var
  LIdempotency: ISidekiqRenewableIdempotency;
begin
  LIdempotency := TSidekiqStateStoreIdempotency.Create(AStateStore);
  Result := LIdempotency;
end;

class function TSidekiqStateStoreIdempotency.StateKey(const AKey: string): string;
begin
  Result := 'idempotency:' + AKey;
end;

function TSidekiqStateStoreIdempotency.TryBegin(
  const AKey: string;
  const ATtlSeconds: Integer): Boolean;
begin
  Result := Assigned(FStateStore) and not AKey.Trim.IsEmpty;
  if not Result then
    Exit(False);

  if IsCompleted(AKey) then
    Exit(False);

  Result := FStateStore.TryPutIfAbsent(StateKey(AKey), 'processing', ATtlSeconds);
end;

end.
