unit Hefesto.Idempotency;

interface

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Metadata,
  Hefesto.Store.Interfaces;

type
  IHefestoIdempotency = interface
    ['{D903DA58-9A62-4E2E-8477-53D20E8A0E5B}']
    function KeyForJob(const AJob: IHefestoJobEnvelope): string;
    function IsCompleted(const AKey: string): Boolean;
    function TryBegin(const AKey: string; const ATtlSeconds: Integer = 300): Boolean;
    procedure MarkCompleted(const AKey: string; const ATtlSeconds: Integer = 86400);
    procedure Clear(const AKey: string);
  end;

  IHefestoRenewableIdempotency = interface(IHefestoIdempotency)
    ['{F8A27149-D028-4A8B-AF5A-8804C4A8227A}']
    procedure Renew(const AKey: string; const ATtlSeconds: Integer = 300);
  end;

  THefestoStateStoreIdempotency = class(
    TInterfacedObject,
    IHefestoIdempotency,
    IHefestoRenewableIdempotency)
  private
    FStateStore: IHefestoStateStore;
    class function StateKey(const AKey: string): string; static;
  public
    constructor Create(const AStateStore: IHefestoStateStore);

    class function New(const AStateStore: IHefestoStateStore): IHefestoIdempotency;

    function KeyForJob(const AJob: IHefestoJobEnvelope): string;
    function IsCompleted(const AKey: string): Boolean;
    function TryBegin(const AKey: string; const ATtlSeconds: Integer = 300): Boolean;
    procedure MarkCompleted(const AKey: string; const ATtlSeconds: Integer = 86400);
    procedure Clear(const AKey: string);
    procedure Renew(const AKey: string; const ATtlSeconds: Integer = 300);
  end;

implementation

{ THefestoStateStoreIdempotency }

procedure THefestoStateStoreIdempotency.Clear(const AKey: string);
begin
  if Assigned(FStateStore) and not AKey.Trim.IsEmpty then
    FStateStore.Delete(StateKey(AKey));
end;

constructor THefestoStateStoreIdempotency.Create(
  const AStateStore: IHefestoStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
end;

function THefestoStateStoreIdempotency.IsCompleted(const AKey: string): Boolean;
begin
  Result := Assigned(FStateStore) and SameText(FStateStore.Get(StateKey(AKey)), 'completed');
end;

function THefestoStateStoreIdempotency.KeyForJob(
  const AJob: IHefestoJobEnvelope): string;
begin
  Result := AJob.Attribute(THefestoJobAttribute.IdempotencyKey);
end;

procedure THefestoStateStoreIdempotency.MarkCompleted(
  const AKey: string;
  const ATtlSeconds: Integer);
begin
  if Assigned(FStateStore) and not AKey.Trim.IsEmpty then
    FStateStore.Put(StateKey(AKey), 'completed', ATtlSeconds);
end;

procedure THefestoStateStoreIdempotency.Renew(
  const AKey: string;
  const ATtlSeconds: Integer);
begin
  if not Assigned(FStateStore) or AKey.Trim.IsEmpty or (ATtlSeconds <= 0) then
    Exit;

  if SameText(FStateStore.Get(StateKey(AKey)), 'processing') then
    FStateStore.Put(StateKey(AKey), 'processing', ATtlSeconds);
end;

class function THefestoStateStoreIdempotency.New(
  const AStateStore: IHefestoStateStore): IHefestoIdempotency;
var
  LIdempotency: IHefestoRenewableIdempotency;
begin
  LIdempotency := THefestoStateStoreIdempotency.Create(AStateStore);
  Result := LIdempotency;
end;

class function THefestoStateStoreIdempotency.StateKey(const AKey: string): string;
begin
  Result := 'idempotency:' + AKey;
end;

function THefestoStateStoreIdempotency.TryBegin(
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
