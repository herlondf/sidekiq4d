unit Hefesto.Store.Postgres;

interface

uses
  System.SysUtils,
  System.DateUtils,
  Hefesto.Store.Interfaces;

type
  THefestoPostgresStateEntry = record
    Key: string;
    Value: string;
    ExpiresAt: TDateTime;
    HasExpiration: Boolean;
    class function Create(
      const AKey, AValue: string;
      const AHasExpiration: Boolean;
      const AExpiresAt: TDateTime): THefestoPostgresStateEntry; static;
    function IsExpired(const AReferenceTime: TDateTime): Boolean;
  end;

  IHefestoPostgresStateStoreBackend = interface
    ['{F9C56C40-0D96-470D-BDBC-4FD7EEEA3D9C}']
    function GetEntry(
      const ATableName, AKey: string;
      out AEntry: THefestoPostgresStateEntry): Boolean;
    procedure PutEntry(
      const ATableName: string;
      const AEntry: THefestoPostgresStateEntry);
    procedure DeleteEntry(const ATableName, AKey: string);
    function ListEntries(
      const ATableName, APrefix: string): TArray<THefestoPostgresStateEntry>;
    function TryPutIfAbsentOrExpired(
      const ATableName: string;
      const AEntry: THefestoPostgresStateEntry;
      const AReferenceTime: TDateTime): Boolean;
  end;

  THefestoPostgresStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    // Configuration fields: set once before first use. Thread-safe after init.
    FConnectionString: string;
    FTableName: string;
    FBackend: IHefestoPostgresStateStoreBackend;
    function BuildEntry(
      const AKey, AValue: string;
      const ATtlSeconds: Integer): THefestoPostgresStateEntry;
    function EntryIsAlive(
      const AEntry: THefestoPostgresStateEntry): Boolean;
    procedure EnsureConfigured;
    procedure PurgeExpiredKey(const AKey: string);
  public
    constructor Create;

    class function New: THefestoPostgresStateStore;

    function ConnectionString(const AValue: string): THefestoPostgresStateStore;
    function Backend(
      const AValue: IHefestoPostgresStateStoreBackend): THefestoPostgresStateStore;
    function TableName(const AValue: string): THefestoPostgresStateStore;

    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

class function THefestoPostgresStateEntry.Create(
  const AKey, AValue: string;
  const AHasExpiration: Boolean;
  const AExpiresAt: TDateTime): THefestoPostgresStateEntry;
begin
  Result.Key := AKey;
  Result.Value := AValue;
  Result.HasExpiration := AHasExpiration;
  Result.ExpiresAt := AExpiresAt;
end;

function THefestoPostgresStateEntry.IsExpired(
  const AReferenceTime: TDateTime): Boolean;
begin
  Result := HasExpiration and (AReferenceTime >= ExpiresAt);
end;

function THefestoPostgresStateStore.Backend(
  const AValue: IHefestoPostgresStateStoreBackend): THefestoPostgresStateStore;
begin
  Result := Self;
  FBackend := AValue;
end;

function THefestoPostgresStateStore.BuildEntry(
  const AKey, AValue: string;
  const ATtlSeconds: Integer): THefestoPostgresStateEntry;
begin
  Result := THefestoPostgresStateEntry.Create(
    AKey,
    AValue,
    ATtlSeconds > 0,
    IncSecond(Now, ATtlSeconds));
end;

function THefestoPostgresStateStore.ConnectionString(
  const AValue: string): THefestoPostgresStateStore;
begin
  Result := Self;
  FConnectionString := AValue.Trim;
end;

constructor THefestoPostgresStateStore.Create;
begin
  inherited Create;
  FTableName := 'sidekiq_state';
end;

procedure THefestoPostgresStateStore.Delete(const AKey: string);
begin
  EnsureConfigured;
  FBackend.DeleteEntry(FTableName, AKey);
end;

function THefestoPostgresStateStore.EntryIsAlive(
  const AEntry: THefestoPostgresStateEntry): Boolean;
begin
  Result := not AEntry.IsExpired(Now);
end;

procedure THefestoPostgresStateStore.EnsureConfigured;
begin
  if not Assigned(FBackend) then
    raise EHefestoStateStoreNotImplemented.CreateFmt(
      'Postgres state store backend not configured. Connection=%s Table=%s',
      [FConnectionString, FTableName]);
end;

function THefestoPostgresStateStore.Exists(const AKey: string): Boolean;
var
  LEntry: THefestoPostgresStateEntry;
begin
  EnsureConfigured;
  Result := FBackend.GetEntry(FTableName, AKey, LEntry) and EntryIsAlive(LEntry);
  if not Result then
    PurgeExpiredKey(AKey);
end;

function THefestoPostgresStateStore.Get(const AKey: string): string;
var
  LEntry: THefestoPostgresStateEntry;
begin
  EnsureConfigured;
  if FBackend.GetEntry(FTableName, AKey, LEntry) and EntryIsAlive(LEntry) then
    Exit(LEntry.Value);
  PurgeExpiredKey(AKey);
  Result := EmptyStr;
end;

function THefestoPostgresStateStore.ListKeys(const APrefix: string): TArray<string>;
var
  LEntries: TArray<THefestoPostgresStateEntry>;
  LEntry: THefestoPostgresStateEntry;
  LCount: Integer;
begin
  EnsureConfigured;
  LEntries := FBackend.ListEntries(FTableName, APrefix);
  SetLength(Result, Length(LEntries));
  LCount := 0;
  for LEntry in LEntries do
  begin
    if not EntryIsAlive(LEntry) then
    begin
      FBackend.DeleteEntry(FTableName, LEntry.Key);
      Continue;
    end;
    Result[LCount] := LEntry.Key;
    Inc(LCount);
  end;
  SetLength(Result, LCount);
end;

class function THefestoPostgresStateStore.New: THefestoPostgresStateStore;
begin
  Result := THefestoPostgresStateStore.Create;
end;

procedure THefestoPostgresStateStore.Put(
  const AKey, AValue: string;
  const ATtlSeconds: Integer);
begin
  EnsureConfigured;
  FBackend.PutEntry(FTableName, BuildEntry(AKey, AValue, ATtlSeconds));
end;

procedure THefestoPostgresStateStore.PurgeExpiredKey(const AKey: string);
var
  LEntry: THefestoPostgresStateEntry;
begin
  if FBackend.GetEntry(FTableName, AKey, LEntry) and LEntry.IsExpired(Now) then
    FBackend.DeleteEntry(FTableName, AKey);
end;

function THefestoPostgresStateStore.TableName(
  const AValue: string): THefestoPostgresStateStore;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FTableName := AValue.Trim;
end;

function THefestoPostgresStateStore.TryPutIfAbsent(
  const AKey, AValue: string;
  const ATtlSeconds: Integer): Boolean;
begin
  EnsureConfigured;
  Result := FBackend.TryPutIfAbsentOrExpired(
    FTableName,
    BuildEntry(AKey, AValue, ATtlSeconds),
    Now);
end;

end.
