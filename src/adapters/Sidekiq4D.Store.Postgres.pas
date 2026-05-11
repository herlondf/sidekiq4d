unit Sidekiq4D.Store.Postgres;

interface

uses
  System.SysUtils,
  System.DateUtils,
  Sidekiq4D.Store.Interfaces;

type
  TSidekiqPostgresStateEntry = record
    Key: string;
    Value: string;
    ExpiresAt: TDateTime;
    HasExpiration: Boolean;
    class function Create(
      const AKey, AValue: string;
      const AHasExpiration: Boolean;
      const AExpiresAt: TDateTime): TSidekiqPostgresStateEntry; static;
    function IsExpired(const AReferenceTime: TDateTime): Boolean;
  end;

  ISidekiqPostgresStateStoreBackend = interface
    ['{F9C56C40-0D96-470D-BDBC-4FD7EEEA3D9C}']
    function GetEntry(
      const ATableName, AKey: string;
      out AEntry: TSidekiqPostgresStateEntry): Boolean;
    procedure PutEntry(
      const ATableName: string;
      const AEntry: TSidekiqPostgresStateEntry);
    procedure DeleteEntry(const ATableName, AKey: string);
    function ListEntries(
      const ATableName, APrefix: string): TArray<TSidekiqPostgresStateEntry>;
    function TryPutIfAbsentOrExpired(
      const ATableName: string;
      const AEntry: TSidekiqPostgresStateEntry;
      const AReferenceTime: TDateTime): Boolean;
  end;

  TSidekiqPostgresStateStore = class(TInterfacedObject, ISidekiqStateStore)
  private
    // Configuration fields: set once before first use. Thread-safe after init.
    FConnectionString: string;
    FTableName: string;
    FBackend: ISidekiqPostgresStateStoreBackend;
    function BuildEntry(
      const AKey, AValue: string;
      const ATtlSeconds: Integer): TSidekiqPostgresStateEntry;
    function EntryIsAlive(
      const AEntry: TSidekiqPostgresStateEntry): Boolean;
    procedure EnsureConfigured;
    procedure PurgeExpiredKey(const AKey: string);
  public
    constructor Create;

    class function New: TSidekiqPostgresStateStore;

    function ConnectionString(const AValue: string): TSidekiqPostgresStateStore;
    function Backend(
      const AValue: ISidekiqPostgresStateStoreBackend): TSidekiqPostgresStateStore;
    function TableName(const AValue: string): TSidekiqPostgresStateStore;

    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

class function TSidekiqPostgresStateEntry.Create(
  const AKey, AValue: string;
  const AHasExpiration: Boolean;
  const AExpiresAt: TDateTime): TSidekiqPostgresStateEntry;
begin
  Result.Key := AKey;
  Result.Value := AValue;
  Result.HasExpiration := AHasExpiration;
  Result.ExpiresAt := AExpiresAt;
end;

function TSidekiqPostgresStateEntry.IsExpired(
  const AReferenceTime: TDateTime): Boolean;
begin
  Result := HasExpiration and (AReferenceTime >= ExpiresAt);
end;

function TSidekiqPostgresStateStore.Backend(
  const AValue: ISidekiqPostgresStateStoreBackend): TSidekiqPostgresStateStore;
begin
  Result := Self;
  FBackend := AValue;
end;

function TSidekiqPostgresStateStore.BuildEntry(
  const AKey, AValue: string;
  const ATtlSeconds: Integer): TSidekiqPostgresStateEntry;
begin
  Result := TSidekiqPostgresStateEntry.Create(
    AKey,
    AValue,
    ATtlSeconds > 0,
    IncSecond(Now, ATtlSeconds));
end;

function TSidekiqPostgresStateStore.ConnectionString(
  const AValue: string): TSidekiqPostgresStateStore;
begin
  Result := Self;
  FConnectionString := AValue.Trim;
end;

constructor TSidekiqPostgresStateStore.Create;
begin
  inherited Create;
  FTableName := 'sidekiq_state';
end;

procedure TSidekiqPostgresStateStore.Delete(const AKey: string);
begin
  EnsureConfigured;
  FBackend.DeleteEntry(FTableName, AKey);
end;

function TSidekiqPostgresStateStore.EntryIsAlive(
  const AEntry: TSidekiqPostgresStateEntry): Boolean;
begin
  Result := not AEntry.IsExpired(Now);
end;

procedure TSidekiqPostgresStateStore.EnsureConfigured;
begin
  if not Assigned(FBackend) then
    raise ESidekiqStateStoreNotImplemented.CreateFmt(
      'Postgres state store backend not configured. Connection=%s Table=%s',
      [FConnectionString, FTableName]);
end;

function TSidekiqPostgresStateStore.Exists(const AKey: string): Boolean;
var
  LEntry: TSidekiqPostgresStateEntry;
begin
  EnsureConfigured;
  Result := FBackend.GetEntry(FTableName, AKey, LEntry) and EntryIsAlive(LEntry);
  if not Result then
    PurgeExpiredKey(AKey);
end;

function TSidekiqPostgresStateStore.Get(const AKey: string): string;
var
  LEntry: TSidekiqPostgresStateEntry;
begin
  EnsureConfigured;
  if FBackend.GetEntry(FTableName, AKey, LEntry) and EntryIsAlive(LEntry) then
    Exit(LEntry.Value);
  PurgeExpiredKey(AKey);
  Result := EmptyStr;
end;

function TSidekiqPostgresStateStore.ListKeys(const APrefix: string): TArray<string>;
var
  LEntries: TArray<TSidekiqPostgresStateEntry>;
  LEntry: TSidekiqPostgresStateEntry;
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

class function TSidekiqPostgresStateStore.New: TSidekiqPostgresStateStore;
begin
  Result := TSidekiqPostgresStateStore.Create;
end;

procedure TSidekiqPostgresStateStore.Put(
  const AKey, AValue: string;
  const ATtlSeconds: Integer);
begin
  EnsureConfigured;
  FBackend.PutEntry(FTableName, BuildEntry(AKey, AValue, ATtlSeconds));
end;

procedure TSidekiqPostgresStateStore.PurgeExpiredKey(const AKey: string);
var
  LEntry: TSidekiqPostgresStateEntry;
begin
  if FBackend.GetEntry(FTableName, AKey, LEntry) and LEntry.IsExpired(Now) then
    FBackend.DeleteEntry(FTableName, AKey);
end;

function TSidekiqPostgresStateStore.TableName(
  const AValue: string): TSidekiqPostgresStateStore;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FTableName := AValue.Trim;
end;

function TSidekiqPostgresStateStore.TryPutIfAbsent(
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
