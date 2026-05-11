unit Sidekiq4D.Store.FireDAC;

interface

uses
  System.SysUtils,
  System.DateUtils,
  System.SyncObjs,
  FireDAC.Comp.Client,
  FireDAC.Stan.Param,
  FireDAC.Stan.Def,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.Postgres;

type
  // Backend FireDAC generico para ISidekiqPostgresStateStoreBackend.
  // Funciona com Postgres, SQLite, MySQL, SQL Server e qualquer driver FireDAC.
  //
  // Uso com Postgres:
  //   TSidekiqPostgresStateStore.New
  //     .Backend(TSidekiqFireDACBackend.New(FDConnection))
  //
  // Uso com SQLite:
  //   TSidekiqPostgresStateStore.New
  //     .Backend(TSidekiqFireDACBackend.New(FDConnection))
  //     .TableName('sidekiq_state')
  TSidekiqFireDACBackend = class(TInterfacedObject, ISidekiqPostgresStateStoreBackend)
  private
    FLock: TCriticalSection;
    FConnection: TFDConnection;
    FOwnsConnection: Boolean;
    FTableCreated: Boolean;
    FLastPurge: TDateTime;
    FLastVacuum: TDateTime;
    FIsSQLite: Boolean;
    // Called only while FLock is held by the caller.
    procedure EnsureTable(const ATableName: string);
    procedure PurgeExpiredIfDue(const ATableName: string);
  public
    constructor Create(const AConnection: TFDConnection; AOwnsConnection: Boolean = False);
    destructor Destroy; override;

    class function New(const AConnection: TFDConnection;
      AOwnsConnection: Boolean = False): ISidekiqPostgresStateStoreBackend; static;

    // Cria e retorna uma FDConnection pronta para SQLite em arquivo
    class function NewSQLiteConnection(const AFilePath: string): TFDConnection; static;

    // Cria e retorna uma FDConnection pronta para Postgres
    class function NewPostgresConnection(
      const AHost: string; APort: Integer;
      const ADatabase, AUser, APassword: string): TFDConnection; static;

    function GetEntry(const ATableName, AKey: string;
      out AEntry: TSidekiqPostgresStateEntry): Boolean;
    procedure PutEntry(const ATableName: string;
      const AEntry: TSidekiqPostgresStateEntry);
    procedure DeleteEntry(const ATableName, AKey: string);
    function ListEntries(const ATableName, APrefix: string): TArray<TSidekiqPostgresStateEntry>;
    function TryPutIfAbsentOrExpired(const ATableName: string;
      const AEntry: TSidekiqPostgresStateEntry;
      const AReferenceTime: TDateTime): Boolean;
  end;

implementation

uses
  FireDAC.Stan.Option,
  FireDAC.DApt,
  FireDAC.Phys.SQLite,
  FireDAC.Phys.PG;

{ TSidekiqFireDACBackend }

constructor TSidekiqFireDACBackend.Create(
  const AConnection: TFDConnection; AOwnsConnection: Boolean);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FConnection := AConnection;
  FOwnsConnection := AOwnsConnection;
  FTableCreated := False;
  FIsSQLite := SameText(AConnection.DriverName, 'SQLite');
  FLastPurge := 0;
  FLastVacuum := 0;
end;

destructor TSidekiqFireDACBackend.Destroy;
begin
  if FOwnsConnection then
    FConnection.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqFireDACBackend.New(
  const AConnection: TFDConnection;
  AOwnsConnection: Boolean): ISidekiqPostgresStateStoreBackend;
begin
  Result := TSidekiqFireDACBackend.Create(AConnection, AOwnsConnection);
end;

class function TSidekiqFireDACBackend.NewSQLiteConnection(
  const AFilePath: string): TFDConnection;
begin
  Result := TFDConnection.Create(nil);
  Result.DriverName := 'SQLite';
  Result.Params.Values['Database'] := AFilePath;
  Result.Params.Values['LockingMode'] := 'Normal';
  Result.LoginPrompt := False;
  Result.Connected := True;
end;

class function TSidekiqFireDACBackend.NewPostgresConnection(
  const AHost: string; APort: Integer;
  const ADatabase, AUser, APassword: string): TFDConnection;
begin
  Result := TFDConnection.Create(nil);
  Result.DriverName := 'PG';
  Result.Params.Values['Server'] := AHost;
  Result.Params.Values['Port'] := IntToStr(APort);
  Result.Params.Values['Database'] := ADatabase;
  Result.Params.Values['User_Name'] := AUser;
  Result.Params.Values['Password'] := APassword;
  Result.Params.Values['MetaDefSchema'] := 'public';
  Result.LoginPrompt := False;
  Result.Connected := True;
  Result.ExecSQL('SET search_path TO public');
end;

procedure TSidekiqFireDACBackend.EnsureTable(const ATableName: string);
var
  LQualified: string;
begin
  if FTableCreated then
    Exit;
  if SameText(FConnection.DriverName, 'PG') then
    LQualified := 'public.' + ATableName
  else
    LQualified := ATableName;
  FConnection.ExecSQL(Format(
    'CREATE TABLE IF NOT EXISTS %s (' +
    '  key TEXT PRIMARY KEY,' +
    '  value TEXT NOT NULL,' +
    '  has_expiration INTEGER NOT NULL DEFAULT 0,' +
    '  expires_at TEXT' +
    ')', [LQualified]));
  // Index for efficient expiry cleanup — avoids full table scan on PurgeExpired
  FConnection.ExecSQL(Format(
    'CREATE INDEX IF NOT EXISTS idx_%s_exp ON %s(expires_at) ' +
    'WHERE has_expiration = 1',
    [ATableName, LQualified]));
  FTableCreated := True;
end;

procedure TSidekiqFireDACBackend.PurgeExpiredIfDue(const ATableName: string);
const
  PurgeIntervalHours = 1;
  VacuumIntervalHours = 24;
var
  LNow: TDateTime;
  LNowISO: string;
begin
  LNow := Now;
  if (FLastPurge > 0) and (LNow - FLastPurge < PurgeIntervalHours / 24) then
    Exit;

  LNowISO := DateToISO8601(LNow);
  FConnection.ExecSQL(Format(
    'DELETE FROM %s WHERE has_expiration = 1 AND expires_at <= :now',
    [ATableName]),
    [LNowISO]);
  FLastPurge := LNow;

  // SQLite-only: reclaim disk space after bulk deletes
  if FIsSQLite and ((FLastVacuum = 0) or (LNow - FLastVacuum >= VacuumIntervalHours / 24)) then
  begin
    FConnection.ExecSQL('PRAGMA incremental_vacuum');
    FLastVacuum := LNow;
  end;
end;

function TSidekiqFireDACBackend.GetEntry(const ATableName, AKey: string;
  out AEntry: TSidekiqPostgresStateEntry): Boolean;
var
  LQuery: TFDQuery;
begin
  FLock.Acquire;
  try
    EnsureTable(ATableName);
    Result := False;
    LQuery := TFDQuery.Create(nil);
    try
      LQuery.Connection := FConnection;
      LQuery.SQL.Text := Format(
        'SELECT key, value, has_expiration, expires_at FROM %s WHERE key = :key',
        [ATableName]);
      LQuery.ParamByName('key').AsString := AKey;
      LQuery.Open;
      if not LQuery.Eof then
      begin
        AEntry.Key := LQuery.FieldByName('key').AsString;
        AEntry.Value := LQuery.FieldByName('value').AsString;
        AEntry.HasExpiration := LQuery.FieldByName('has_expiration').AsInteger = 1;
        if AEntry.HasExpiration and (not LQuery.FieldByName('expires_at').IsNull) then
          AEntry.ExpiresAt := ISO8601ToDate(LQuery.FieldByName('expires_at').AsString)
        else
          AEntry.ExpiresAt := 0;
        Result := True;
      end;
    finally
      LQuery.Free;
    end;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqFireDACBackend.PutEntry(const ATableName: string;
  const AEntry: TSidekiqPostgresStateEntry);
var
  LExpiresAt: string;
  LSQL: string;
begin
  FLock.Acquire;
  try
    EnsureTable(ATableName);
    if AEntry.HasExpiration then
      LExpiresAt := DateToISO8601(AEntry.ExpiresAt)
    else
      LExpiresAt := '';

    // Atomic upsert: single statement, no gap between DELETE and INSERT.
    // SQLite >= 3.24 (2018) and Postgres >= 9.5 both support ON CONFLICT syntax.
    if FIsSQLite then
      LSQL :=
        'INSERT INTO %s (key, value, has_expiration, expires_at) VALUES (:key, :value, :has_exp, :exp_at) ' +
        'ON CONFLICT(key) DO UPDATE SET ' +
        '  value = excluded.value,' +
        '  has_expiration = excluded.has_expiration,' +
        '  expires_at = excluded.expires_at'
    else
      LSQL :=
        'INSERT INTO %s (key, value, has_expiration, expires_at) VALUES (:key, :value, :has_exp, :exp_at) ' +
        'ON CONFLICT(key) DO UPDATE SET ' +
        '  value = EXCLUDED.value,' +
        '  has_expiration = EXCLUDED.has_expiration,' +
        '  expires_at = EXCLUDED.expires_at';

    FConnection.ExecSQL(Format(LSQL, [ATableName]),
      [AEntry.Key, AEntry.Value, Ord(AEntry.HasExpiration), LExpiresAt]);
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqFireDACBackend.DeleteEntry(const ATableName, AKey: string);
begin
  FLock.Acquire;
  try
    EnsureTable(ATableName);
    FConnection.ExecSQL(Format('DELETE FROM %s WHERE key = :key', [ATableName]),
      [AKey]);
  finally
    FLock.Release;
  end;
end;

function TSidekiqFireDACBackend.ListEntries(
  const ATableName, APrefix: string): TArray<TSidekiqPostgresStateEntry>;
var
  LQuery: TFDQuery;
  LList: TArray<TSidekiqPostgresStateEntry>;
  LCount: Integer;
begin
  FLock.Acquire;
  try
    EnsureTable(ATableName);
    PurgeExpiredIfDue(ATableName);
    LQuery := TFDQuery.Create(nil);
    try
      LQuery.Connection := FConnection;
      if APrefix.IsEmpty then
        LQuery.SQL.Text := Format('SELECT key, value, has_expiration, expires_at FROM %s', [ATableName])
      else
      begin
        LQuery.SQL.Text := Format(
          'SELECT key, value, has_expiration, expires_at FROM %s WHERE key LIKE :prefix',
          [ATableName]);
        LQuery.ParamByName('prefix').AsString := APrefix + '%';
      end;
      LQuery.Open;
      SetLength(LList, 100);
      LCount := 0;
      while not LQuery.Eof do
      begin
        if LCount >= Length(LList) then
          SetLength(LList, LCount + 100);
        LList[LCount].Key := LQuery.FieldByName('key').AsString;
        LList[LCount].Value := LQuery.FieldByName('value').AsString;
        LList[LCount].HasExpiration := LQuery.FieldByName('has_expiration').AsInteger = 1;
        if LList[LCount].HasExpiration and (not LQuery.FieldByName('expires_at').IsNull) then
          LList[LCount].ExpiresAt := ISO8601ToDate(LQuery.FieldByName('expires_at').AsString)
        else
          LList[LCount].ExpiresAt := 0;
        Inc(LCount);
        LQuery.Next;
      end;
      SetLength(LList, LCount);
      Result := LList;
    finally
      LQuery.Free;
    end;
  finally
    FLock.Release;
  end;
end;

function TSidekiqFireDACBackend.TryPutIfAbsentOrExpired(
  const ATableName: string;
  const AEntry: TSidekiqPostgresStateEntry;
  const AReferenceTime: TDateTime): Boolean;
var
  LExisting: TSidekiqPostgresStateEntry;
  LFound: Boolean;
  LQuery: TFDQuery;
  LExpiresAt: string;
  LSQL: string;
begin
  // Holds FLock for the whole check-then-act sequence.
  // Does NOT call the public GetEntry/DeleteEntry/PutEntry to avoid re-entering
  // the non-reentrant TCriticalSection.
  FLock.Acquire;
  try
    EnsureTable(ATableName);

    // --- inline GetEntry ---
    LFound := False;
    LQuery := TFDQuery.Create(nil);
    try
      LQuery.Connection := FConnection;
      LQuery.SQL.Text := Format(
        'SELECT key, value, has_expiration, expires_at FROM %s WHERE key = :key',
        [ATableName]);
      LQuery.ParamByName('key').AsString := AEntry.Key;
      LQuery.Open;
      if not LQuery.Eof then
      begin
        LExisting.Key := LQuery.FieldByName('key').AsString;
        LExisting.Value := LQuery.FieldByName('value').AsString;
        LExisting.HasExpiration := LQuery.FieldByName('has_expiration').AsInteger = 1;
        if LExisting.HasExpiration and (not LQuery.FieldByName('expires_at').IsNull) then
          LExisting.ExpiresAt := ISO8601ToDate(LQuery.FieldByName('expires_at').AsString)
        else
          LExisting.ExpiresAt := 0;
        LFound := True;
      end;
    finally
      LQuery.Free;
    end;

    if LFound then
    begin
      if not LExisting.IsExpired(AReferenceTime) then
        Exit(False);
      // --- inline DeleteEntry ---
      FConnection.ExecSQL(Format('DELETE FROM %s WHERE key = :key', [ATableName]),
        [AEntry.Key]);
    end;

    // --- inline PutEntry ---
    if AEntry.HasExpiration then
      LExpiresAt := DateToISO8601(AEntry.ExpiresAt)
    else
      LExpiresAt := '';
    if FIsSQLite then
      LSQL :=
        'INSERT INTO %s (key, value, has_expiration, expires_at) VALUES (:key, :value, :has_exp, :exp_at) ' +
        'ON CONFLICT(key) DO UPDATE SET ' +
        '  value = excluded.value,' +
        '  has_expiration = excluded.has_expiration,' +
        '  expires_at = excluded.expires_at'
    else
      LSQL :=
        'INSERT INTO %s (key, value, has_expiration, expires_at) VALUES (:key, :value, :has_exp, :exp_at) ' +
        'ON CONFLICT(key) DO UPDATE SET ' +
        '  value = EXCLUDED.value,' +
        '  has_expiration = EXCLUDED.has_expiration,' +
        '  expires_at = EXCLUDED.expires_at';
    FConnection.ExecSQL(Format(LSQL, [ATableName]),
      [AEntry.Key, AEntry.Value, Ord(AEntry.HasExpiration), LExpiresAt]);

    Result := True;
  finally
    FLock.Release;
  end;
end;

end.
