unit Hefesto.Store.InMemory;

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  System.DateUtils,
  System.SyncObjs,
  Hefesto.Store.Interfaces;

type
  THefestoInMemoryStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    FValues: TDictionary<string, string>;
    FExpiresAt: TDictionary<string, TDateTime>;
    FLock: TCriticalSection;

    procedure DeleteUnlocked(const AKey: string);
    procedure PurgeIfExpired(const AKey: string);
    procedure PurgeAllExpired;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoStateStore;

    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

{ THefestoInMemoryStateStore }

constructor THefestoInMemoryStateStore.Create;
begin
  inherited Create;
  FValues := TDictionary<string, string>.Create;
  FExpiresAt := TDictionary<string, TDateTime>.Create;
  FLock := TCriticalSection.Create;
end;

procedure THefestoInMemoryStateStore.Delete(const AKey: string);
begin
  FLock.Acquire;
  try
    DeleteUnlocked(AKey);
  finally
    FLock.Release;
  end;
end;

procedure THefestoInMemoryStateStore.DeleteUnlocked(const AKey: string);
begin
  FValues.Remove(AKey);
  FExpiresAt.Remove(AKey);
end;

destructor THefestoInMemoryStateStore.Destroy;
begin
  FLock.Free;
  FExpiresAt.Free;
  FValues.Free;
  inherited;
end;

function THefestoInMemoryStateStore.Exists(const AKey: string): Boolean;
begin
  FLock.Acquire;
  try
    PurgeIfExpired(AKey);
    Result := FValues.ContainsKey(AKey);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryStateStore.Get(const AKey: string): string;
begin
  FLock.Acquire;
  try
    PurgeIfExpired(AKey);
    if not FValues.TryGetValue(AKey, Result) then
      Result := EmptyStr;
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryStateStore.ListKeys(const APrefix: string): TArray<string>;
var
  LKey: string;
  LKeys: TArray<string>;
  LCount: Integer;
begin
  FLock.Acquire;
  try
    PurgeAllExpired;
    LKeys := FValues.Keys.ToArray;
    SetLength(Result, Length(LKeys));
    LCount := 0;
    for LKey in LKeys do
    begin
      if (not APrefix.IsEmpty) and (not LKey.StartsWith(APrefix)) then
        Continue;
      Result[LCount] := LKey;
      Inc(LCount);
    end;
    SetLength(Result, LCount);
  finally
    FLock.Release;
  end;
end;

class function THefestoInMemoryStateStore.New: IHefestoStateStore;
begin
  Result := THefestoInMemoryStateStore.Create;
end;

procedure THefestoInMemoryStateStore.PurgeAllExpired;
var
  LKey: string;
  LKeys: TArray<string>;
begin
  LKeys := FExpiresAt.Keys.ToArray;
  for LKey in LKeys do
    PurgeIfExpired(LKey);
end;

procedure THefestoInMemoryStateStore.PurgeIfExpired(const AKey: string);
var
  LExpiresAt: TDateTime;
begin
  if FExpiresAt.TryGetValue(AKey, LExpiresAt) and (Now >= LExpiresAt) then
    DeleteUnlocked(AKey);
end;

procedure THefestoInMemoryStateStore.Put(
  const AKey, AValue: string;
  const ATtlSeconds: Integer);
begin
  FLock.Acquire;
  try
    PurgeAllExpired;
    FValues.AddOrSetValue(AKey, AValue);
    if ATtlSeconds > 0 then
      FExpiresAt.AddOrSetValue(AKey, IncSecond(Now, ATtlSeconds))
    else
      FExpiresAt.Remove(AKey);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryStateStore.TryPutIfAbsent(
  const AKey, AValue: string;
  const ATtlSeconds: Integer): Boolean;
begin
  FLock.Acquire;
  try
    PurgeIfExpired(AKey);
    Result := not FValues.ContainsKey(AKey);
    if Result then
    begin
      FValues.AddOrSetValue(AKey, AValue);
      if ATtlSeconds > 0 then
        FExpiresAt.AddOrSetValue(AKey, IncSecond(Now, ATtlSeconds))
      else
        FExpiresAt.Remove(AKey);
    end;
  finally
    FLock.Release;
  end;
end;

end.
