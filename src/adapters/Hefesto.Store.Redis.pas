unit Hefesto.Store.Redis;

// Convenience facade: wires up a THefestoRedis4DStateStore from a connection
// string or an already-built IHefestoRedisClient / IHefestoStateStore.
//
// Uso:
//   LStore := THefestoRedisStateStore.New
//     .ConnectionString('redis://127.0.0.1:6379/0');
//
//   LStore := THefestoRedisStateStore.New
//     .RedisClient(TRedis4DClientBridge.NewFromConnectionString(...));

interface

uses
  System.SysUtils,
  Hefesto.Store.Interfaces,
  Hefesto.Redis.Client;

type
  THefestoRedisStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FInner: IHefestoStateStore;
    function EnsureInner: IHefestoStateStore;
  public
    class function New: THefestoRedisStateStore;

    function ConnectionString(const AValue: string): THefestoRedisStateStore;
    function RedisClient(const AValue: IHefestoRedisClient): THefestoRedisStateStore;
    function StateStore(const AValue: IHefestoStateStore): THefestoRedisStateStore;

    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

uses
  Hefesto.Store.Redis4D,
  Hefesto.Redis4D.Client;

function THefestoRedisStateStore.EnsureInner: IHefestoStateStore;
begin
  if not Assigned(FInner) then
    raise EHefestoStateStoreNotImplemented.Create(
      'No Redis client configured. Call .ConnectionString(...) or .RedisClient(...).');
  Result := FInner;
end;

class function THefestoRedisStateStore.New: THefestoRedisStateStore;
begin
  Result := THefestoRedisStateStore.Create;
end;

function THefestoRedisStateStore.ConnectionString(
  const AValue: string): THefestoRedisStateStore;
begin
  Result := Self;
  FInner := THefestoRedis4DStateStore.New
    .RedisClient(TRedis4DClientBridge.NewFromConnectionString(AValue));
end;

function THefestoRedisStateStore.RedisClient(
  const AValue: IHefestoRedisClient): THefestoRedisStateStore;
begin
  Result := Self;
  FInner := THefestoRedis4DStateStore.New.RedisClient(AValue);
end;

function THefestoRedisStateStore.StateStore(
  const AValue: IHefestoStateStore): THefestoRedisStateStore;
begin
  Result := Self;
  FInner := AValue;
end;

function THefestoRedisStateStore.Get(const AKey: string): string;
begin
  Result := EnsureInner.Get(AKey);
end;

procedure THefestoRedisStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
begin
  EnsureInner.Put(AKey, AValue, ATtlSeconds);
end;

procedure THefestoRedisStateStore.Delete(const AKey: string);
begin
  EnsureInner.Delete(AKey);
end;

function THefestoRedisStateStore.Exists(const AKey: string): Boolean;
begin
  Result := EnsureInner.Exists(AKey);
end;

function THefestoRedisStateStore.ListKeys(const APrefix: string): TArray<string>;
begin
  Result := EnsureInner.ListKeys(APrefix);
end;

function THefestoRedisStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
begin
  Result := EnsureInner.TryPutIfAbsent(AKey, AValue, ATtlSeconds);
end;

end.
