unit Sidekiq4D.Store.Redis;

// Convenience facade: wires up a TSidekiqRedis4DStateStore from a connection
// string or an already-built ISidekiqRedisClient / ISidekiqStateStore.
//
// Uso:
//   LStore := TSidekiqRedisStateStore.New
//     .ConnectionString('redis://127.0.0.1:6379/0');
//
//   LStore := TSidekiqRedisStateStore.New
//     .RedisClient(TRedis4DClientBridge.NewFromConnectionString(...));

interface

uses
  System.SysUtils,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Redis.Client;

type
  TSidekiqRedisStateStore = class(TInterfacedObject, ISidekiqStateStore)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FInner: ISidekiqStateStore;
    function EnsureInner: ISidekiqStateStore;
  public
    class function New: TSidekiqRedisStateStore;

    function ConnectionString(const AValue: string): TSidekiqRedisStateStore;
    function RedisClient(const AValue: ISidekiqRedisClient): TSidekiqRedisStateStore;
    function StateStore(const AValue: ISidekiqStateStore): TSidekiqRedisStateStore;

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
  Sidekiq4D.Store.Redis4D,
  Sidekiq4D.Redis4D.Client;

function TSidekiqRedisStateStore.EnsureInner: ISidekiqStateStore;
begin
  if not Assigned(FInner) then
    raise ESidekiqStateStoreNotImplemented.Create(
      'No Redis client configured. Call .ConnectionString(...) or .RedisClient(...).');
  Result := FInner;
end;

class function TSidekiqRedisStateStore.New: TSidekiqRedisStateStore;
begin
  Result := TSidekiqRedisStateStore.Create;
end;

function TSidekiqRedisStateStore.ConnectionString(
  const AValue: string): TSidekiqRedisStateStore;
begin
  Result := Self;
  FInner := TSidekiqRedis4DStateStore.New
    .RedisClient(TRedis4DClientBridge.NewFromConnectionString(AValue));
end;

function TSidekiqRedisStateStore.RedisClient(
  const AValue: ISidekiqRedisClient): TSidekiqRedisStateStore;
begin
  Result := Self;
  FInner := TSidekiqRedis4DStateStore.New.RedisClient(AValue);
end;

function TSidekiqRedisStateStore.StateStore(
  const AValue: ISidekiqStateStore): TSidekiqRedisStateStore;
begin
  Result := Self;
  FInner := AValue;
end;

function TSidekiqRedisStateStore.Get(const AKey: string): string;
begin
  Result := EnsureInner.Get(AKey);
end;

procedure TSidekiqRedisStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
begin
  EnsureInner.Put(AKey, AValue, ATtlSeconds);
end;

procedure TSidekiqRedisStateStore.Delete(const AKey: string);
begin
  EnsureInner.Delete(AKey);
end;

function TSidekiqRedisStateStore.Exists(const AKey: string): Boolean;
begin
  Result := EnsureInner.Exists(AKey);
end;

function TSidekiqRedisStateStore.ListKeys(const APrefix: string): TArray<string>;
begin
  Result := EnsureInner.ListKeys(APrefix);
end;

function TSidekiqRedisStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
begin
  Result := EnsureInner.TryPutIfAbsent(AKey, AValue, ATtlSeconds);
end;

end.
