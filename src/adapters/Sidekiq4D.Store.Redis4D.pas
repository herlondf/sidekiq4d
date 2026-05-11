unit Sidekiq4D.Store.Redis4D;

interface

uses
  System.SysUtils,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Redis.Client;

type
  TSidekiqRedis4DStateStore = class(TInterfacedObject, ISidekiqStateStore)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FClient: ISidekiqRedisClient;
    procedure EnsureClient;
  public
    class function New: TSidekiqRedis4DStateStore;

    function RedisClient(const AValue: ISidekiqRedisClient): TSidekiqRedis4DStateStore;

    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

procedure TSidekiqRedis4DStateStore.EnsureClient;
begin
  if not Assigned(FClient) then
    raise ESidekiqStateStoreNotImplemented.Create(
      'No Redis client configured. Call .RedisClient(TRedis4DClientBridge.NewFromConnectionString(...)).');
end;

class function TSidekiqRedis4DStateStore.New: TSidekiqRedis4DStateStore;
begin
  Result := TSidekiqRedis4DStateStore.Create;
end;

function TSidekiqRedis4DStateStore.RedisClient(
  const AValue: ISidekiqRedisClient): TSidekiqRedis4DStateStore;
begin
  Result := Self;
  FClient := AValue;
end;

function TSidekiqRedis4DStateStore.Get(const AKey: string): string;
begin
  EnsureClient;
  Result := FClient.Get(AKey);
end;

procedure TSidekiqRedis4DStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
begin
  EnsureClient;
  FClient.SetEx(AKey, AValue, ATtlSeconds);
end;

procedure TSidekiqRedis4DStateStore.Delete(const AKey: string);
begin
  EnsureClient;
  FClient.Del(AKey);
end;

function TSidekiqRedis4DStateStore.Exists(const AKey: string): Boolean;
begin
  EnsureClient;
  Result := FClient.Exists(AKey);
end;

function TSidekiqRedis4DStateStore.ListKeys(const APrefix: string): TArray<string>;
begin
  EnsureClient;
  if APrefix.IsEmpty then
    Result := FClient.Keys('*')
  else
    Result := FClient.Keys(APrefix + '*');
end;

function TSidekiqRedis4DStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
begin
  EnsureClient;
  Result := FClient.SetNX(AKey, AValue, ATtlSeconds);
end;

end.
