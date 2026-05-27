unit Hefesto.Store.Redis4D;

interface

uses
  System.SysUtils,
  Hefesto.Store.Interfaces,
  Hefesto.Redis.Client;

type
  THefestoRedis4DStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FClient: IHefestoRedisClient;
    procedure EnsureClient;
  public
    class function New: THefestoRedis4DStateStore;

    function RedisClient(const AValue: IHefestoRedisClient): THefestoRedis4DStateStore;

    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

procedure THefestoRedis4DStateStore.EnsureClient;
begin
  if not Assigned(FClient) then
    raise EHefestoStateStoreNotImplemented.Create(
      'No Redis client configured. Call .RedisClient(TRedis4DClientBridge.NewFromConnectionString(...)).');
end;

class function THefestoRedis4DStateStore.New: THefestoRedis4DStateStore;
begin
  Result := THefestoRedis4DStateStore.Create;
end;

function THefestoRedis4DStateStore.RedisClient(
  const AValue: IHefestoRedisClient): THefestoRedis4DStateStore;
begin
  Result := Self;
  FClient := AValue;
end;

function THefestoRedis4DStateStore.Get(const AKey: string): string;
begin
  EnsureClient;
  Result := FClient.Get(AKey);
end;

procedure THefestoRedis4DStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
begin
  EnsureClient;
  FClient.SetEx(AKey, AValue, ATtlSeconds);
end;

procedure THefestoRedis4DStateStore.Delete(const AKey: string);
begin
  EnsureClient;
  FClient.Del(AKey);
end;

function THefestoRedis4DStateStore.Exists(const AKey: string): Boolean;
begin
  EnsureClient;
  Result := FClient.Exists(AKey);
end;

function THefestoRedis4DStateStore.ListKeys(const APrefix: string): TArray<string>;
begin
  EnsureClient;
  if APrefix.IsEmpty then
    Result := FClient.Keys('*')
  else
    Result := FClient.Keys(APrefix + '*');
end;

function THefestoRedis4DStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
begin
  EnsureClient;
  Result := FClient.SetNX(AKey, AValue, ATtlSeconds);
end;

end.
