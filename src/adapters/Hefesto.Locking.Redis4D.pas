unit Hefesto.Locking.Redis4D;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  Hefesto.Locking,
  Hefesto.Redis.Client;

type
  THefestoRedis4DLockHandle = class(
    TInterfacedObject,
    IHefestoLockHandle,
    IHefestoRenewableLockHandle)
  private
    FClient: IHefestoRedisClient;
    FKey: string;
    FToken: string;
    // 0 = not released, 1 = released. Integer so TInterlocked ops work correctly.
    FReleased: Integer;
    function RedisKey: string;
  public
    constructor Create(
      const AClient: IHefestoRedisClient;
      const AKey, AToken: string);

    function Key: string;
    function Token: string;
    procedure Release;
    procedure Renew(const ATtlSeconds: Integer);
  end;

  THefestoRedis4DLockProvider = class(
    TInterfacedObject,
    IHefestoLockProvider,
    IHefestoRecoverableLockProvider)
  private
    FClient: IHefestoRedisClient;
    function GetClient: IHefestoRedisClient;
  public
    class function New: THefestoRedis4DLockProvider;

    function ConnectionString(const AValue: string): THefestoRedis4DLockProvider;
    function RedisClient(
      const AValue: IHefestoRedisClient): THefestoRedis4DLockProvider;
    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): IHefestoLockHandle;
    procedure ForceRelease(const AKey: string);
  end;

implementation

uses
  Hefesto.Redis4D.Client;

const
  LUA_RELEASE_LOCK =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then ' +
    'return redis.call("DEL", KEYS[1]) else return 0 end';

  LUA_RENEW_LOCK =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then ' +
    'return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2])) else return 0 end';

{ THefestoRedis4DLockHandle }

constructor THefestoRedis4DLockHandle.Create(
  const AClient: IHefestoRedisClient;
  const AKey, AToken: string);
begin
  inherited Create;
  FClient := AClient;
  FKey    := AKey;
  FToken  := AToken;
end;

function THefestoRedis4DLockHandle.Key: string;
begin
  Result := FKey;
end;

function THefestoRedis4DLockHandle.Token: string;
begin
  Result := FToken;
end;

function THefestoRedis4DLockHandle.RedisKey: string;
begin
  Result := 'lock:' + FKey;
end;

procedure THefestoRedis4DLockHandle.Release;
begin
  // CompareExchange: only the first caller (0→1) executes the Lua release.
  if TInterlocked.CompareExchange(FReleased, 1, 0) <> 0 then
    Exit;
  if Assigned(FClient) then
    FClient.Eval(LUA_RELEASE_LOCK, [RedisKey], [FToken]);
end;

procedure THefestoRedis4DLockHandle.Renew(const ATtlSeconds: Integer);
begin
  if (TInterlocked.Read(FReleased) <> 0) or not Assigned(FClient) or (ATtlSeconds <= 0) then
    Exit;
  FClient.Eval(LUA_RENEW_LOCK, [RedisKey], [FToken, IntToStr(ATtlSeconds)]);
end;

{ THefestoRedis4DLockProvider }

class function THefestoRedis4DLockProvider.New: THefestoRedis4DLockProvider;
begin
  Result := THefestoRedis4DLockProvider.Create;
end;

function THefestoRedis4DLockProvider.GetClient: IHefestoRedisClient;
begin
  if not Assigned(FClient) then
    raise EArgumentException.Create(
      'No Redis client configured. Call .ConnectionString(...) or .RedisClient(...).');
  Result := FClient;
end;

function THefestoRedis4DLockProvider.ConnectionString(
  const AValue: string): THefestoRedis4DLockProvider;
begin
  Result := Self;
  FClient := TRedis4DClientBridge.NewFromConnectionString(AValue);
end;

function THefestoRedis4DLockProvider.RedisClient(
  const AValue: IHefestoRedisClient): THefestoRedis4DLockProvider;
begin
  Result := Self;
  FClient := AValue;
end;

function THefestoRedis4DLockProvider.TryAcquire(
  const AKey: string;
  const ATtlSeconds: Integer): IHefestoLockHandle;
var
  LToken: string;
begin
  Result := nil;
  LToken := GuidToString(TGuid.NewGuid);
  if GetClient.SetNX('lock:' + AKey, LToken, ATtlSeconds) then
    Result := THefestoRedis4DLockHandle.Create(GetClient, AKey, LToken);
end;

procedure THefestoRedis4DLockProvider.ForceRelease(const AKey: string);
begin
  GetClient.Del('lock:' + AKey);
end;

end.
