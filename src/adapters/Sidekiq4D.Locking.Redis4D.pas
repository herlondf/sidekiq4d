unit Sidekiq4D.Locking.Redis4D;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  Sidekiq4D.Locking,
  Sidekiq4D.Redis.Client;

type
  TSidekiqRedis4DLockHandle = class(
    TInterfacedObject,
    ISidekiqLockHandle,
    ISidekiqRenewableLockHandle)
  private
    FClient: ISidekiqRedisClient;
    FKey: string;
    FToken: string;
    // 0 = not released, 1 = released. Integer so TInterlocked ops work correctly.
    FReleased: Integer;
    function RedisKey: string;
  public
    constructor Create(
      const AClient: ISidekiqRedisClient;
      const AKey, AToken: string);

    function Key: string;
    function Token: string;
    procedure Release;
    procedure Renew(const ATtlSeconds: Integer);
  end;

  TSidekiqRedis4DLockProvider = class(
    TInterfacedObject,
    ISidekiqLockProvider,
    ISidekiqRecoverableLockProvider)
  private
    FClient: ISidekiqRedisClient;
    function GetClient: ISidekiqRedisClient;
  public
    class function New: TSidekiqRedis4DLockProvider;

    function ConnectionString(const AValue: string): TSidekiqRedis4DLockProvider;
    function RedisClient(
      const AValue: ISidekiqRedisClient): TSidekiqRedis4DLockProvider;
    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): ISidekiqLockHandle;
    procedure ForceRelease(const AKey: string);
  end;

implementation

uses
  Sidekiq4D.Redis4D.Client;

const
  LUA_RELEASE_LOCK =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then ' +
    'return redis.call("DEL", KEYS[1]) else return 0 end';

  LUA_RENEW_LOCK =
    'if redis.call("GET", KEYS[1]) == ARGV[1] then ' +
    'return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2])) else return 0 end';

{ TSidekiqRedis4DLockHandle }

constructor TSidekiqRedis4DLockHandle.Create(
  const AClient: ISidekiqRedisClient;
  const AKey, AToken: string);
begin
  inherited Create;
  FClient := AClient;
  FKey    := AKey;
  FToken  := AToken;
end;

function TSidekiqRedis4DLockHandle.Key: string;
begin
  Result := FKey;
end;

function TSidekiqRedis4DLockHandle.Token: string;
begin
  Result := FToken;
end;

function TSidekiqRedis4DLockHandle.RedisKey: string;
begin
  Result := 'lock:' + FKey;
end;

procedure TSidekiqRedis4DLockHandle.Release;
begin
  // CompareExchange: only the first caller (0→1) executes the Lua release.
  if TInterlocked.CompareExchange(FReleased, 1, 0) <> 0 then
    Exit;
  if Assigned(FClient) then
    FClient.Eval(LUA_RELEASE_LOCK, [RedisKey], [FToken]);
end;

procedure TSidekiqRedis4DLockHandle.Renew(const ATtlSeconds: Integer);
begin
  if (TInterlocked.Read(FReleased) <> 0) or not Assigned(FClient) or (ATtlSeconds <= 0) then
    Exit;
  FClient.Eval(LUA_RENEW_LOCK, [RedisKey], [FToken, IntToStr(ATtlSeconds)]);
end;

{ TSidekiqRedis4DLockProvider }

class function TSidekiqRedis4DLockProvider.New: TSidekiqRedis4DLockProvider;
begin
  Result := TSidekiqRedis4DLockProvider.Create;
end;

function TSidekiqRedis4DLockProvider.GetClient: ISidekiqRedisClient;
begin
  if not Assigned(FClient) then
    raise EArgumentException.Create(
      'No Redis client configured. Call .ConnectionString(...) or .RedisClient(...).');
  Result := FClient;
end;

function TSidekiqRedis4DLockProvider.ConnectionString(
  const AValue: string): TSidekiqRedis4DLockProvider;
begin
  Result := Self;
  FClient := TRedis4DClientBridge.NewFromConnectionString(AValue);
end;

function TSidekiqRedis4DLockProvider.RedisClient(
  const AValue: ISidekiqRedisClient): TSidekiqRedis4DLockProvider;
begin
  Result := Self;
  FClient := AValue;
end;

function TSidekiqRedis4DLockProvider.TryAcquire(
  const AKey: string;
  const ATtlSeconds: Integer): ISidekiqLockHandle;
var
  LToken: string;
begin
  Result := nil;
  LToken := GuidToString(TGuid.NewGuid);
  if GetClient.SetNX('lock:' + AKey, LToken, ATtlSeconds) then
    Result := TSidekiqRedis4DLockHandle.Create(GetClient, AKey, LToken);
end;

procedure TSidekiqRedis4DLockProvider.ForceRelease(const AKey: string);
begin
  GetClient.Del('lock:' + AKey);
end;

end.
