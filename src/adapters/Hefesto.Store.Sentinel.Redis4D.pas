unit Hefesto.Store.Sentinel.Redis4D;

interface

uses
  System.SyncObjs,
  Hefesto.Store.Interfaces,
  Hefesto.Redis.Client,
  Hefesto.Redis4D.Client;

type
  // State store para ambientes com Redis Sentinel (HA sem cluster nativo).
  // Padrao fluente consistente com THefestoRedis4DStateStore.
  //
  // Uso:
  //   LStore := THefestoRedis4DSentinelStateStore.New
  //     .SentinelSeeds(['127.0.0.1:26379', '127.0.0.1:26380'])
  //     .ServiceName('mymaster')
  //     .Password('secret');  // opcional
  THefestoRedis4DSentinelStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    FLock: TCriticalSection;
    FSeeds: TArray<string>;
    FServiceName: string;
    FPassword: string;
    FDatabase: Integer;
    FClient: IHefestoRedisClient;

    function GetClient: IHefestoRedisClient;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoRedis4DSentinelStateStore; static;

    function SentinelSeeds(
      const ASeeds: TArray<string>): THefestoRedis4DSentinelStateStore;
    function ServiceName(
      const AName: string): THefestoRedis4DSentinelStateStore;
    function Password(
      const APassword: string): THefestoRedis4DSentinelStateStore;
    function Database(
      ADatabase: Integer): THefestoRedis4DSentinelStateStore;

    function Get(const AKey: string): string;
    procedure Put(
      const AKey, AValue: string;
      const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string;
      const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

uses
  System.SysUtils;

function THefestoRedis4DSentinelStateStore.GetClient: IHefestoRedisClient;
begin
  if not Assigned(FClient) then
  begin
    FLock.Acquire;
    try
      if not Assigned(FClient) then
      begin
        if Length(FSeeds) = 0 then
          raise EArgumentException.Create(
            'SentinelSeeds deve ser configurado antes de usar o store.');
        if FServiceName.IsEmpty then
          raise EArgumentException.Create(
            'ServiceName deve ser configurado antes de usar o store.');
        FClient := TRedis4DSentinelClientBridge.New(
          FSeeds, FServiceName, FPassword, FDatabase);
      end;
    finally
      FLock.Release;
    end;
  end;
  Result := FClient;
end;

constructor THefestoRedis4DSentinelStateStore.Create;
begin
  inherited Create;
  FLock     := TCriticalSection.Create;
  FDatabase := 0;
end;

destructor THefestoRedis4DSentinelStateStore.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function THefestoRedis4DSentinelStateStore.New: THefestoRedis4DSentinelStateStore;
begin
  Result := THefestoRedis4DSentinelStateStore.Create;
end;

function THefestoRedis4DSentinelStateStore.SentinelSeeds(
  const ASeeds: TArray<string>): THefestoRedis4DSentinelStateStore;
begin
  Result := Self;
  FLock.Acquire;
  try
    FSeeds  := Copy(ASeeds);
    FClient := nil;
  finally
    FLock.Release;
  end;
end;

function THefestoRedis4DSentinelStateStore.ServiceName(
  const AName: string): THefestoRedis4DSentinelStateStore;
begin
  Result := Self;
  FLock.Acquire;
  try
    FServiceName := AName.Trim;
    FClient      := nil;
  finally
    FLock.Release;
  end;
end;

function THefestoRedis4DSentinelStateStore.Password(
  const APassword: string): THefestoRedis4DSentinelStateStore;
begin
  Result := Self;
  FLock.Acquire;
  try
    FPassword := APassword;
    FClient   := nil;
  finally
    FLock.Release;
  end;
end;

function THefestoRedis4DSentinelStateStore.Database(
  ADatabase: Integer): THefestoRedis4DSentinelStateStore;
begin
  Result := Self;
  FLock.Acquire;
  try
    FDatabase := ADatabase;
    FClient   := nil;
  finally
    FLock.Release;
  end;
end;

function THefestoRedis4DSentinelStateStore.Get(const AKey: string): string;
begin
  Result := GetClient.Get(AKey);
end;

procedure THefestoRedis4DSentinelStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
begin
  GetClient.SetEx(AKey, AValue, ATtlSeconds);
end;

procedure THefestoRedis4DSentinelStateStore.Delete(const AKey: string);
begin
  GetClient.Del(AKey);
end;

function THefestoRedis4DSentinelStateStore.Exists(const AKey: string): Boolean;
begin
  Result := GetClient.Exists(AKey);
end;

function THefestoRedis4DSentinelStateStore.ListKeys(
  const APrefix: string): TArray<string>;
begin
  if APrefix.IsEmpty then
    Result := GetClient.Keys('*')
  else
    Result := GetClient.Keys(APrefix + '*');
end;

function THefestoRedis4DSentinelStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
begin
  Result := GetClient.SetNX(AKey, AValue, ATtlSeconds);
end;

end.
