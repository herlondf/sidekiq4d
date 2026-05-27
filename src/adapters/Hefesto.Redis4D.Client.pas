unit Hefesto.Redis4D.Client;

// Bridge: implements IHefestoRedisClient using the Redis4D library.
//
// THIS IS THE ONLY FILE IN SIDEKIQ4D THAT IMPORTS Redis4D.*.
// All other adapters depend solely on IHefestoRedisClient.
//
// Usage:
//   uses Hefesto.Redis4D.Client;
//
//   // Standard connection
//   LClient := TRedis4DClientBridge.NewFromConnectionString('redis://127.0.0.1:6379/0');
//
//   // Sentinel (HA)
//   LClient := TRedis4DSentinelClientBridge.New(
//     ['127.0.0.1:26379', '127.0.0.1:26380'], 'mymaster');

interface

uses
  System.SysUtils,
  System.Classes,
  System.StrUtils,
  System.SyncObjs,
  Hefesto.Redis.Client,
  Redis4D.Interfaces,
  Redis4D.Features.Interfaces;

type
  // Base class — all IHefestoRedisClient methods delegate to GetClient.
  TRedis4DClientBridgeBase = class(TInterfacedObject, IHefestoRedisClient)
  private
    FStreams: IHefestoRedisStreams;
  protected
    // Shared lock: used by Streams() lazy-init in this class and by
    // TRedis4DSentinelClientBridge.GetClient() in the subclass.
    FLock: TCriticalSection;
    function GetClient: IRedis4DClient; virtual; abstract;
  public
    constructor Create;
    destructor Destroy; override;
    function Get(const AKey: string): string;
    procedure SetEx(const AKey, AValue: string; ATtlSeconds: Integer);
    function SetNX(const AKey, AValue: string; ATtlSeconds: Integer): Boolean;
    procedure Del(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function Keys(const APattern: string): TArray<string>;
    procedure ZAdd(const AKey: string; AScore: Double; const AMember: string);
    function ZCard(const AKey: string): Integer;
    procedure RPush(const AKey: string; const AValues: TArray<string>);
    function LRange(const AKey: string; AStart, AStop: Integer): TArray<string>;
    procedure LRem(const AKey: string; ACount: Integer; const AValue: string);
    function LLen(const AKey: string): Integer;
    function BeginPipeline: IHefestoRedisPipeline;
    function LLenBatch(const AKeys: TArray<string>): TArray<Integer>;
    function Eval(const AScript: string;
      const AKeys, AArgs: TArray<string>): TArray<string>;
    function Streams: IHefestoRedisStreams;
  end;

  // Pool-backed bridge: each IHefestoRedisClient call borrows a connection
  // from IRedis4DPool and returns it immediately after the call.
  // Use this when multiple threads share one IHefestoRedisClient concurrently,
  // or when per-call connection overhead needs to be amortized.
  //
  // Usage:
  //   LClient := TRedis4DPooledClientBridge.NewWithPool('redis://127.0.0.1:6379/0', 8);
  TRedis4DPooledClientBridge = class(TInterfacedObject, IHefestoRedisClient)
  private
    FPool: IRedis4DPool;
  public
    constructor Create(const APool: IRedis4DPool);

    class function NewWithPool(
      const AConnectionString: string;
      APoolSize: Integer = 8;
      AWarmup: Integer = 2): IHefestoRedisClient; static;

    function Get(const AKey: string): string;
    procedure SetEx(const AKey, AValue: string; ATtlSeconds: Integer);
    function SetNX(const AKey, AValue: string; ATtlSeconds: Integer): Boolean;
    procedure Del(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function Keys(const APattern: string): TArray<string>;
    procedure ZAdd(const AKey: string; AScore: Double; const AMember: string);
    function ZCard(const AKey: string): Integer;
    procedure RPush(const AKey: string; const AValues: TArray<string>);
    function LRange(const AKey: string; AStart, AStop: Integer): TArray<string>;
    procedure LRem(const AKey: string; ACount: Integer; const AValue: string);
    function LLen(const AKey: string): Integer;
    function BeginPipeline: IHefestoRedisPipeline;
    function LLenBatch(const AKeys: TArray<string>): TArray<Integer>;
    function Eval(const AScript: string;
      const AKeys, AArgs: TArray<string>): TArray<string>;
    function Streams: IHefestoRedisStreams;
  end;

  // Standard Redis4D client bridge (single server or Redis Cluster).
  TRedis4DClientBridge = class(TRedis4DClientBridgeBase)
  private
    FClient: IRedis4DClient;
  protected
    function GetClient: IRedis4DClient; override;
  public
    constructor Create(const AClient: IRedis4DClient);

    class function NewFromConnectionString(
      const AConnectionString: string): IHefestoRedisClient; static;

    // Parses redis://[:password@]host[:port][/db] format.
    // Exposed as a utility for adapters that need it.
    class procedure ParseConnectionString(
      const AConnectionString: string;
      out AHost: string;
      out APort: Word;
      out APassword: string;
      out ADatabase: Integer); static;
  end;

  // Redis Sentinel (HA) client bridge.
  TRedis4DSentinelClientBridge = class(TRedis4DClientBridgeBase)
  private
    FSeeds: TArray<string>;
    FServiceName: string;
    FPassword: string;
    FDatabase: Integer;
    FClient: IRedis4DClient;
  protected
    function GetClient: IRedis4DClient; override;
  public
    constructor Create(
      const ASeeds: TArray<string>;
      const AServiceName: string;
      const APassword: string;
      ADatabase: Integer);

    class function New(
      const ASeeds: TArray<string>;
      const AServiceName: string;
      const APassword: string = '';
      ADatabase: Integer = 0): IHefestoRedisClient; static;
  end;

implementation

uses
  System.Generics.Collections,
  Redis4D.Client,
  Redis4D.Core.RESP,
  Redis4D.Features.Pipeline,
  Redis4D.Features.Pool,
  Redis4D.Features.Streams,
  Redis4D.Sentinel,
  Redis4D.Sentinel.Client;

type
  TRedis4DStreamsBridge = class(TInterfacedObject, IHefestoRedisStreams)
  private
    FClient: IRedis4DClient;
    function GetStreams: IRedis4DStreams;
  public
    constructor Create(const AClient: IRedis4DClient);
    procedure CreateGroup(
      const AStreamKey, AGroupName, AStartID: string);
    function ReadGroup(
      const AGroupName, AConsumerName, AStreamKey, AStart: string;
      ACount, AWaitMs: Integer): TArray<THefestoStreamMessage>;
    procedure Acknowledge(
      const AStreamKey, AGroupName: string;
      const AMessageIDs: TArray<string>);
    procedure Add(
      const AStreamKey, AMessageID: string;
      const AFields: TStrings);
  end;

{ TRedis4DClientBridgeBase }

constructor TRedis4DClientBridgeBase.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor TRedis4DClientBridgeBase.Destroy;
begin
  FLock.Free;
  inherited;
end;

{ TRedis4DStreamsBridge }

constructor TRedis4DStreamsBridge.Create(const AClient: IRedis4DClient);
begin
  inherited Create;
  FClient := AClient;
end;

function TRedis4DStreamsBridge.GetStreams: IRedis4DStreams;
begin
  Result := TRedis4DStreams.New(FClient);
end;

procedure TRedis4DStreamsBridge.CreateGroup(
  const AStreamKey, AGroupName, AStartID: string);
begin
  try
    GetStreams.XGroupCreate(AStreamKey, AGroupName, AStartID, True);
  except
    // BUSYGROUP: group already exists — ignore
  end;
end;

function TRedis4DStreamsBridge.ReadGroup(
  const AGroupName, AConsumerName, AStreamKey, AStart: string;
  ACount, AWaitMs: Integer): TArray<THefestoStreamMessage>;
var
  LReads: TArray<TRedis4DStreamRead>;
  LRead: TRedis4DStreamRead;
  LEntry: TRedis4DStreamEntry;
  LMsg: THefestoStreamMessage;
  LMsgCount, LFldIndex, LTotal: Integer;
begin
  LReads := GetStreams.XReadGroup(
    AGroupName, AConsumerName, [AStreamKey, AStart], ACount, AWaitMs);

  // Count total entries across all reads
  LTotal := 0;
  for LRead in LReads do
    Inc(LTotal, Length(LRead.Entries));

  SetLength(Result, LTotal);
  LMsgCount := 0;

  for LRead in LReads do
    for LEntry in LRead.Entries do
    begin
      LMsg.ID := LEntry.Id;
      SetLength(LMsg.Fields, Length(LEntry.Fields));
      for LFldIndex := 0 to Pred(Length(LEntry.Fields)) do
      begin
        LMsg.Fields[LFldIndex].Name  := LEntry.Fields[LFldIndex].Name;
        LMsg.Fields[LFldIndex].Value := LEntry.Fields[LFldIndex].Value;
      end;
      Result[LMsgCount] := LMsg;
      Inc(LMsgCount);
    end;
end;

procedure TRedis4DStreamsBridge.Acknowledge(
  const AStreamKey, AGroupName: string;
  const AMessageIDs: TArray<string>);
begin
  GetStreams.XAck(AStreamKey, AGroupName, AMessageIDs);
end;

procedure TRedis4DStreamsBridge.Add(
  const AStreamKey, AMessageID: string;
  const AFields: TStrings);
var
  LFieldValues: TArray<string>;
  LIndex: Integer;
  LName: string;
begin
  SetLength(LFieldValues, AFields.Count * 2);
  for LIndex := 0 to Pred(AFields.Count) do
  begin
    LName := AFields.Names[LIndex];
    if LName.IsEmpty then
      LName := AFields[LIndex];
    LFieldValues[LIndex * 2]     := LName;
    LFieldValues[LIndex * 2 + 1] := AFields.Values[LName];
  end;
  GetStreams.XAdd(AStreamKey, LFieldValues, AMessageID);
end;

{ TRedis4DStreamsBridgePooled }

type
  // Wraps TRedis4DStreamsBridge and releases the borrowed pool connection
  // back to IRedis4DPool when this object is destroyed (interface ref-count = 0).
  TRedis4DStreamsBridgePooled = class(TInterfacedObject, IHefestoRedisStreams)
  private
    FInner: IHefestoRedisStreams;
    FPool: IRedis4DPool;
    FClient: IRedis4DClient;
  public
    constructor Create(
      const APool: IRedis4DPool;
      const AClient: IRedis4DClient);
    destructor Destroy; override;

    procedure CreateGroup(
      const AStreamKey, AGroupName, AStartID: string);
    function ReadGroup(
      const AGroupName, AConsumerName, AStreamKey, AStart: string;
      ACount, AWaitMs: Integer): TArray<THefestoStreamMessage>;
    procedure Acknowledge(
      const AStreamKey, AGroupName: string;
      const AMessageIDs: TArray<string>);
    procedure Add(
      const AStreamKey, AMessageID: string;
      const AFields: TStrings);
  end;

constructor TRedis4DStreamsBridgePooled.Create(
  const APool: IRedis4DPool;
  const AClient: IRedis4DClient);
begin
  inherited Create;
  FPool   := APool;
  FClient := AClient;
  FInner  := TRedis4DStreamsBridge.Create(AClient);
end;

destructor TRedis4DStreamsBridgePooled.Destroy;
begin
  FInner := nil; // release inner bridge before returning connection
  if Assigned(FPool) and Assigned(FClient) then
    FPool.Release(FClient);
  FClient := nil;
  inherited;
end;

procedure TRedis4DStreamsBridgePooled.CreateGroup(
  const AStreamKey, AGroupName, AStartID: string);
begin
  FInner.CreateGroup(AStreamKey, AGroupName, AStartID);
end;

function TRedis4DStreamsBridgePooled.ReadGroup(
  const AGroupName, AConsumerName, AStreamKey, AStart: string;
  ACount, AWaitMs: Integer): TArray<THefestoStreamMessage>;
begin
  Result := FInner.ReadGroup(AGroupName, AConsumerName, AStreamKey, AStart,
    ACount, AWaitMs);
end;

procedure TRedis4DStreamsBridgePooled.Acknowledge(
  const AStreamKey, AGroupName: string;
  const AMessageIDs: TArray<string>);
begin
  FInner.Acknowledge(AStreamKey, AGroupName, AMessageIDs);
end;

procedure TRedis4DStreamsBridgePooled.Add(
  const AStreamKey, AMessageID: string;
  const AFields: TStrings);
begin
  FInner.Add(AStreamKey, AMessageID, AFields);
end;

{ THefestoRedisPipelineImpl }

type
  // Internal pipeline implementation. Holds a list of commands and an
  // IRedis4DClient reference. An optional FOnRelease callback (used by the
  // pooled bridge) returns the borrowed connection to the pool on Execute.
  THefestoRedisPipelineImpl = class(TInterfacedObject, IHefestoRedisPipeline)
  private
    FClient: IRedis4DClient;
    FCommands: TList<TArray<string>>;
    FOnRelease: TProc<IRedis4DClient>; // nil for non-pooled bridges
    FExecuted: Boolean;
    procedure ReleaseClient;
  public
    constructor Create(
      const AClient: IRedis4DClient;
      const AOnRelease: TProc<IRedis4DClient>);
    destructor Destroy; override;

    function LPush(const AKey, AValue: string): IHefestoRedisPipeline;
    function ZAdd(
      const AKey: string; AScore: Double;
      const AMember: string): IHefestoRedisPipeline;
    function QueueCommand(
      const ACommand: TArray<string>): IHefestoRedisPipeline;
    procedure Execute;
  end;

constructor THefestoRedisPipelineImpl.Create(
  const AClient: IRedis4DClient;
  const AOnRelease: TProc<IRedis4DClient>);
begin
  inherited Create;
  FClient := AClient;
  FOnRelease := AOnRelease;
  FCommands := TList<TArray<string>>.Create;
  FExecuted := False;
end;

destructor THefestoRedisPipelineImpl.Destroy;
begin
  // If Execute was never called (e.g. early return, exception), release connection
  if not FExecuted then
    ReleaseClient;
  FCommands.Free;
  inherited;
end;

procedure THefestoRedisPipelineImpl.ReleaseClient;
begin
  if Assigned(FOnRelease) and Assigned(FClient) then
    FOnRelease(FClient);
  FClient := nil;
end;

function THefestoRedisPipelineImpl.LPush(
  const AKey, AValue: string): IHefestoRedisPipeline;
begin
  Result := Self;
  FCommands.Add(['LPUSH', AKey, AValue]);
end;

function THefestoRedisPipelineImpl.ZAdd(
  const AKey: string; AScore: Double;
  const AMember: string): IHefestoRedisPipeline;
begin
  Result := Self;
  FCommands.Add(['ZADD', AKey, FloatToStr(AScore), AMember]);
end;

function THefestoRedisPipelineImpl.QueueCommand(
  const ACommand: TArray<string>): IHefestoRedisPipeline;
begin
  Result := Self;
  FCommands.Add(Copy(ACommand));
end;

procedure THefestoRedisPipelineImpl.Execute;
begin
  if FExecuted then
    Exit;
  FExecuted := True;
  try
    if FCommands.Count > 0 then
      FClient.ExecutePipeline(FCommands.ToArray);
  finally
    ReleaseClient;
  end;
end;

{ TRedis4DClientBridgeBase }

function TRedis4DClientBridgeBase.Get(const AKey: string): string;
var
  LValue: IRedis4DRESPValue;
begin
  LValue := GetClient.GetValue(AKey);
  if LValue.IsNull then Result := '' else Result := LValue.AsString;
end;

procedure TRedis4DClientBridgeBase.SetEx(
  const AKey, AValue: string; ATtlSeconds: Integer);
begin
  GetClient.SetValue(AKey, AValue, ATtlSeconds);
end;

function TRedis4DClientBridgeBase.SetNX(
  const AKey, AValue: string; ATtlSeconds: Integer): Boolean;
begin
  Result := GetClient.SetValueIfAbsent(AKey, AValue, ATtlSeconds);
end;

procedure TRedis4DClientBridgeBase.Del(const AKey: string);
begin
  GetClient.Delete(AKey);
end;

function TRedis4DClientBridgeBase.Exists(const AKey: string): Boolean;
begin
  Result := GetClient.Exists(AKey);
end;

function TRedis4DClientBridgeBase.Keys(const APattern: string): TArray<string>;
begin
  Result := GetClient.ListKeys(APattern);
end;

procedure TRedis4DClientBridgeBase.ZAdd(
  const AKey: string; AScore: Double; const AMember: string);
begin
  GetClient.ZAdd(AKey, AScore, AMember);
end;

function TRedis4DClientBridgeBase.ZCard(const AKey: string): Integer;
begin
  Result := GetClient.ZCard(AKey);
end;

procedure TRedis4DClientBridgeBase.RPush(
  const AKey: string; const AValues: TArray<string>);
begin
  GetClient.RPush(AKey, AValues);
end;

function TRedis4DClientBridgeBase.LRange(
  const AKey: string; AStart, AStop: Integer): TArray<string>;
begin
  Result := GetClient.LRange(AKey, AStart, AStop);
end;

procedure TRedis4DClientBridgeBase.LRem(
  const AKey: string; ACount: Integer; const AValue: string);
begin
  GetClient.Execute(['LREM', AKey, IntToStr(ACount), AValue]);
end;

function TRedis4DClientBridgeBase.LLen(const AKey: string): Integer;
begin
  Result := GetClient.LLen(AKey);
end;

function TRedis4DClientBridgeBase.BeginPipeline: IHefestoRedisPipeline;
begin
  Result := THefestoRedisPipelineImpl.Create(GetClient, nil);
end;

function TRedis4DClientBridgeBase.LLenBatch(
  const AKeys: TArray<string>): TArray<Integer>;
var
  LCommands: TArray<TArray<string>>;
  LResults: TArray<IRedis4DRESPValue>;
  LIndex: Integer;
begin
  if Length(AKeys) = 0 then
    Exit(nil);
  SetLength(LCommands, Length(AKeys));
  for LIndex := 0 to Pred(Length(AKeys)) do
    LCommands[LIndex] := ['LLEN', AKeys[LIndex]];
  LResults := GetClient.ExecutePipeline(LCommands);
  SetLength(Result, Length(LResults));
  for LIndex := 0 to Pred(Length(LResults)) do
    if Assigned(LResults[LIndex]) and not LResults[LIndex].IsNull then
      Result[LIndex] := LResults[LIndex].AsInteger
    else
      Result[LIndex] := 0;
end;

function TRedis4DClientBridgeBase.Eval(
  const AScript: string;
  const AKeys, AArgs: TArray<string>): TArray<string>;
var
  LResponse: IRedis4DRESPValue;
  LIndex: Integer;
begin
  LResponse := GetClient.Eval(AScript, AKeys, AArgs);
  if LResponse.ArrayLength = 0 then
    Exit(nil);
  SetLength(Result, LResponse.ArrayLength);
  for LIndex := 0 to Pred(LResponse.ArrayLength) do
    Result[LIndex] := LResponse.Item(LIndex).AsString;
end;

function TRedis4DClientBridgeBase.Streams: IHefestoRedisStreams;
begin
  if not Assigned(FStreams) then
  begin
    FLock.Acquire;
    try
      if not Assigned(FStreams) then
        FStreams := TRedis4DStreamsBridge.Create(GetClient);
    finally
      FLock.Release;
    end;
  end;
  Result := FStreams;
end;

{ TRedis4DClientBridge }

constructor TRedis4DClientBridge.Create(const AClient: IRedis4DClient);
begin
  inherited Create;
  FClient := AClient;
end;

function TRedis4DClientBridge.GetClient: IRedis4DClient;
begin
  Result := FClient;
end;

class function TRedis4DClientBridge.NewFromConnectionString(
  const AConnectionString: string): IHefestoRedisClient;
var
  LHost: string;
  LPort: Word;
  LPassword: string;
  LDatabase: Integer;
begin
  ParseConnectionString(AConnectionString, LHost, LPort, LPassword, LDatabase);
  Result := TRedis4DClientBridge.Create(
    TRedis4DClient.NewWithPassword(LHost, LPort, nil, LPassword, LDatabase)
      .ConnectTimeout(2000)
      .ReadTimeout(2000));
end;

class procedure TRedis4DClientBridge.ParseConnectionString(
  const AConnectionString: string;
  out AHost: string;
  out APort: Word;
  out APassword: string;
  out ADatabase: Integer);
var
  LValue, LAuthHost, LDb, LHostPort, LPortText: string;
  LAtPos, LColonPos, LSlashPos: Integer;
begin
  AHost := '127.0.0.1';
  APort := 6379;
  APassword := '';
  ADatabase := 0;
  LValue := AConnectionString.Trim;
  if LValue.IsEmpty then Exit;
  if StartsText('redis://', LValue) then
    System.Delete(LValue, 1, Length('redis://'));
  LSlashPos := Pos('/', LValue);
  if LSlashPos > 0 then
  begin
    LDb    := Copy(LValue, LSlashPos + 1, MaxInt);
    LValue := Copy(LValue, 1, LSlashPos - 1);
    ADatabase := StrToIntDef(LDb, 0);
  end;
  LAtPos := Pos('@', LValue);
  if LAtPos > 0 then
  begin
    LAuthHost := Copy(LValue, 1, LAtPos - 1);
    LValue    := Copy(LValue, LAtPos + 1, MaxInt);
    if StartsText(':', LAuthHost) then
      System.Delete(LAuthHost, 1, 1);
    APassword := LAuthHost;
  end;
  LHostPort  := LValue;
  LColonPos  := LastDelimiter(':', LHostPort);
  if LColonPos > 0 then
  begin
    AHost    := Copy(LHostPort, 1, LColonPos - 1);
    LPortText := Copy(LHostPort, LColonPos + 1, MaxInt);
    APort    := StrToIntDef(LPortText, 6379);
  end
  else if not LHostPort.IsEmpty then
    AHost := LHostPort;
end;

{ TRedis4DSentinelClientBridge }

constructor TRedis4DSentinelClientBridge.Create(
  const ASeeds: TArray<string>;
  const AServiceName, APassword: string;
  ADatabase: Integer);
begin
  inherited Create;
  FSeeds       := Copy(ASeeds);
  FServiceName := AServiceName;
  FPassword    := APassword;
  FDatabase    := ADatabase;
end;

class function TRedis4DSentinelClientBridge.New(
  const ASeeds: TArray<string>;
  const AServiceName: string;
  const APassword: string;
  ADatabase: Integer): IHefestoRedisClient;
begin
  Result := TRedis4DSentinelClientBridge.Create(
    ASeeds, AServiceName, APassword, ADatabase);
end;

function TRedis4DSentinelClientBridge.GetClient: IRedis4DClient;
var
  LDiscovery: IRedis4DSentinelDiscovery;
begin
  if not Assigned(FClient) then
  begin
    FLock.Acquire;
    try
      if not Assigned(FClient) then
      begin
        if Length(FSeeds) = 0 then
          raise EArgumentException.Create('SentinelSeeds must be configured.');
        if FServiceName.IsEmpty then
          raise EArgumentException.Create('ServiceName must be configured.');
        LDiscovery := TRedis4DSentinelDiscovery.New(FSeeds);
        FClient := TRedis4DSentinelAwareClient.New(
          LDiscovery, FServiceName, FPassword, FDatabase)
          .ConnectTimeout(2000)
          .ReadTimeout(2000);
      end;
    finally
      FLock.Release;
    end;
  end;
  Result := FClient;
end;

{ TRedis4DPooledClientBridge }

constructor TRedis4DPooledClientBridge.Create(const APool: IRedis4DPool);
begin
  inherited Create;
  FPool := APool;
end;

class function TRedis4DPooledClientBridge.NewWithPool(
  const AConnectionString: string;
  APoolSize: Integer;
  AWarmup: Integer): IHefestoRedisClient;
var
  LHost: string;
  LPort: Word;
  LPassword: string;
  LDatabase: Integer;
  LPool: IRedis4DPool;
begin
  TRedis4DClientBridge.ParseConnectionString(
    AConnectionString, LHost, LPort, LPassword, LDatabase);
  LPool := TRedis4DPool.New(
    function: IRedis4DClient
    begin
      Result := TRedis4DClient.NewWithPassword(LHost, LPort, nil, LPassword, LDatabase)
        .ConnectTimeout(2000)
        .ReadTimeout(5000);
    end,
    APoolSize, 3000)
    .ReturnOnError(True)
    .HealthCheckOnAcquire(False)
    .KeepAlive(30000)
    .Warmup(AWarmup);
  Result := TRedis4DPooledClientBridge.Create(LPool);
end;

function TRedis4DPooledClientBridge.Get(const AKey: string): string;
var
  LClient: IRedis4DClient;
  LValue: IRedis4DRESPValue;
begin
  LClient := FPool.Acquire;
  try
    LValue := LClient.GetValue(AKey);
    if LValue.IsNull then Result := '' else Result := LValue.AsString;
  finally
    FPool.Release(LClient);
  end;
end;

procedure TRedis4DPooledClientBridge.SetEx(
  const AKey, AValue: string; ATtlSeconds: Integer);
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    LClient.SetValue(AKey, AValue, ATtlSeconds);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.SetNX(
  const AKey, AValue: string; ATtlSeconds: Integer): Boolean;
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    Result := LClient.SetValueIfAbsent(AKey, AValue, ATtlSeconds);
  finally
    FPool.Release(LClient);
  end;
end;

procedure TRedis4DPooledClientBridge.Del(const AKey: string);
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    LClient.Delete(AKey);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.Exists(const AKey: string): Boolean;
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    Result := LClient.Exists(AKey);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.Keys(
  const APattern: string): TArray<string>;
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    Result := LClient.ListKeys(APattern);
  finally
    FPool.Release(LClient);
  end;
end;

procedure TRedis4DPooledClientBridge.ZAdd(
  const AKey: string; AScore: Double; const AMember: string);
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    LClient.ZAdd(AKey, AScore, AMember);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.ZCard(const AKey: string): Integer;
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    Result := LClient.ZCard(AKey);
  finally
    FPool.Release(LClient);
  end;
end;

procedure TRedis4DPooledClientBridge.RPush(
  const AKey: string; const AValues: TArray<string>);
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    LClient.RPush(AKey, AValues);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.LRange(
  const AKey: string; AStart, AStop: Integer): TArray<string>;
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    Result := LClient.LRange(AKey, AStart, AStop);
  finally
    FPool.Release(LClient);
  end;
end;

procedure TRedis4DPooledClientBridge.LRem(
  const AKey: string; ACount: Integer; const AValue: string);
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    LClient.Execute(['LREM', AKey, IntToStr(ACount), AValue]);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.LLen(const AKey: string): Integer;
var
  LClient: IRedis4DClient;
begin
  LClient := FPool.Acquire;
  try
    Result := LClient.LLen(AKey);
  finally
    FPool.Release(LClient);
  end;
end;

function TRedis4DPooledClientBridge.BeginPipeline: IHefestoRedisPipeline;
var
  LClient: IRedis4DClient;
  LPool: IRedis4DPool;
begin
  LClient := FPool.Acquire;
  LPool := FPool;
  Result := THefestoRedisPipelineImpl.Create(
    LClient,
    procedure(AClient: IRedis4DClient)
    begin
      LPool.Release(AClient);
    end);
end;

function TRedis4DPooledClientBridge.LLenBatch(
  const AKeys: TArray<string>): TArray<Integer>;
var
  LClient: IRedis4DClient;
  LCommands: TArray<TArray<string>>;
  LResults: TArray<IRedis4DRESPValue>;
  LIndex: Integer;
begin
  if Length(AKeys) = 0 then
    Exit(nil);
  SetLength(LCommands, Length(AKeys));
  for LIndex := 0 to Pred(Length(AKeys)) do
    LCommands[LIndex] := ['LLEN', AKeys[LIndex]];
  LClient := FPool.Acquire;
  try
    LResults := LClient.ExecutePipeline(LCommands);
  finally
    FPool.Release(LClient);
  end;
  SetLength(Result, Length(LResults));
  for LIndex := 0 to Pred(Length(LResults)) do
    if Assigned(LResults[LIndex]) and not LResults[LIndex].IsNull then
      Result[LIndex] := LResults[LIndex].AsInteger
    else
      Result[LIndex] := 0;
end;

function TRedis4DPooledClientBridge.Eval(
  const AScript: string;
  const AKeys, AArgs: TArray<string>): TArray<string>;
var
  LClient: IRedis4DClient;
  LResponse: IRedis4DRESPValue;
  LIndex: Integer;
begin
  LClient := FPool.Acquire;
  try
    LResponse := LClient.Eval(AScript, AKeys, AArgs);
  finally
    FPool.Release(LClient);
  end;
  if LResponse.ArrayLength = 0 then
    Exit(nil);
  SetLength(Result, LResponse.ArrayLength);
  for LIndex := 0 to Pred(LResponse.ArrayLength) do
    Result[LIndex] := LResponse.Item(LIndex).AsString;
end;

function TRedis4DPooledClientBridge.Streams: IHefestoRedisStreams;
var
  LClient: IRedis4DClient;
  LPool: IRedis4DPool;
begin
  // Acquire a dedicated connection for the lifetime of the returned
  // IHefestoRedisStreams object. The connection is returned to the pool
  // when the IHefestoRedisStreams reference is released (ref-count drops to 0).
  LClient := FPool.Acquire;
  LPool   := FPool;
  Result  := TRedis4DStreamsBridgePooled.Create(LPool, LClient);
end;

end.
