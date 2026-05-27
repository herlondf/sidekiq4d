unit Hefesto.Scheduled.Redis4D;

interface

uses
  Hefesto.Scheduled,
  Hefesto.Redis.Client;

type
  THefestoRedis4DScheduledStore = class(TInterfacedObject, IHefestoScheduledStore)
  private const
    SCHEDULED_KEY = 'sidekiq4d:scheduled';
    FIELD_SEP = #1;
    ATTR_SEP  = #2;
    LUA_LIST =
      'return redis.call(''ZRANGE'', KEYS[1], 0, -1)';
    LUA_DELETE_BY_SCORE_PREFIX =
      'local cands = redis.call(''ZRANGEBYSCORE'', KEYS[1], ARGV[1], ARGV[1]) ' +
      'local sep = string.char(1) ' +
      'for i, m in ipairs(cands) do ' +
      '  local parts = {} ' +
      '  for p in (m .. sep):gmatch("(.-)" .. sep) do parts[#parts+1] = p if #parts >= 2 then break end end ' +
      '  if parts[1] == ARGV[2] and parts[2] == ARGV[3] then ' +
      '    redis.call(''ZREM'', KEYS[1], m) ' +
      '    return 1 ' +
      '  end ' +
      'end ' +
      'return 0';
    LUA_POP_DUE =
      'local due = redis.call(''ZRANGEBYSCORE'', KEYS[1], ''-inf'', ARGV[1]) ' +
      'if #due == 0 then return {} end ' +
      'local limit = tonumber(ARGV[2]) ' +
      'local result = {} ' +
      'for i = 1, math.min(#due, limit) do ' +
      '  result[i] = due[i] ' +
      '  redis.call(''ZREM'', KEYS[1], due[i]) ' +
      'end ' +
      'return result';
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FClient: IHefestoRedisClient;
    function GetClient: IHefestoRedisClient;
    function Serialize(const AEntry: THefestoScheduledEntry): string;
    function Deserialize(const AValue: string): THefestoScheduledEntry;
    function DueAtScore(const ADueAt: TDateTime): string;
  public
    constructor Create(const AClient: IHefestoRedisClient); overload;
    constructor Create; overload;

    class function New(
      const AClient: IHefestoRedisClient): IHefestoScheduledStore; overload; static;
    class function New: THefestoRedis4DScheduledStore; overload; static;

    function ConnectionString(
      const AValue: string): THefestoRedis4DScheduledStore;
    function RedisClient(
      const AValue: IHefestoRedisClient): THefestoRedis4DScheduledStore;

    procedure Schedule(const AEntry: THefestoScheduledEntry);
    function PopDue(
      const ANow: TDateTime;
      const ALimit: Integer): TArray<THefestoScheduledEntry>;
    function Count: Integer;
    procedure Clear;
    function List: TArray<THefestoScheduledEntry>;
    procedure Delete(const AQueueName, AAction: string; const ADueAt: TDateTime);
  end;

implementation

uses
  System.SysUtils,
  System.DateUtils,
  Hefesto.Redis4D.Client;

function THefestoRedis4DScheduledStore.GetClient: IHefestoRedisClient;
begin
  if not Assigned(FClient) then
    raise EArgumentException.Create(
      'No Redis client configured. Call .ConnectionString(...) or .RedisClient(...).');
  Result := FClient;
end;

function THefestoRedis4DScheduledStore.Serialize(
  const AEntry: THefestoScheduledEntry): string;
var
  LIndex: Integer;
  LParts: TArray<string>;
  LBase: Integer;
begin
  LBase := 4;
  SetLength(LParts, LBase + Length(AEntry.Attributes));
  LParts[0] := AEntry.QueueName;
  LParts[1] := AEntry.Action;
  LParts[2] := AEntry.Body;
  LParts[3] := DueAtScore(AEntry.DueAt);
  for LIndex := 0 to Pred(Length(AEntry.Attributes)) do
    LParts[LBase + LIndex] :=
      AEntry.Attributes[LIndex].Key + ATTR_SEP + AEntry.Attributes[LIndex].Value;
  Result := String.Join(FIELD_SEP, LParts);
end;

function THefestoRedis4DScheduledStore.Deserialize(
  const AValue: string): THefestoScheduledEntry;
var
  LParts: TArray<string>;
  LAttrParts: TArray<string>;
  LIndex: Integer;
  LAttrCount: Integer;
begin
  LParts := AValue.Split([FIELD_SEP]);
  if Length(LParts) < 4 then
    Exit;

  Result.QueueName := LParts[0];
  Result.Action    := LParts[1];
  Result.Body      := LParts[2];
  Result.DueAt     := UnixToDateTime(StrToInt64Def(LParts[3], 0), False);

  LAttrCount := Length(LParts) - 4;
  SetLength(Result.Attributes, LAttrCount);
  for LIndex := 0 to Pred(LAttrCount) do
  begin
    LAttrParts := LParts[4 + LIndex].Split([ATTR_SEP]);
    if Length(LAttrParts) >= 2 then
    begin
      Result.Attributes[LIndex].Key   := LAttrParts[0];
      Result.Attributes[LIndex].Value := LAttrParts[1];
    end
    else if Length(LAttrParts) = 1 then
    begin
      Result.Attributes[LIndex].Key   := LAttrParts[0];
      Result.Attributes[LIndex].Value := '';
    end;
  end;
end;

function THefestoRedis4DScheduledStore.DueAtScore(
  const ADueAt: TDateTime): string;
begin
  Result := IntToStr(DateTimeToUnix(ADueAt, False));
end;

constructor THefestoRedis4DScheduledStore.Create(const AClient: IHefestoRedisClient);
begin
  inherited Create;
  FClient := AClient;
end;

constructor THefestoRedis4DScheduledStore.Create;
begin
  inherited Create;
end;

class function THefestoRedis4DScheduledStore.New(
  const AClient: IHefestoRedisClient): IHefestoScheduledStore;
begin
  Result := THefestoRedis4DScheduledStore.Create(AClient);
end;

class function THefestoRedis4DScheduledStore.New: THefestoRedis4DScheduledStore;
begin
  Result := THefestoRedis4DScheduledStore.Create;
end;

function THefestoRedis4DScheduledStore.ConnectionString(
  const AValue: string): THefestoRedis4DScheduledStore;
begin
  Result := Self;
  FClient := TRedis4DClientBridge.NewFromConnectionString(AValue);
end;

function THefestoRedis4DScheduledStore.RedisClient(
  const AValue: IHefestoRedisClient): THefestoRedis4DScheduledStore;
begin
  Result := Self;
  FClient := AValue;
end;

procedure THefestoRedis4DScheduledStore.Schedule(
  const AEntry: THefestoScheduledEntry);
var
  LScore: Double;
begin
  LScore := DateTimeToUnix(AEntry.DueAt, False);
  GetClient.ZAdd(SCHEDULED_KEY, LScore, Serialize(AEntry));
end;

function THefestoRedis4DScheduledStore.PopDue(
  const ANow: TDateTime;
  const ALimit: Integer): TArray<THefestoScheduledEntry>;
var
  LRaw: TArray<string>;
  LIndex: Integer;
  LLimit: Integer;
begin
  LLimit := ALimit;
  if LLimit <= 0 then
    LLimit := 500;

  LRaw := GetClient.Eval(
    LUA_POP_DUE,
    [SCHEDULED_KEY],
    [DueAtScore(ANow), IntToStr(LLimit)]);

  SetLength(Result, Length(LRaw));
  for LIndex := 0 to Pred(Length(LRaw)) do
    Result[LIndex] := Deserialize(LRaw[LIndex]);
end;

function THefestoRedis4DScheduledStore.Count: Integer;
begin
  Result := GetClient.ZCard(SCHEDULED_KEY);
end;

procedure THefestoRedis4DScheduledStore.Clear;
begin
  GetClient.Del(SCHEDULED_KEY);
end;

function THefestoRedis4DScheduledStore.List: TArray<THefestoScheduledEntry>;
var
  LRaw  : TArray<string>;
  LIndex: Integer;
begin
  LRaw := GetClient.Eval(LUA_LIST, [SCHEDULED_KEY], []);
  SetLength(Result, Length(LRaw));
  for LIndex := 0 to Pred(Length(LRaw)) do
    Result[LIndex] := Deserialize(LRaw[LIndex]);
end;

procedure THefestoRedis4DScheduledStore.Delete(
  const AQueueName, AAction: string; const ADueAt: TDateTime);
begin
  GetClient.Eval(
    LUA_DELETE_BY_SCORE_PREFIX,
    [SCHEDULED_KEY],
    [DueAtScore(ADueAt), AQueueName, AAction]);
end;

end.
