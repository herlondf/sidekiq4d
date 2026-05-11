unit Sidekiq4D.RateLimit;

interface

uses
  System.SysUtils,
  System.Math,
  System.DateUtils,
  System.SyncObjs,
  Sidekiq4D.Store.Interfaces;

type
  ESidekiqRateLimiter = class(Exception);

  ISidekiqRateLimiter = interface
    ['{C7E02E0A-8AB4-4A91-BE9F-9376C7AA7B77}']
    function TryAcquire(
      const AKey: string;
      const ACapacity, AIntervalSeconds, ACost: Integer;
      out ARetryAfterSeconds: Integer): Boolean;
  end;

  TSidekiqNoopRateLimiter = class(TInterfacedObject, ISidekiqRateLimiter)
  public
    class function New: ISidekiqRateLimiter;
    function TryAcquire(
      const AKey: string;
      const ACapacity, AIntervalSeconds, ACost: Integer;
      out ARetryAfterSeconds: Integer): Boolean;
  end;

  TSidekiqTokenBucketRateLimiter = class(TInterfacedObject, ISidekiqRateLimiter)
  private type
    TTokenBucketState = record
      TokensMilli: Int64;
      LastRefillTick: Int64;
    end;
  private const
    TokenScale = 1000;
  private
    FStateStore: ISidekiqStateStore;
    FLock: TCriticalSection;

    class function EmptyState(
      const ACapacity: Integer;
      const ANowTick: Int64): TTokenBucketState; static;
    class function LoadState(
      const ASerialized: string;
      const ACapacity: Integer;
      const ANowTick: Int64): TTokenBucketState; static;
    class function NowTick: Int64; static;
    class function RefillTokens(
      const AState: TTokenBucketState;
      const ACapacity, AIntervalSeconds: Integer;
      const ANowTick: Int64): TTokenBucketState; static;
    class function SerializeState(
      const AState: TTokenBucketState): string; static;
    class function StateKey(const AKey: string): string; static;
  public
    constructor Create(const AStateStore: ISidekiqStateStore);
    destructor Destroy; override;

    class function New(const AStateStore: ISidekiqStateStore): ISidekiqRateLimiter;

    function TryAcquire(
      const AKey: string;
      const ACapacity, AIntervalSeconds, ACost: Integer;
      out ARetryAfterSeconds: Integer): Boolean;
  end;

implementation

{ TSidekiqNoopRateLimiter }

class function TSidekiqNoopRateLimiter.New: ISidekiqRateLimiter;
begin
  Result := TSidekiqNoopRateLimiter.Create;
end;

function TSidekiqNoopRateLimiter.TryAcquire(
  const AKey: string;
  const ACapacity, AIntervalSeconds, ACost: Integer;
  out ARetryAfterSeconds: Integer): Boolean;
begin
  ARetryAfterSeconds := 0;
  Result := True;
end;

{ TSidekiqTokenBucketRateLimiter }

constructor TSidekiqTokenBucketRateLimiter.Create(
  const AStateStore: ISidekiqStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
  FLock := TCriticalSection.Create;
end;

destructor TSidekiqTokenBucketRateLimiter.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function TSidekiqTokenBucketRateLimiter.EmptyState(
  const ACapacity: Integer;
  const ANowTick: Int64): TTokenBucketState;
begin
  Result.TokensMilli := ACapacity * TokenScale;
  Result.LastRefillTick := ANowTick;
end;

class function TSidekiqTokenBucketRateLimiter.LoadState(
  const ASerialized: string;
  const ACapacity: Integer;
  const ANowTick: Int64): TTokenBucketState;
var
  LParts: TArray<string>;
begin
  if ASerialized.Trim.IsEmpty then
    Exit(EmptyState(ACapacity, ANowTick));

  LParts := ASerialized.Split(['|']);
  if Length(LParts) <> 2 then
    Exit(EmptyState(ACapacity, ANowTick));

  Result.TokensMilli := StrToInt64Def(LParts[0], ACapacity * TokenScale);
  Result.LastRefillTick := StrToInt64Def(LParts[1], ANowTick);
  if Result.TokensMilli < 0 then
    Result.TokensMilli := 0;
  if Result.TokensMilli > (ACapacity * TokenScale) then
    Result.TokensMilli := ACapacity * TokenScale;
end;

class function TSidekiqTokenBucketRateLimiter.New(
  const AStateStore: ISidekiqStateStore): ISidekiqRateLimiter;
begin
  Result := TSidekiqTokenBucketRateLimiter.Create(AStateStore);
end;

class function TSidekiqTokenBucketRateLimiter.NowTick: Int64;
const
  UnixEpoch: TDateTime = 25569.0;
begin
  Result := Round((Now - UnixEpoch) * MSecsPerDay);
end;

class function TSidekiqTokenBucketRateLimiter.RefillTokens(
  const AState: TTokenBucketState;
  const ACapacity, AIntervalSeconds: Integer;
  const ANowTick: Int64): TTokenBucketState;
var
  LElapsedMs: Int64;
  LRefillMilli: Int64;
  LWindowMs: Int64;
  LCapacityMilli: Int64;
begin
  Result := AState;
  if ANowTick <= Result.LastRefillTick then
    Exit;

  LElapsedMs := ANowTick - Result.LastRefillTick;
  LWindowMs := Int64(AIntervalSeconds) * 1000;
  if LWindowMs <= 0 then
    Exit;

  LCapacityMilli := Int64(ACapacity) * TokenScale;
  LRefillMilli := (LElapsedMs * LCapacityMilli) div LWindowMs;
  if LRefillMilli > 0 then
  begin
    Result.TokensMilli := Min(LCapacityMilli, Result.TokensMilli + LRefillMilli);
    Result.LastRefillTick := ANowTick;
  end;
end;

class function TSidekiqTokenBucketRateLimiter.SerializeState(
  const AState: TTokenBucketState): string;
begin
  Result := IntToStr(AState.TokensMilli) + '|' + IntToStr(AState.LastRefillTick);
end;

class function TSidekiqTokenBucketRateLimiter.StateKey(const AKey: string): string;
begin
  Result := 'rate_limit:' + AKey;
end;

function TSidekiqTokenBucketRateLimiter.TryAcquire(
  const AKey: string;
  const ACapacity, AIntervalSeconds, ACost: Integer;
  out ARetryAfterSeconds: Integer): Boolean;
var
  LStateKey: string;
  LState: TTokenBucketState;
  LSerialized: string;
  LNowTick: Int64;
  LRequiredMilli: Int64;
  LMissingMilli: Int64;
  LWindowMs: Int64;
begin
  if AKey.Trim.IsEmpty then
    raise ESidekiqRateLimiter.Create('Rate limit key is required.');
  if ACapacity <= 0 then
    raise ESidekiqRateLimiter.Create('Rate limit capacity must be greater than zero.');
  if AIntervalSeconds <= 0 then
    raise ESidekiqRateLimiter.Create('Rate limit interval must be greater than zero.');
  if ACost <= 0 then
    raise ESidekiqRateLimiter.Create('Rate limit cost must be greater than zero.');
  if ACost > ACapacity then
    raise ESidekiqRateLimiter.Create('Rate limit cost cannot be greater than capacity.');
  if not Assigned(FStateStore) then
    raise ESidekiqRateLimiter.Create('Rate limiter requires an active state store.');

  ARetryAfterSeconds := 0;
  LRequiredMilli := Int64(ACost) * TokenScale;
  LStateKey := StateKey(AKey);
  LNowTick := NowTick;

  FLock.Acquire;
  try
    LSerialized := FStateStore.Get(LStateKey);
    LState := LoadState(LSerialized, ACapacity, LNowTick);
    LState := RefillTokens(LState, ACapacity, AIntervalSeconds, LNowTick);

    if LState.TokensMilli >= LRequiredMilli then
    begin
      LState.TokensMilli := LState.TokensMilli - LRequiredMilli;
      FStateStore.Put(LStateKey, SerializeState(LState), Max(AIntervalSeconds * 2, 60));
      Exit(True);
    end;

    LMissingMilli := LRequiredMilli - LState.TokensMilli;
    LWindowMs := Int64(AIntervalSeconds) * 1000;
    ARetryAfterSeconds := Ceil(LMissingMilli * LWindowMs / (Int64(ACapacity) * TokenScale * 1000));
    if ARetryAfterSeconds <= 0 then
      ARetryAfterSeconds := 1;

    FStateStore.Put(LStateKey, SerializeState(LState), Max(AIntervalSeconds * 2, 60));
    Result := False;
  finally
    FLock.Release;
  end;
end;

end.
