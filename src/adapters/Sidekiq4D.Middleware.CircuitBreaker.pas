unit Sidekiq4D.Middleware.CircuitBreaker;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  System.DateUtils,
  System.Generics.Collections,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Middleware;

type
  TCircuitState = (csClosed, csOpen, csHalfOpen);

  // Circuit breaker por action (target/provider).
  //
  // Quando um handler falha consecutivamente N vezes para uma action,
  // o circuito abre e rejeita jobs dessa action por CooldownSeconds.
  // Apos o cooldown, permite 1 tentativa (half-open). Se sucede, fecha.
  //
  // Uso:
  //   Server.UseServerMiddleware(
  //     TSidekiqCircuitBreakerMiddleware.New
  //       .FailureThreshold(5)
  //       .CooldownSeconds(60));
  TSidekiqCircuitBreakerMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
  private
    FFailureThreshold: Integer;
    FCooldownSeconds: Integer;
    FLock: TCriticalSection;
    FCircuits: TDictionary<string, TCircuitState>;
    FFailureCounts: TDictionary<string, Integer>;
    FLastFailureTimes: TDictionary<string, TDateTime>;

    // Reads FCircuits WITHOUT acquiring FLock — caller must hold FLock.
    function GetStateLocked(const AAction: string): TCircuitState;
    function GetState(const AAction: string): TCircuitState;
    function IsOpenAndCoolingDown(const AAction: string): Boolean;
    procedure RecordSuccess(const AAction: string);
    procedure RecordFailure(const AAction: string);
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqCircuitBreakerMiddleware;

    function FailureThreshold(AValue: Integer): TSidekiqCircuitBreakerMiddleware;
    function CooldownSeconds(AValue: Integer): TSidekiqCircuitBreakerMiddleware;

    // ISidekiqServerMiddleware
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);

    // Consulta
    function CircuitStateFor(const AAction: string): TCircuitState;
    function FailureCountFor(const AAction: string): Integer;
  end;

implementation

{ TSidekiqCircuitBreakerMiddleware }

constructor TSidekiqCircuitBreakerMiddleware.Create;
begin
  inherited Create;
  FFailureThreshold := 5;
  FCooldownSeconds := 60;
  FLock := TCriticalSection.Create;
  FCircuits := TDictionary<string, TCircuitState>.Create;
  FFailureCounts := TDictionary<string, Integer>.Create;
  FLastFailureTimes := TDictionary<string, TDateTime>.Create;
end;

destructor TSidekiqCircuitBreakerMiddleware.Destroy;
begin
  FLastFailureTimes.Free;
  FFailureCounts.Free;
  FCircuits.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqCircuitBreakerMiddleware.New: TSidekiqCircuitBreakerMiddleware;
begin
  Result := TSidekiqCircuitBreakerMiddleware.Create;
end;

function TSidekiqCircuitBreakerMiddleware.FailureThreshold(
  AValue: Integer): TSidekiqCircuitBreakerMiddleware;
begin
  Result := Self;
  FFailureThreshold := AValue;
end;

function TSidekiqCircuitBreakerMiddleware.CooldownSeconds(
  AValue: Integer): TSidekiqCircuitBreakerMiddleware;
begin
  Result := Self;
  FCooldownSeconds := AValue;
end;

function TSidekiqCircuitBreakerMiddleware.GetStateLocked(
  const AAction: string): TCircuitState;
begin
  // Caller must hold FLock.
  if not FCircuits.TryGetValue(AAction, Result) then
    Result := csClosed;
end;

function TSidekiqCircuitBreakerMiddleware.GetState(
  const AAction: string): TCircuitState;
begin
  FLock.Enter;
  try
    Result := GetStateLocked(AAction);
  finally
    FLock.Leave;
  end;
end;

function TSidekiqCircuitBreakerMiddleware.IsOpenAndCoolingDown(
  const AAction: string): Boolean;
var
  LLastFailure: TDateTime;
begin
  Result := False;
  FLock.Enter;
  try
    // Use GetStateLocked — FLock is already held here; calling GetState() would deadlock.
    if GetStateLocked(AAction) <> csOpen then
      Exit(False);
    if FLastFailureTimes.TryGetValue(AAction, LLastFailure) then
    begin
      if SecondsBetween(Now, LLastFailure) >= FCooldownSeconds then
      begin
        // Cooldown expirou -> half-open
        FCircuits.AddOrSetValue(AAction, csHalfOpen);
        Exit(False);
      end;
    end;
    Result := True; // Ainda em cooldown
  finally
    FLock.Leave;
  end;
end;

procedure TSidekiqCircuitBreakerMiddleware.RecordSuccess(const AAction: string);
begin
  FLock.Enter;
  try
    FCircuits.AddOrSetValue(AAction, csClosed);
    FFailureCounts.AddOrSetValue(AAction, 0);
  finally
    FLock.Leave;
  end;
end;

procedure TSidekiqCircuitBreakerMiddleware.RecordFailure(const AAction: string);
var
  LCount: Integer;
begin
  FLock.Enter;
  try
    if not FFailureCounts.TryGetValue(AAction, LCount) then
      LCount := 0;
    Inc(LCount);
    FFailureCounts.AddOrSetValue(AAction, LCount);
    FLastFailureTimes.AddOrSetValue(AAction, Now);

    if LCount >= FFailureThreshold then
      FCircuits.AddOrSetValue(AAction, csOpen);
  finally
    FLock.Leave;
  end;
end;

procedure TSidekiqCircuitBreakerMiddleware.Call(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ANext: TSidekiqNextProc);
var
  LAction: string;
begin
  LAction := AJob.Action;

  // Circuito aberto e em cooldown -> rejeita (nack para retry depois)
  if IsOpenAndCoolingDown(LAction) then
  begin
    AQueue.Nack(AJob, FCooldownSeconds);
    Exit;
  end;

  // Executa o job
  try
    ANext;
    RecordSuccess(LAction);
  except
    on E: Exception do
    begin
      RecordFailure(LAction);
      raise; // Re-raise para o executor tratar (retry policy)
    end;
  end;
end;

function TSidekiqCircuitBreakerMiddleware.CircuitStateFor(
  const AAction: string): TCircuitState;
begin
  Result := GetState(AAction);
end;

function TSidekiqCircuitBreakerMiddleware.FailureCountFor(
  const AAction: string): Integer;
begin
  FLock.Enter;
  try
    if not FFailureCounts.TryGetValue(AAction, Result) then
      Result := 0;
  finally
    FLock.Leave;
  end;
end;

end.
