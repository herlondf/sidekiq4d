unit Hefesto.Middleware.CircuitBreaker;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  System.DateUtils,
  System.Generics.Collections,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Middleware;

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
  //     THefestoCircuitBreakerMiddleware.New
  //       .FailureThreshold(5)
  //       .CooldownSeconds(60));
  THefestoCircuitBreakerMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
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

    class function New: THefestoCircuitBreakerMiddleware;

    function FailureThreshold(AValue: Integer): THefestoCircuitBreakerMiddleware;
    function CooldownSeconds(AValue: Integer): THefestoCircuitBreakerMiddleware;

    // IHefestoServerMiddleware
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);

    // Consulta
    function CircuitStateFor(const AAction: string): TCircuitState;
    function FailureCountFor(const AAction: string): Integer;
  end;

implementation

{ THefestoCircuitBreakerMiddleware }

constructor THefestoCircuitBreakerMiddleware.Create;
begin
  inherited Create;
  FFailureThreshold := 5;
  FCooldownSeconds := 60;
  FLock := TCriticalSection.Create;
  FCircuits := TDictionary<string, TCircuitState>.Create;
  FFailureCounts := TDictionary<string, Integer>.Create;
  FLastFailureTimes := TDictionary<string, TDateTime>.Create;
end;

destructor THefestoCircuitBreakerMiddleware.Destroy;
begin
  FLastFailureTimes.Free;
  FFailureCounts.Free;
  FCircuits.Free;
  FLock.Free;
  inherited;
end;

class function THefestoCircuitBreakerMiddleware.New: THefestoCircuitBreakerMiddleware;
begin
  Result := THefestoCircuitBreakerMiddleware.Create;
end;

function THefestoCircuitBreakerMiddleware.FailureThreshold(
  AValue: Integer): THefestoCircuitBreakerMiddleware;
begin
  Result := Self;
  FFailureThreshold := AValue;
end;

function THefestoCircuitBreakerMiddleware.CooldownSeconds(
  AValue: Integer): THefestoCircuitBreakerMiddleware;
begin
  Result := Self;
  FCooldownSeconds := AValue;
end;

function THefestoCircuitBreakerMiddleware.GetStateLocked(
  const AAction: string): TCircuitState;
begin
  // Caller must hold FLock.
  if not FCircuits.TryGetValue(AAction, Result) then
    Result := csClosed;
end;

function THefestoCircuitBreakerMiddleware.GetState(
  const AAction: string): TCircuitState;
begin
  FLock.Enter;
  try
    Result := GetStateLocked(AAction);
  finally
    FLock.Leave;
  end;
end;

function THefestoCircuitBreakerMiddleware.IsOpenAndCoolingDown(
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

procedure THefestoCircuitBreakerMiddleware.RecordSuccess(const AAction: string);
begin
  FLock.Enter;
  try
    FCircuits.AddOrSetValue(AAction, csClosed);
    FFailureCounts.AddOrSetValue(AAction, 0);
  finally
    FLock.Leave;
  end;
end;

procedure THefestoCircuitBreakerMiddleware.RecordFailure(const AAction: string);
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

procedure THefestoCircuitBreakerMiddleware.Call(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ANext: THefestoNextProc);
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

function THefestoCircuitBreakerMiddleware.CircuitStateFor(
  const AAction: string): TCircuitState;
begin
  Result := GetState(AAction);
end;

function THefestoCircuitBreakerMiddleware.FailureCountFor(
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
