unit Hefesto.Retry;

interface

uses
  System.SysUtils,
  Hefesto.Job;

type
  THefestoRetryDecisionKind = (rdRetry, rdDiscard, rdDeadLetter, rdReraise);

  THefestoRetryDecision = record
    Kind: THefestoRetryDecisionKind;
    DelaySeconds: Integer;
    Reason: string;

    class function Retry(const ADelaySeconds: Integer; const AReason: string): THefestoRetryDecision; static;
    class function DeadLetter(const AReason: string): THefestoRetryDecision; static;
  end;

  IHefestoRetryPolicy = interface
    ['{7F127C8E-8494-45B0-A527-714A1D70B47E}']
    function Decide(const AJob: IHefestoJobEnvelope; const AError: Exception): THefestoRetryDecision;
  end;

  THefestoSimpleRetryPolicy = class(TInterfacedObject, IHefestoRetryPolicy)
  private
    FMaxAttempts: Integer;
    FDelaySeconds: Integer;
  public
    constructor Create(const AMaxAttempts, ADelaySeconds: Integer);
    class function New(
      const AMaxAttempts: Integer = 3;
      const ADelaySeconds: Integer = 30): IHefestoRetryPolicy;

    function Decide(const AJob: IHefestoJobEnvelope; const AError: Exception): THefestoRetryDecision;
  end;

  { Delay = BaseSeconds * (Attempt ^ 2), com teto em MaxDelaySeconds }
  THefestoExponentialRetryPolicy = class(TInterfacedObject, IHefestoRetryPolicy)
  private
    FMaxAttempts:   Integer;
    FBaseSeconds:   Integer;
    FMaxDelaySeconds: Integer;
  public
    constructor Create(const AMaxAttempts, ABaseSeconds, AMaxDelaySeconds: Integer);
    class function New(
      const AMaxAttempts:    Integer = 5;
      const ABaseSeconds:    Integer = 15;
      const AMaxDelaySeconds: Integer = 3600): IHefestoRetryPolicy;

    function Decide(const AJob: IHefestoJobEnvelope; const AError: Exception): THefestoRetryDecision;
  end;

implementation

{ THefestoRetryDecision }

class function THefestoRetryDecision.DeadLetter(
  const AReason: string): THefestoRetryDecision;
begin
  Result.Kind := rdDeadLetter;
  Result.DelaySeconds := 0;
  Result.Reason := AReason;
end;

class function THefestoRetryDecision.Retry(
  const ADelaySeconds: Integer;
  const AReason: string): THefestoRetryDecision;
begin
  Result.Kind := rdRetry;
  Result.DelaySeconds := ADelaySeconds;
  Result.Reason := AReason;
end;

{ THefestoSimpleRetryPolicy }

constructor THefestoSimpleRetryPolicy.Create(
  const AMaxAttempts,
  ADelaySeconds: Integer);
begin
  inherited Create;
  FMaxAttempts := AMaxAttempts;
  FDelaySeconds := ADelaySeconds;
end;

function THefestoSimpleRetryPolicy.Decide(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception): THefestoRetryDecision;
begin
  if AJob.Attempts < FMaxAttempts then
    Exit(THefestoRetryDecision.Retry(FDelaySeconds, AError.Message));

  Result := THefestoRetryDecision.DeadLetter(AError.Message);
end;

class function THefestoSimpleRetryPolicy.New(
  const AMaxAttempts,
  ADelaySeconds: Integer): IHefestoRetryPolicy;
begin
  Result := THefestoSimpleRetryPolicy.Create(AMaxAttempts, ADelaySeconds);
end;

{ THefestoExponentialRetryPolicy }

constructor THefestoExponentialRetryPolicy.Create(
  const AMaxAttempts, ABaseSeconds, AMaxDelaySeconds: Integer);
begin
  inherited Create;
  FMaxAttempts     := AMaxAttempts;
  FBaseSeconds     := ABaseSeconds;
  FMaxDelaySeconds := AMaxDelaySeconds;
end;

class function THefestoExponentialRetryPolicy.New(
  const AMaxAttempts, ABaseSeconds, AMaxDelaySeconds: Integer): IHefestoRetryPolicy;
begin
  Result := THefestoExponentialRetryPolicy.Create(
    AMaxAttempts, ABaseSeconds, AMaxDelaySeconds);
end;

function THefestoExponentialRetryPolicy.Decide(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception): THefestoRetryDecision;
var
  LDelay: Integer;
begin
  if AJob.Attempts >= FMaxAttempts then
    Exit(THefestoRetryDecision.DeadLetter(AError.Message));

  LDelay := FBaseSeconds * (AJob.Attempts * AJob.Attempts);
  if LDelay > FMaxDelaySeconds then
    LDelay := FMaxDelaySeconds;
  if LDelay < 1 then
    LDelay := 1;

  Result := THefestoRetryDecision.Retry(LDelay, AError.Message);
end;

end.
