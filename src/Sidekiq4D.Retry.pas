unit Sidekiq4D.Retry;

interface

uses
  System.SysUtils,
  Sidekiq4D.Job;

type
  TSidekiqRetryDecisionKind = (rdRetry, rdDiscard, rdDeadLetter, rdReraise);

  TSidekiqRetryDecision = record
    Kind: TSidekiqRetryDecisionKind;
    DelaySeconds: Integer;
    Reason: string;

    class function Retry(const ADelaySeconds: Integer; const AReason: string): TSidekiqRetryDecision; static;
    class function DeadLetter(const AReason: string): TSidekiqRetryDecision; static;
  end;

  ISidekiqRetryPolicy = interface
    ['{7F127C8E-8494-45B0-A527-714A1D70B47E}']
    function Decide(const AJob: ISidekiqJobEnvelope; const AError: Exception): TSidekiqRetryDecision;
  end;

  TSidekiqSimpleRetryPolicy = class(TInterfacedObject, ISidekiqRetryPolicy)
  private
    FMaxAttempts: Integer;
    FDelaySeconds: Integer;
  public
    constructor Create(const AMaxAttempts, ADelaySeconds: Integer);
    class function New(
      const AMaxAttempts: Integer = 3;
      const ADelaySeconds: Integer = 30): ISidekiqRetryPolicy;

    function Decide(const AJob: ISidekiqJobEnvelope; const AError: Exception): TSidekiqRetryDecision;
  end;

  { Delay = BaseSeconds * (Attempt ^ 2), com teto em MaxDelaySeconds }
  TSidekiqExponentialRetryPolicy = class(TInterfacedObject, ISidekiqRetryPolicy)
  private
    FMaxAttempts:   Integer;
    FBaseSeconds:   Integer;
    FMaxDelaySeconds: Integer;
  public
    constructor Create(const AMaxAttempts, ABaseSeconds, AMaxDelaySeconds: Integer);
    class function New(
      const AMaxAttempts:    Integer = 5;
      const ABaseSeconds:    Integer = 15;
      const AMaxDelaySeconds: Integer = 3600): ISidekiqRetryPolicy;

    function Decide(const AJob: ISidekiqJobEnvelope; const AError: Exception): TSidekiqRetryDecision;
  end;

implementation

{ TSidekiqRetryDecision }

class function TSidekiqRetryDecision.DeadLetter(
  const AReason: string): TSidekiqRetryDecision;
begin
  Result.Kind := rdDeadLetter;
  Result.DelaySeconds := 0;
  Result.Reason := AReason;
end;

class function TSidekiqRetryDecision.Retry(
  const ADelaySeconds: Integer;
  const AReason: string): TSidekiqRetryDecision;
begin
  Result.Kind := rdRetry;
  Result.DelaySeconds := ADelaySeconds;
  Result.Reason := AReason;
end;

{ TSidekiqSimpleRetryPolicy }

constructor TSidekiqSimpleRetryPolicy.Create(
  const AMaxAttempts,
  ADelaySeconds: Integer);
begin
  inherited Create;
  FMaxAttempts := AMaxAttempts;
  FDelaySeconds := ADelaySeconds;
end;

function TSidekiqSimpleRetryPolicy.Decide(
  const AJob: ISidekiqJobEnvelope;
  const AError: Exception): TSidekiqRetryDecision;
begin
  if AJob.Attempts < FMaxAttempts then
    Exit(TSidekiqRetryDecision.Retry(FDelaySeconds, AError.Message));

  Result := TSidekiqRetryDecision.DeadLetter(AError.Message);
end;

class function TSidekiqSimpleRetryPolicy.New(
  const AMaxAttempts,
  ADelaySeconds: Integer): ISidekiqRetryPolicy;
begin
  Result := TSidekiqSimpleRetryPolicy.Create(AMaxAttempts, ADelaySeconds);
end;

{ TSidekiqExponentialRetryPolicy }

constructor TSidekiqExponentialRetryPolicy.Create(
  const AMaxAttempts, ABaseSeconds, AMaxDelaySeconds: Integer);
begin
  inherited Create;
  FMaxAttempts     := AMaxAttempts;
  FBaseSeconds     := ABaseSeconds;
  FMaxDelaySeconds := AMaxDelaySeconds;
end;

class function TSidekiqExponentialRetryPolicy.New(
  const AMaxAttempts, ABaseSeconds, AMaxDelaySeconds: Integer): ISidekiqRetryPolicy;
begin
  Result := TSidekiqExponentialRetryPolicy.Create(
    AMaxAttempts, ABaseSeconds, AMaxDelaySeconds);
end;

function TSidekiqExponentialRetryPolicy.Decide(
  const AJob: ISidekiqJobEnvelope;
  const AError: Exception): TSidekiqRetryDecision;
var
  LDelay: Integer;
begin
  if AJob.Attempts >= FMaxAttempts then
    Exit(TSidekiqRetryDecision.DeadLetter(AError.Message));

  LDelay := FBaseSeconds * (AJob.Attempts * AJob.Attempts);
  if LDelay > FMaxDelaySeconds then
    LDelay := FMaxDelaySeconds;
  if LDelay < 1 then
    LDelay := 1;

  Result := TSidekiqRetryDecision.Retry(LDelay, AError.Message);
end;

end.
