unit Sidekiq4D.Unique;

interface

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Metadata;

type
  TSidekiqUniqueStrategy = (
    us_UntilExecuted,
    us_UntilAndWhileExecuting,
    us_UntilExpired
  );

  TSidekiqUniqueStrategyHelper = record helper for TSidekiqUniqueStrategy
  public const
    ValueUntilExecuted = 'until_executed';
    ValueUntilAndWhileExecuting = 'until_and_while_executing';
    ValueUntilExpired = 'until_expired';
  public
    function AsString: string;
    class function TryParse(const AValue: string;
      out AStrategy: TSidekiqUniqueStrategy): Boolean; static;
    class function FromJob(const AJob: ISidekiqJobEnvelope): TSidekiqUniqueStrategy; static;
  end;

implementation

{ TSidekiqUniqueStrategyHelper }

function TSidekiqUniqueStrategyHelper.AsString: string;
begin
  case Self of
    us_UntilAndWhileExecuting: Result := ValueUntilAndWhileExecuting;
    us_UntilExpired: Result := ValueUntilExpired;
  else
    Result := ValueUntilExecuted;
  end;
end;

class function TSidekiqUniqueStrategyHelper.FromJob(
  const AJob: ISidekiqJobEnvelope): TSidekiqUniqueStrategy;
var
  LRaw: string;
begin
  LRaw := EmptyStr;
  if Assigned(AJob) then
    LRaw := AJob.Attribute(TSidekiqJobAttribute.UniqueStrategy);

  if not TryParse(LRaw, Result) then
    Result := us_UntilExecuted;
end;

class function TSidekiqUniqueStrategyHelper.TryParse(
  const AValue: string;
  out AStrategy: TSidekiqUniqueStrategy): Boolean;
var
  LValue: string;
begin
  LValue := AValue.Trim.ToLower;
  Result := True;
  if SameText(LValue, ValueUntilAndWhileExecuting) then
    AStrategy := us_UntilAndWhileExecuting
  else if SameText(LValue, ValueUntilExpired) then
    AStrategy := us_UntilExpired
  else if SameText(LValue, ValueUntilExecuted) then
    AStrategy := us_UntilExecuted
  else
    Result := False;
end;

end.
