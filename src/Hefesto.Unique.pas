unit Hefesto.Unique;

interface

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Metadata;

type
  THefestoUniqueStrategy = (
    us_UntilExecuted,
    us_UntilAndWhileExecuting,
    us_UntilExpired
  );

  THefestoUniqueStrategyHelper = record helper for THefestoUniqueStrategy
  public const
    ValueUntilExecuted = 'until_executed';
    ValueUntilAndWhileExecuting = 'until_and_while_executing';
    ValueUntilExpired = 'until_expired';
  public
    function AsString: string;
    class function TryParse(const AValue: string;
      out AStrategy: THefestoUniqueStrategy): Boolean; static;
    class function FromJob(const AJob: IHefestoJobEnvelope): THefestoUniqueStrategy; static;
  end;

implementation

{ THefestoUniqueStrategyHelper }

function THefestoUniqueStrategyHelper.AsString: string;
begin
  case Self of
    us_UntilAndWhileExecuting: Result := ValueUntilAndWhileExecuting;
    us_UntilExpired: Result := ValueUntilExpired;
  else
    Result := ValueUntilExecuted;
  end;
end;

class function THefestoUniqueStrategyHelper.FromJob(
  const AJob: IHefestoJobEnvelope): THefestoUniqueStrategy;
var
  LRaw: string;
begin
  LRaw := EmptyStr;
  if Assigned(AJob) then
    LRaw := AJob.Attribute(THefestoJobAttribute.UniqueStrategy);

  if not TryParse(LRaw, Result) then
    Result := us_UntilExecuted;
end;

class function THefestoUniqueStrategyHelper.TryParse(
  const AValue: string;
  out AStrategy: THefestoUniqueStrategy): Boolean;
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
