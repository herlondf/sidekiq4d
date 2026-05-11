unit Sidekiq4D.Periodic;

interface

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.Generics.Collections;

type
  ESidekiqPeriodic = class(Exception);

  TSidekiqPeriodicDefinition = record
    Name: string;
    CronExpression: string;
    QueueName: string;
    Action: string;
    Body: string;
    Attributes: TArray<TPair<string, string>>;
  end;

  TSidekiqCronSchedule = class
  private
    class function MatchField(
      const AField: string;
      const AValue, AMin, AMax: Integer): Boolean; static;
    class function MatchSegment(
      const ASegment: string;
      const AValue, AMin, AMax: Integer): Boolean; static;
    class function ParseNumber(
      const AValue: string;
      const AMin, AMax: Integer;
      const AFieldName: string): Integer; static;
  public
    class function Matches(
      const AExpression: string;
      const AValue: TDateTime): Boolean; static;
    class function TruncateToMinute(const AValue: TDateTime): TDateTime; static;
  end;

function MakePeriodicDefinition(
  const AName, ACronExpression, AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): TSidekiqPeriodicDefinition;

procedure ApplyPeriodicAttributesToStrings(
  const ADefinition: TSidekiqPeriodicDefinition;
  const ATarget: TStrings);

implementation

procedure ApplyPeriodicAttributesToStrings(
  const ADefinition: TSidekiqPeriodicDefinition;
  const ATarget: TStrings);
var
  LIndex: Integer;
begin
  if not Assigned(ATarget) then
    Exit;

  for LIndex := 0 to Pred(Length(ADefinition.Attributes)) do
    ATarget.Values[ADefinition.Attributes[LIndex].Key] :=
      ADefinition.Attributes[LIndex].Value;
end;

function MakePeriodicDefinition(
  const AName, ACronExpression, AQueueName, AAction, ABody: string;
  const AAttributes: TStrings): TSidekiqPeriodicDefinition;
var
  LIndex: Integer;
  LCount: Integer;
  LName: string;
begin
  if AName.Trim.IsEmpty then
    raise ESidekiqPeriodic.Create('Periodic job name is required.');
  if ACronExpression.Trim.IsEmpty then
    raise ESidekiqPeriodic.Create('Periodic cron expression is required.');
  if AAction.Trim.IsEmpty then
    raise ESidekiqPeriodic.Create('Periodic action is required.');

  TSidekiqCronSchedule.Matches(
    ACronExpression,
    TSidekiqCronSchedule.TruncateToMinute(Now));

  Result.Name := AName;
  Result.CronExpression := ACronExpression;
  Result.QueueName := AQueueName;
  Result.Action := AAction;
  Result.Body := ABody;
  LCount := 0;
  if Assigned(AAttributes) then
  begin
    SetLength(Result.Attributes, AAttributes.Count);
    for LIndex := 0 to Pred(AAttributes.Count) do
    begin
      LName := AAttributes.Names[LIndex];
      if LName.IsEmpty then
        Continue;
      Result.Attributes[LCount].Key := LName;
      Result.Attributes[LCount].Value := AAttributes.ValueFromIndex[LIndex];
      Inc(LCount);
    end;
    SetLength(Result.Attributes, LCount);
  end
  else
    SetLength(Result.Attributes, 0);
end;

{ TSidekiqCronSchedule }

class function TSidekiqCronSchedule.MatchField(
  const AField: string;
  const AValue, AMin, AMax: Integer): Boolean;
var
  LSegment: string;
begin
  for LSegment in AField.Split([',']) do
    if MatchSegment(LSegment.Trim, AValue, AMin, AMax) then
      Exit(True);
  Result := False;
end;

class function TSidekiqCronSchedule.MatchSegment(
  const ASegment: string;
  const AValue, AMin, AMax: Integer): Boolean;
var
  LStepParts: TArray<string>;
  LRangeParts: TArray<string>;
  LRangeText: string;
  LStep: Integer;
  LStart: Integer;
  LFinish: Integer;
begin
  if ASegment.IsEmpty then
    raise ESidekiqPeriodic.Create('Invalid empty cron segment.');

  LStep := 1;
  LStepParts := ASegment.Split(['/']);
  if Length(LStepParts) > 2 then
    raise ESidekiqPeriodic.Create('Invalid cron step segment: ' + ASegment);

  LRangeText := LStepParts[0].Trim;
  if Length(LStepParts) = 2 then
  begin
    LStep := ParseNumber(LStepParts[1].Trim, 1, MaxInt, 'step');
    if LStep <= 0 then
      raise ESidekiqPeriodic.Create('Cron step must be greater than zero.');
  end;

  if (LRangeText = '*') or LRangeText.IsEmpty then
  begin
    LStart := AMin;
    LFinish := AMax;
  end
  else
  begin
    LRangeParts := LRangeText.Split(['-']);
    if Length(LRangeParts) = 1 then
    begin
      LStart := ParseNumber(LRangeText, AMin, AMax, 'value');
      LFinish := LStart;
    end
    else if Length(LRangeParts) = 2 then
    begin
      LStart := ParseNumber(LRangeParts[0].Trim, AMin, AMax, 'range start');
      LFinish := ParseNumber(LRangeParts[1].Trim, AMin, AMax, 'range end');
      if LFinish < LStart then
        raise ESidekiqPeriodic.Create('Cron range end cannot be lower than start.');
    end
    else
      raise ESidekiqPeriodic.Create('Invalid cron range segment: ' + ASegment);
  end;

  Result := (AValue >= LStart)
    and (AValue <= LFinish)
    and (((AValue - LStart) mod LStep) = 0);
end;

class function TSidekiqCronSchedule.Matches(
  const AExpression: string;
  const AValue: TDateTime): Boolean;
var
  LFields: TArray<string>;
  LMinute: Word;
  LHour: Word;
  LDay: Word;
  LMonth: Word;
  LSecond: Word;
  LMilli: Word;
  LDayOfWeek: Integer;
begin
  LFields := AExpression.Split([' '], TStringSplitOptions.ExcludeEmpty);
  if Length(LFields) <> 5 then
    raise ESidekiqPeriodic.Create(
      'Cron expression must contain 5 fields (minute hour day month weekday).');

  DecodeTime(AValue, LHour, LMinute, LSecond, LMilli);
  LDay := DayOfTheMonth(AValue);
  LMonth := MonthOfTheYear(AValue);
  LDayOfWeek := DayOfWeek(AValue) - 1;

  Result := MatchField(LFields[0], LMinute, 0, 59)
    and MatchField(LFields[1], LHour, 0, 23)
    and MatchField(LFields[2], LDay, 1, 31)
    and MatchField(LFields[3], LMonth, 1, 12)
    and MatchField(LFields[4], LDayOfWeek, 0, 6);
end;

class function TSidekiqCronSchedule.ParseNumber(
  const AValue: string;
  const AMin, AMax: Integer;
  const AFieldName: string): Integer;
begin
  Result := StrToIntDef(AValue, Low(Integer));
  if (Result < AMin) or (Result > AMax) then
    raise ESidekiqPeriodic.Create(
      Format('Invalid cron %s value: %s', [AFieldName, AValue]));
end;

class function TSidekiqCronSchedule.TruncateToMinute(
  const AValue: TDateTime): TDateTime;
begin
  Result := EncodeDateTime(
    YearOf(AValue),
    MonthOfTheYear(AValue),
    DayOfTheMonth(AValue),
    HourOf(AValue),
    MinuteOf(AValue),
    0,
    0);
end;

end.
