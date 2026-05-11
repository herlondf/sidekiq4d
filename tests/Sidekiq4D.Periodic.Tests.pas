unit Sidekiq4D.Periodic.Tests;

{
  Testes unitários para TSidekiqCronSchedule e MakePeriodicDefinition.
  Cobre: parsing CRON, matching de datas, truncação e validações de definição.
}

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  Sidekiq4D.Periodic;

type
  [TestFixture('CronSchedule')]
  TCronScheduleTests = class
  private
    function MakeDateTime(
      AYear, AMonth, ADay, AHour, AMinute: Word): TDateTime;
  public
    { Matches — expressões simples }

    [Test]
    [Category('Unit')]
    procedure Matches_Wildcard_MatchesAnyTime;

    [Test]
    [Category('Unit')]
    procedure Matches_ExactMinute_MatchesOnlyThatMinute;

    [Test]
    [Category('Unit')]
    procedure Matches_ExactMinute_DoesNotMatchOtherMinute;

    [Test]
    [Category('Unit')]
    procedure Matches_ExactHour_MatchesCorrectly;

    [Test]
    [Category('Unit')]
    procedure Matches_StepMinute_MatchesEvery15Minutes;

    [Test]
    [Category('Unit')]
    procedure Matches_StepMinute_DoesNotMatchNonMultiple;

    [Test]
    [Category('Unit')]
    procedure Matches_CommaSeparated_MatchesAllListedValues;

    [Test]
    [Category('Unit')]
    procedure Matches_Range_MatchesWithinRange;

    [Test]
    [Category('Unit')]
    procedure Matches_Range_DoesNotMatchOutsideRange;

    [Test]
    [Category('Unit')]
    procedure Matches_DayOfWeek_MatchesMonday;

    [Test]
    [Category('Unit')]
    procedure Matches_InvalidFieldCount_RaisesESidekiqPeriodic;

    [Test]
    [Category('Unit')]
    procedure Matches_OutOfRangeMinute_RaisesESidekiqPeriodic;

    { TruncateToMinute }

    [Test]
    [Category('Unit')]
    procedure TruncateToMinute_StripSecondsAndMillis;

    [Test]
    [Category('Unit')]
    procedure TruncateToMinute_PreservesDateAndHourAndMinute;
  end;

  [TestFixture('PeriodicDefinition')]
  TPeriodicDefinitionTests = class
  public
    [Test]
    [Category('Unit')]
    procedure MakePeriodicDefinition_ValidArgs_ReturnsDefinition;

    [Test]
    [Category('Unit')]
    procedure MakePeriodicDefinition_EmptyName_RaisesESidekiqPeriodic;

    [Test]
    [Category('Unit')]
    procedure MakePeriodicDefinition_EmptyExpression_RaisesESidekiqPeriodic;

    [Test]
    [Category('Unit')]
    procedure MakePeriodicDefinition_EmptyAction_RaisesESidekiqPeriodic;

    [Test]
    [Category('Unit')]
    procedure MakePeriodicDefinition_WithAttributes_StoresKeyValuePairs;

    [Test]
    [Category('Unit')]
    procedure ApplyPeriodicAttributesToStrings_FillsTarget;

    [Test]
    [Category('Unit')]
    procedure ApplyPeriodicAttributesToStrings_NilTarget_DoesNotRaise;
  end;

implementation

{ TCronScheduleTests — helpers }

function TCronScheduleTests.MakeDateTime(
  AYear, AMonth, ADay, AHour, AMinute: Word): TDateTime;
begin
  Result := EncodeDateTime(AYear, AMonth, ADay, AHour, AMinute, 0, 0);
end;

{ TCronScheduleTests — Matches }

procedure TCronScheduleTests.Matches_Wildcard_MatchesAnyTime;
begin
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('* * * * *', MakeDateTime(2024, 3, 15, 10, 30)),
    '"* * * * *" deve casar com qualquer data/hora');
end;

procedure TCronScheduleTests.Matches_ExactMinute_MatchesOnlyThatMinute;
begin
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('30 * * * *', MakeDateTime(2024, 1, 1, 8, 30)),
    'Minuto 30 deve casar');
end;

procedure TCronScheduleTests.Matches_ExactMinute_DoesNotMatchOtherMinute;
begin
  Assert.IsFalse(
    TSidekiqCronSchedule.Matches('30 * * * *', MakeDateTime(2024, 1, 1, 8, 31)),
    'Minuto 31 não deve casar com expressão de minuto 30');
end;

procedure TCronScheduleTests.Matches_ExactHour_MatchesCorrectly;
begin
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('0 9 * * *', MakeDateTime(2024, 1, 1, 9, 0)),
    '"0 9 * * *" deve casar às 09:00');
  Assert.IsFalse(
    TSidekiqCronSchedule.Matches('0 9 * * *', MakeDateTime(2024, 1, 1, 10, 0)),
    '"0 9 * * *" não deve casar às 10:00');
end;

procedure TCronScheduleTests.Matches_StepMinute_MatchesEvery15Minutes;
begin
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('*/15 * * * *', MakeDateTime(2024, 1, 1, 0, 0)));
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('*/15 * * * *', MakeDateTime(2024, 1, 1, 0, 15)));
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('*/15 * * * *', MakeDateTime(2024, 1, 1, 0, 30)));
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('*/15 * * * *', MakeDateTime(2024, 1, 1, 0, 45)));
end;

procedure TCronScheduleTests.Matches_StepMinute_DoesNotMatchNonMultiple;
begin
  Assert.IsFalse(
    TSidekiqCronSchedule.Matches('*/15 * * * *', MakeDateTime(2024, 1, 1, 0, 7)),
    'Minuto 7 não é múltiplo de 15');
end;

procedure TCronScheduleTests.Matches_CommaSeparated_MatchesAllListedValues;
begin
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('0,30 * * * *', MakeDateTime(2024, 1, 1, 5, 0)));
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('0,30 * * * *', MakeDateTime(2024, 1, 1, 5, 30)));
  Assert.IsFalse(
    TSidekiqCronSchedule.Matches('0,30 * * * *', MakeDateTime(2024, 1, 1, 5, 15)));
end;

procedure TCronScheduleTests.Matches_Range_MatchesWithinRange;
begin
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('* 9-17 * * *', MakeDateTime(2024, 1, 1, 12, 0)),
    'Hora 12 está dentro do range 9-17');
end;

procedure TCronScheduleTests.Matches_Range_DoesNotMatchOutsideRange;
begin
  Assert.IsFalse(
    TSidekiqCronSchedule.Matches('* 9-17 * * *', MakeDateTime(2024, 1, 1, 8, 0)),
    'Hora 8 está fora do range 9-17');
end;

procedure TCronScheduleTests.Matches_DayOfWeek_MatchesMonday;
var
  // 2024-01-01 é segunda-feira (DayOfWeek retorna 2, -1 = 1 no cron 0-based)
  LMonday: TDateTime;
begin
  LMonday := EncodeDate(2024, 1, 1);
  Assert.IsTrue(
    TSidekiqCronSchedule.Matches('0 9 * * 1', EncodeDateTime(2024, 1, 1, 9, 0, 0, 0)),
    '"0 9 * * 1" deve casar na segunda-feira às 09:00');
end;

procedure TCronScheduleTests.Matches_InvalidFieldCount_RaisesESidekiqPeriodic;
begin
  Assert.WillRaise(
    procedure
    begin
      TSidekiqCronSchedule.Matches('* * * *', Now); // apenas 4 campos
    end,
    ESidekiqPeriodic,
    'Expressão com != 5 campos deve levantar ESidekiqPeriodic');
end;

procedure TCronScheduleTests.Matches_OutOfRangeMinute_RaisesESidekiqPeriodic;
begin
  Assert.WillRaise(
    procedure
    begin
      TSidekiqCronSchedule.Matches('60 * * * *', Now); // minuto 60 inválido
    end,
    ESidekiqPeriodic,
    'Minuto 60 deve levantar ESidekiqPeriodic');
end;

{ TCronScheduleTests — TruncateToMinute }

procedure TCronScheduleTests.TruncateToMinute_StripSecondsAndMillis;
var
  LInput, LResult: TDateTime;
begin
  LInput  := EncodeDateTime(2024, 6, 15, 14, 22, 45, 999);
  LResult := TSidekiqCronSchedule.TruncateToMinute(LInput);
  Assert.AreEqual(
    EncodeDateTime(2024, 6, 15, 14, 22, 0, 0),
    LResult,
    'TruncateToMinute deve zerar segundos e milissegundos');
end;

procedure TCronScheduleTests.TruncateToMinute_PreservesDateAndHourAndMinute;
var
  LResult: TDateTime;
begin
  LResult := TSidekiqCronSchedule.TruncateToMinute(
    EncodeDateTime(2025, 12, 31, 23, 59, 59, 500));
  Assert.AreEqual(2025, YearOf(LResult));
  Assert.AreEqual(12,   MonthOfTheYear(LResult));
  Assert.AreEqual(31,   DayOfTheMonth(LResult));
  Assert.AreEqual(23,   HourOf(LResult));
  Assert.AreEqual(59,   MinuteOf(LResult));
end;

{ TPeriodicDefinitionTests }

procedure TPeriodicDefinitionTests.MakePeriodicDefinition_ValidArgs_ReturnsDefinition;
var
  LDef: TSidekiqPeriodicDefinition;
begin
  LDef := MakePeriodicDefinition('cleanup', '0 2 * * *', 'default', 'Cleanup', '{}', nil);
  Assert.AreEqual('cleanup', LDef.Name);
  Assert.AreEqual('0 2 * * *', LDef.CronExpression);
  Assert.AreEqual('default', LDef.QueueName);
  Assert.AreEqual('Cleanup', LDef.Action);
  Assert.AreEqual('{}', LDef.Body);
end;

procedure TPeriodicDefinitionTests.MakePeriodicDefinition_EmptyName_RaisesESidekiqPeriodic;
begin
  Assert.WillRaise(
    procedure
    begin
      MakePeriodicDefinition('', '* * * * *', 'q', 'Act', '{}', nil);
    end,
    ESidekiqPeriodic,
    'Nome vazio deve levantar ESidekiqPeriodic');
end;

procedure TPeriodicDefinitionTests.MakePeriodicDefinition_EmptyExpression_RaisesESidekiqPeriodic;
begin
  Assert.WillRaise(
    procedure
    begin
      MakePeriodicDefinition('job', '', 'q', 'Act', '{}', nil);
    end,
    ESidekiqPeriodic,
    'Expressão CRON vazia deve levantar ESidekiqPeriodic');
end;

procedure TPeriodicDefinitionTests.MakePeriodicDefinition_EmptyAction_RaisesESidekiqPeriodic;
begin
  Assert.WillRaise(
    procedure
    begin
      MakePeriodicDefinition('job', '* * * * *', 'q', '', '{}', nil);
    end,
    ESidekiqPeriodic,
    'Action vazia deve levantar ESidekiqPeriodic');
end;

procedure TPeriodicDefinitionTests.MakePeriodicDefinition_WithAttributes_StoresKeyValuePairs;
var
  LAttrs: TStringList;
  LDef: TSidekiqPeriodicDefinition;
begin
  LAttrs := TStringList.Create;
  try
    LAttrs.Values['region'] := 'us-east-1';
    LAttrs.Values['priority'] := 'high';
    LDef := MakePeriodicDefinition('job', '* * * * *', 'q', 'Act', '{}', LAttrs);
    Assert.AreEqual(2, Length(LDef.Attributes));
  finally
    LAttrs.Free;
  end;
end;

procedure TPeriodicDefinitionTests.ApplyPeriodicAttributesToStrings_FillsTarget;
var
  LAttrs: TStringList;
  LDef: TSidekiqPeriodicDefinition;
  LTarget: TStringList;
begin
  LAttrs := TStringList.Create;
  LTarget := TStringList.Create;
  try
    LAttrs.Values['env'] := 'production';
    LDef := MakePeriodicDefinition('job', '* * * * *', 'q', 'Act', '{}', LAttrs);
    ApplyPeriodicAttributesToStrings(LDef, LTarget);
    Assert.AreEqual('production', LTarget.Values['env'],
      'ApplyPeriodicAttributesToStrings deve copiar os atributos para o target');
  finally
    LAttrs.Free;
    LTarget.Free;
  end;
end;

procedure TPeriodicDefinitionTests.ApplyPeriodicAttributesToStrings_NilTarget_DoesNotRaise;
var
  LDef: TSidekiqPeriodicDefinition;
begin
  LDef := MakePeriodicDefinition('job', '* * * * *', 'q', 'Act', '{}', nil);
  Assert.WillNotRaise(
    procedure
    begin
      ApplyPeriodicAttributesToStrings(LDef, nil);
    end,
    nil,
    'nil target não deve levantar exceção');
end;

initialization
  TDUnitX.RegisterTestFixture(TCronScheduleTests);
  TDUnitX.RegisterTestFixture(TPeriodicDefinitionTests);

end.
