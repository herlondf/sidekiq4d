unit Hefesto.Retry.Tests;

{
  Testes unitários para THefestoSimpleRetryPolicy e THefestoExponentialRetryPolicy.
  Cobre: decisões de retry, dead-letter, delay e propagação da mensagem de exceção.
}

interface

uses
  DUnitX.TestFramework,
  Hefesto.Job,
  Hefesto.Retry;

type
  [TestFixture('RetryPolicy')]
  TRetryPolicyTests = class
  public
    [Test]
    [Category('Unit')]
    procedure Decide_BelowMaxAttempts_ReturnsRetry;

    [Test]
    [Category('Unit')]
    procedure Decide_AtMaxAttempts_ReturnsDeadLetter;

    [Test]
    [Category('Unit')]
    procedure Decide_AboveMaxAttempts_ReturnsDeadLetter;

    [Test]
    [Category('Unit')]
    procedure Decide_RetryDecision_HasConfiguredDelaySeconds;

    [Test]
    [Category('Unit')]
    procedure Decide_DeadLetterReason_ContainsExceptionMessage;

    [Test]
    [Category('Unit')]
    procedure Decide_MaxAttemptsZero_ImmediatelyDeadLetters;

    [Test]
    [Category('Unit')]
    procedure Decide_OneBeforeMax_StillRetries;
  end;

  [TestFixture('ExponentialRetryPolicy')]
  TExponentialRetryPolicyTests = class
  public
    [Test]
    [Category('Unit')]
    procedure Decide_FirstAttempt_ReturnsRetryWithBaseDelay;

    [Test]
    [Category('Unit')]
    procedure Decide_SecondAttempt_DelayGrowsExponentially;

    [Test]
    [Category('Unit')]
    procedure Decide_DelayExceedsMax_ClampedToMax;

    [Test]
    [Category('Unit')]
    procedure Decide_AtMaxAttempts_ReturnsDeadLetter;

    [Test]
    [Category('Unit')]
    procedure Decide_ZeroAttempt_DelayIsAtLeastOne;

    [Test]
    [Category('Unit')]
    procedure Decide_EachAttemptIncreasesDelay;
  end;

implementation

uses
  System.SysUtils;

function MakeJob(const AAttempts: Integer): IHefestoJobEnvelope;
begin
  Result := THefestoJobEnvelope.New('j-1', 'default', 'rh', '{}', 'act', AAttempts);
end;

{ TRetryPolicyTests }

procedure TRetryPolicyTests.Decide_BelowMaxAttempts_ReturnsRetry;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(3, 30);
  LErr := Exception.Create('falha simulada');
  try
    LDecision := LPolicy.Decide(MakeJob(0), LErr);
  finally
    LErr.Free;
  end;
  Assert.AreEqual(Ord(rdRetry), Ord(LDecision.Kind),
    'Attempts < Max deve retornar rdRetry');
end;

procedure TRetryPolicyTests.Decide_AtMaxAttempts_ReturnsDeadLetter;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(3, 30);
  LErr := Exception.Create('falha simulada');
  try
    LDecision := LPolicy.Decide(MakeJob(3), LErr);
  finally
    LErr.Free;
  end;
  Assert.AreEqual(Ord(rdDeadLetter), Ord(LDecision.Kind),
    'Attempts = Max deve retornar rdDeadLetter');
end;

procedure TRetryPolicyTests.Decide_AboveMaxAttempts_ReturnsDeadLetter;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(2, 30);
  LErr := Exception.Create('falha simulada');
  try
    LDecision := LPolicy.Decide(MakeJob(10), LErr);
  finally
    LErr.Free;
  end;
  Assert.AreEqual(Ord(rdDeadLetter), Ord(LDecision.Kind),
    'Attempts > Max deve retornar rdDeadLetter');
end;

procedure TRetryPolicyTests.Decide_RetryDecision_HasConfiguredDelaySeconds;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(5, 45);
  LErr := Exception.Create('falha');
  try
    LDecision := LPolicy.Decide(MakeJob(0), LErr);
  finally
    LErr.Free;
  end;
  Assert.AreEqual(45, LDecision.DelaySeconds,
    'DelaySeconds deve refletir o valor configurado no construtor');
end;

procedure TRetryPolicyTests.Decide_DeadLetterReason_ContainsExceptionMessage;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(1, 10);
  LErr := Exception.Create('connection timeout');
  try
    LDecision := LPolicy.Decide(MakeJob(1), LErr);
  finally
    LErr.Free;
  end;
  Assert.IsTrue(LDecision.Reason.Contains('connection timeout'),
    'Reason do DeadLetter deve conter a mensagem da exceção');
end;

procedure TRetryPolicyTests.Decide_MaxAttemptsZero_ImmediatelyDeadLetters;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(0, 30);
  LErr := Exception.Create('falha');
  try
    LDecision := LPolicy.Decide(MakeJob(0), LErr);
  finally
    LErr.Free;
  end;
  Assert.AreEqual(Ord(rdDeadLetter), Ord(LDecision.Kind),
    'MaxAttempts=0 deve enviar para DLQ imediatamente');
end;

procedure TRetryPolicyTests.Decide_OneBeforeMax_StillRetries;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoSimpleRetryPolicy.New(5, 30);
  LErr := Exception.Create('falha');
  try
    LDecision := LPolicy.Decide(MakeJob(4), LErr);
  finally
    LErr.Free;
  end;
  Assert.AreEqual(Ord(rdRetry), Ord(LDecision.Kind),
    'Attempts = Max-1 ainda deve fazer retry');
end;

{ TExponentialRetryPolicyTests }

procedure TExponentialRetryPolicyTests.Decide_FirstAttempt_ReturnsRetryWithBaseDelay;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoExponentialRetryPolicy.New(5, 10, 3600);
  LErr := Exception.Create('err');
  try
    LDecision := LPolicy.Decide(MakeJob(1), LErr);
  finally LErr.Free; end;
  Assert.AreEqual(Ord(rdRetry), Ord(LDecision.Kind));
  Assert.AreEqual(10, LDecision.DelaySeconds,
    'Tentativa 1: delay = base * 1^2 = 10');
end;

procedure TExponentialRetryPolicyTests.Decide_SecondAttempt_DelayGrowsExponentially;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoExponentialRetryPolicy.New(5, 10, 3600);
  LErr := Exception.Create('err');
  try
    LDecision := LPolicy.Decide(MakeJob(2), LErr);
  finally LErr.Free; end;
  Assert.AreEqual(40, LDecision.DelaySeconds,
    'Tentativa 2: delay = base * 2^2 = 40');
end;

procedure TExponentialRetryPolicyTests.Decide_DelayExceedsMax_ClampedToMax;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoExponentialRetryPolicy.New(10, 100, 50);
  LErr := Exception.Create('err');
  try
    LDecision := LPolicy.Decide(MakeJob(5), LErr);
  finally LErr.Free; end;
  Assert.AreEqual(50, LDecision.DelaySeconds,
    'Delay deve ser limitado ao máximo configurado');
end;

procedure TExponentialRetryPolicyTests.Decide_AtMaxAttempts_ReturnsDeadLetter;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoExponentialRetryPolicy.New(3, 10, 3600);
  LErr := Exception.Create('err');
  try
    LDecision := LPolicy.Decide(MakeJob(3), LErr);
  finally LErr.Free; end;
  Assert.AreEqual(Ord(rdDeadLetter), Ord(LDecision.Kind),
    'Attempts = MaxAttempts deve retornar DeadLetter');
end;

procedure TExponentialRetryPolicyTests.Decide_ZeroAttempt_DelayIsAtLeastOne;
var
  LPolicy: IHefestoRetryPolicy;
  LDecision: THefestoRetryDecision;
  LErr: Exception;
begin
  LPolicy := THefestoExponentialRetryPolicy.New(5, 10, 3600);
  LErr := Exception.Create('err');
  try
    LDecision := LPolicy.Decide(MakeJob(0), LErr);
  finally LErr.Free; end;
  Assert.IsTrue(LDecision.DelaySeconds >= 1,
    'Delay nunca deve ser zero ou negativo');
end;

procedure TExponentialRetryPolicyTests.Decide_EachAttemptIncreasesDelay;
var
  LPolicy: IHefestoRetryPolicy;
  LErr: Exception;
  LPrev, LCurr: THefestoRetryDecision;
begin
  LPolicy := THefestoExponentialRetryPolicy.New(10, 5, 99999);
  LErr := Exception.Create('err');
  try
    LPrev := LPolicy.Decide(MakeJob(1), LErr);
    LCurr := LPolicy.Decide(MakeJob(2), LErr);
    Assert.IsTrue(LCurr.DelaySeconds > LPrev.DelaySeconds,
      'Delay deve crescer a cada tentativa');
    LPrev := LCurr;
    LCurr := LPolicy.Decide(MakeJob(3), LErr);
    Assert.IsTrue(LCurr.DelaySeconds > LPrev.DelaySeconds);
  finally LErr.Free; end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRetryPolicyTests);
  TDUnitX.RegisterTestFixture(TExponentialRetryPolicyTests);

end.
