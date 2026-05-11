program SqsConsole;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.JSON,
  System.Threading,
  Sidekiq4D.Job in '..\..\src\Sidekiq4D.Job.pas',
  Sidekiq4D.Metadata in '..\..\src\Sidekiq4D.Metadata.pas',
  Sidekiq4D.Context in '..\..\src\Sidekiq4D.Context.pas',
  Sidekiq4D.Handler in '..\..\src\Sidekiq4D.Handler.pas',
  Sidekiq4D.Options in '..\..\src\Sidekiq4D.Options.pas',
  Sidekiq4D.Queue.Interfaces in '..\..\src\Sidekiq4D.Queue.Interfaces.pas',
  Sidekiq4D.Queue.SQS in '..\..\src\adapters\Sidekiq4D.Queue.SQS.pas',
  Sidekiq4D.Dispatcher in '..\..\src\Sidekiq4D.Dispatcher.pas',
  Sidekiq4D.Retry in '..\..\src\Sidekiq4D.Retry.pas',
  Sidekiq4D.Telemetry in '..\..\src\Sidekiq4D.Telemetry.pas',
  Sidekiq4D.Server in '..\..\src\Sidekiq4D.Server.pas';

type
  TConsoleJobHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TFailingJobHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TSlowJobHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    class function ResolveDelayMs(const ABody: string): Integer; static;
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

function RequireEnv(const AName: string): string;
begin
  Result := GetEnvironmentVariable(AName);
  if Result.Trim.IsEmpty then
    raise Exception.CreateFmt('Environment variable %s not configured.', [AName]);
end;

function ReadEnvBool(const AName: string; const ADefault: Boolean): Boolean;
var
  LValue: string;
begin
  LValue := GetEnvironmentVariable(AName).Trim.ToLower;
  if LValue.IsEmpty then
    Exit(ADefault);
  Result := SameText(LValue, '1')
    or SameText(LValue, 'true')
    or SameText(LValue, 'yes')
    or SameText(LValue, 'y');
end;

function ReadEnvInt(const AName: string; const ADefault: Integer): Integer;
begin
  Result := StrToIntDef(GetEnvironmentVariable(AName).Trim, ADefault);
end;

function OptionalEnv(const AName, ADefault: string): string;
begin
  Result := GetEnvironmentVariable(AName).Trim;
  if Result.IsEmpty then
    Result := ADefault;
end;

procedure PrintBanner(
  const AQueueName: string;
  const ADemoSeed: Boolean;
  const AScenarioSet: string;
  const AStopWhenIdle: Boolean;
  const ARollingRestartAfterMs: Integer);
begin
  Writeln('=== Sidekiq4D SQS Demo ===');
  Writeln('Queue...............: ' + AQueueName);
  Writeln('Demo seed...........: ' + BoolToStr(ADemoSeed, True));
  Writeln('Scenario set........: ' + AScenarioSet);
  Writeln('Stop when idle......: ' + BoolToStr(AStopWhenIdle, True));
  Writeln('Rolling restart ms..: ' + IntToStr(ARollingRestartAfterMs));
  Writeln('Handlers............: demo.success, demo.fail, demo.slow, demo.batch.complete, demo.batch.success, *');
  Writeln;
end;

procedure SeedFullScenarioSet(
  const AServer: ISidekiqServer;
  const AQueueName: string);
var
  LSlowAttributes: TStringList;
  LExpiredAttributes: TStringList;
begin
  Writeln('Seeding scenario set: full');
  AServer.Enqueue(AQueueName, 'demo.success', '{"scenario":"success","step":"direct-enqueue"}');
  AServer.Enqueue(AQueueName, 'demo.fail', '{"scenario":"retry-and-dlq","step":"direct-enqueue"}');

  LSlowAttributes := TStringList.Create;
  try
    LSlowAttributes.Values[TSidekiqJobAttribute.IdempotencyKey] := 'demo:slow:001';
    LSlowAttributes.Values[TSidekiqJobAttribute.LockKey] := 'demo:slow:001';
    LSlowAttributes.Values[TSidekiqJobAttribute.ServerLeaseTtlSeconds] := '6';
    LSlowAttributes.Values[TSidekiqJobAttribute.HeartbeatIntervalSeconds] := '2';
    AServer.Enqueue(
      AQueueName,
      'demo.slow',
      '{"scenario":"slow","delay_ms":8000}',
      LSlowAttributes);
  finally
    LSlowAttributes.Free;
  end;

  AServer.Batch('demo-batch')
    .OnComplete(AQueueName, 'demo.batch.complete', '{"scenario":"batch-complete"}')
    .OnSuccess(AQueueName, 'demo.batch.success', '{"scenario":"batch-success"}')
    .Enqueue(AQueueName, 'demo.success', '{"scenario":"batch-member","member":1}');

  AServer.EnqueueIn(
    AQueueName,
    'demo.success',
    '{"scenario":"scheduled-enqueue-in","delay_seconds":5}',
    5);

  LExpiredAttributes := TStringList.Create;
  try
    LExpiredAttributes.Values[TSidekiqJobAttribute.ExpiresInSeconds] := '1';
    AServer.EnqueueAt(
      AQueueName,
      'demo.success',
      '{"scenario":"expired-before-execution"}',
      IncSecond(Now, 5),
      LExpiredAttributes);
  finally
    LExpiredAttributes.Free;
  end;
end;

procedure SeedRollingRestartScenarioSet(
  const AServer: ISidekiqServer;
  const AQueueName: string);
var
  LSlowAttributes: TStringList;
begin
  Writeln('Seeding scenario set: rolling-restart');
  LSlowAttributes := TStringList.Create;
  try
    LSlowAttributes.Values[TSidekiqJobAttribute.IdempotencyKey] := 'demo:drain:001';
    LSlowAttributes.Values[TSidekiqJobAttribute.LockKey] := 'demo:drain:001';
    LSlowAttributes.Values[TSidekiqJobAttribute.ServerLeaseTtlSeconds] := '6';
    LSlowAttributes.Values[TSidekiqJobAttribute.HeartbeatIntervalSeconds] := '2';
    AServer.Enqueue(
      AQueueName,
      'demo.slow',
      '{"scenario":"rolling-restart-slow","delay_ms":12000}',
      LSlowAttributes);
  finally
    LSlowAttributes.Free;
  end;

  AServer.Enqueue(
    AQueueName,
    'demo.success',
    '{"scenario":"rolling-restart-pending-after-drain"}');
end;

procedure SeedDemoScenarios(
  const AServer: ISidekiqServer;
  const AQueueName: string;
  const AScenarioSet: string);
begin
  if SameText(AScenarioSet, 'rolling-restart') then
    SeedRollingRestartScenarioSet(AServer, AQueueName)
  else
    SeedFullScenarioSet(AServer, AQueueName);
end;

procedure ScheduleRollingRestart(
  const AServer: ISidekiqServer;
  const ADelayMs: Integer);
begin
  if ADelayMs <= 0 then
    Exit;

  TTask.Run(
    procedure
    begin
      TThread.Sleep(ADelayMs);
      Writeln(Format(
        '>>> triggering rolling restart after %d ms (active_workers=%d)',
        [ADelayMs, AServer.ActiveWorkers]));
      AServer.BeginRollingRestart;
    end);
end;

{ TConsoleJobHandler }

function TConsoleJobHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TConsoleJobHandler.Perform(const AContext: ISidekiqJobContext);
begin
  Writeln(Format(
    '[handler:ok] queue=%s action=%s attempts=%d body=%s',
    [AContext.Job.QueueName, AContext.Job.Action, AContext.Job.Attempts, AContext.Job.Body]));
end;

{ TFailingJobHandler }

function TFailingJobHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, 'demo.fail');
end;

procedure TFailingJobHandler.Perform(const AContext: ISidekiqJobContext);
begin
  Writeln(Format(
    '[handler:fail] queue=%s action=%s attempts=%d body=%s',
    [AContext.Job.QueueName, AContext.Job.Action, AContext.Job.Attempts, AContext.Job.Body]));
  raise Exception.Create('demo failure requested');
end;

{ TSlowJobHandler }

function TSlowJobHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, 'demo.slow');
end;

procedure TSlowJobHandler.Perform(const AContext: ISidekiqJobContext);
var
  LDelayMs: Integer;
begin
  LDelayMs := ResolveDelayMs(AContext.Job.Body);
  Writeln(Format(
    '[handler:slow:start] queue=%s action=%s delay_ms=%d body=%s',
    [AContext.Job.QueueName, AContext.Job.Action, LDelayMs, AContext.Job.Body]));
  TThread.Sleep(LDelayMs);
  Writeln(Format(
    '[handler:slow:finish] queue=%s action=%s delay_ms=%d',
    [AContext.Job.QueueName, AContext.Job.Action, LDelayMs]));
end;

class function TSlowJobHandler.ResolveDelayMs(const ABody: string): Integer;
var
  LJsonValue: TJSONValue;
  LJsonObject: TJSONObject;
begin
  Result := 8000;
  if ABody.Trim.IsEmpty then
    Exit;

  LJsonValue := TJSONObject.ParseJSONValue(ABody);
  try
    if not (LJsonValue is TJSONObject) then
      Exit;
    LJsonObject := TJSONObject(LJsonValue);
    if Assigned(LJsonObject.GetValue('delay_ms')) then
      Result := LJsonObject.GetValue<Integer>('delay_ms');
  finally
    LJsonValue.Free;
  end;
end;

var
  LQueueAdapter: TSidekiqSqsQueueAdapter;
  LServer: ISidekiqServer;
  LStopWhenIdle: Boolean;
  LDemoSeed: Boolean;
  LScenarioSet: string;
  LRollingRestartAfterMs: Integer;
  LSessionToken: string;
begin
  try
    LDemoSeed := ReadEnvBool('SIDEKIQ_DEMO_SEED', False);
    LScenarioSet := OptionalEnv('SIDEKIQ_DEMO_SCENARIO_SET', 'full');
    LRollingRestartAfterMs := ReadEnvInt('SIDEKIQ_DEMO_ROLLING_RESTART_AFTER_MS', 0);
    LStopWhenIdle := ReadEnvBool('SIDEKIQ_STOP_WHEN_IDLE', not LDemoSeed);

    LQueueAdapter := TSidekiqSqsQueueAdapter.New
      .QueueUrl(RequireEnv('SIDEKIQ_SQS_QUEUE_URL'))
      .AccessKey(RequireEnv('AWS_ACCESS_KEY_ID'))
      .SecretKey(RequireEnv('AWS_SECRET_ACCESS_KEY'))
      .Region(OptionalEnv('AWS_REGION', 'us-east-1'));

    LSessionToken := GetEnvironmentVariable('AWS_SESSION_TOKEN').Trim;
    if not LSessionToken.IsEmpty then
      LQueueAdapter.SessionToken(LSessionToken);

    if not GetEnvironmentVariable('SIDEKIQ_SQS_DLQ_URL').Trim.IsEmpty then
      LQueueAdapter.DeadLetterQueueUrl(GetEnvironmentVariable('SIDEKIQ_SQS_DLQ_URL').Trim);

    PrintBanner(
      LQueueAdapter.Name,
      LDemoSeed,
      LScenarioSet,
      LStopWhenIdle,
      LRollingRestartAfterMs);

    LServer := TSidekiqServer.New
      .UseQueue(LQueueAdapter)
      .BatchSize(ReadEnvInt('SIDEKIQ_BATCH_SIZE', 10))
      .WaitTimeSeconds(ReadEnvInt('SIDEKIQ_WAIT_TIME_SECONDS', 20))
      .VisibilityTimeout(ReadEnvInt('SIDEKIQ_VISIBILITY_TIMEOUT', 300))
      .IdleDelayMs(ReadEnvInt('SIDEKIQ_IDLE_DELAY_MS', 1000))
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(
        ReadEnvInt('SIDEKIQ_RETRY_ATTEMPTS', 2),
        ReadEnvInt('SIDEKIQ_RETRY_DELAY_SECONDS', 5)))
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('demo.fail', TFailingJobHandler.Create)
      .RegisterHandler('demo.slow', TSlowJobHandler.Create)
      .RegisterHandler('*', TConsoleJobHandler.Create);

    if LStopWhenIdle then
      LServer.StopWhenIdle;

    if LDemoSeed then
      SeedDemoScenarios(LServer, LQueueAdapter.Name, LScenarioSet);

    ScheduleRollingRestart(LServer, LRollingRestartAfterMs);
    LServer.Run;
  except
    on E: Exception do
    begin
      Writeln(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
