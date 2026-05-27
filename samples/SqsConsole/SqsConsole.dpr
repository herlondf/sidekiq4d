program SqsConsole;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.JSON,
  System.Threading,
  Hefesto.Job in '..\..\src\Hefesto.Job.pas',
  Hefesto.Metadata in '..\..\src\Hefesto.Metadata.pas',
  Hefesto.Context in '..\..\src\Hefesto.Context.pas',
  Hefesto.Handler in '..\..\src\Hefesto.Handler.pas',
  Hefesto.Options in '..\..\src\Hefesto.Options.pas',
  Hefesto.Queue.Interfaces in '..\..\src\Hefesto.Queue.Interfaces.pas',
  Hefesto.Queue.SQS in '..\..\src\adapters\Hefesto.Queue.SQS.pas',
  Hefesto.Dispatcher in '..\..\src\Hefesto.Dispatcher.pas',
  Hefesto.Retry in '..\..\src\Hefesto.Retry.pas',
  Hefesto.Telemetry in '..\..\src\Hefesto.Telemetry.pas',
  Hefesto.Server in '..\..\src\Hefesto.Server.pas';

type
  TConsoleJobHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TFailingJobHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

  TSlowJobHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    class function ResolveDelayMs(const ABody: string): Integer; static;
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
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
  Writeln('=== Hefesto SQS Demo ===');
  Writeln('Queue...............: ' + AQueueName);
  Writeln('Demo seed...........: ' + BoolToStr(ADemoSeed, True));
  Writeln('Scenario set........: ' + AScenarioSet);
  Writeln('Stop when idle......: ' + BoolToStr(AStopWhenIdle, True));
  Writeln('Rolling restart ms..: ' + IntToStr(ARollingRestartAfterMs));
  Writeln('Handlers............: demo.success, demo.fail, demo.slow, demo.batch.complete, demo.batch.success, *');
  Writeln;
end;

procedure SeedFullScenarioSet(
  const AServer: IHefestoServer;
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
    LSlowAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'demo:slow:001';
    LSlowAttributes.Values[THefestoJobAttribute.LockKey] := 'demo:slow:001';
    LSlowAttributes.Values[THefestoJobAttribute.ServerLeaseTtlSeconds] := '6';
    LSlowAttributes.Values[THefestoJobAttribute.HeartbeatIntervalSeconds] := '2';
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
    LExpiredAttributes.Values[THefestoJobAttribute.ExpiresInSeconds] := '1';
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
  const AServer: IHefestoServer;
  const AQueueName: string);
var
  LSlowAttributes: TStringList;
begin
  Writeln('Seeding scenario set: rolling-restart');
  LSlowAttributes := TStringList.Create;
  try
    LSlowAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'demo:drain:001';
    LSlowAttributes.Values[THefestoJobAttribute.LockKey] := 'demo:drain:001';
    LSlowAttributes.Values[THefestoJobAttribute.ServerLeaseTtlSeconds] := '6';
    LSlowAttributes.Values[THefestoJobAttribute.HeartbeatIntervalSeconds] := '2';
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
  const AServer: IHefestoServer;
  const AQueueName: string;
  const AScenarioSet: string);
begin
  if SameText(AScenarioSet, 'rolling-restart') then
    SeedRollingRestartScenarioSet(AServer, AQueueName)
  else
    SeedFullScenarioSet(AServer, AQueueName);
end;

procedure ScheduleRollingRestart(
  const AServer: IHefestoServer;
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

function TConsoleJobHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TConsoleJobHandler.Perform(const AContext: IHefestoJobContext);
begin
  Writeln(Format(
    '[handler:ok] queue=%s action=%s attempts=%d body=%s',
    [AContext.Job.QueueName, AContext.Job.Action, AContext.Job.Attempts, AContext.Job.Body]));
end;

{ TFailingJobHandler }

function TFailingJobHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, 'demo.fail');
end;

procedure TFailingJobHandler.Perform(const AContext: IHefestoJobContext);
begin
  Writeln(Format(
    '[handler:fail] queue=%s action=%s attempts=%d body=%s',
    [AContext.Job.QueueName, AContext.Job.Action, AContext.Job.Attempts, AContext.Job.Body]));
  raise Exception.Create('demo failure requested');
end;

{ TSlowJobHandler }

function TSlowJobHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, 'demo.slow');
end;

procedure TSlowJobHandler.Perform(const AContext: IHefestoJobContext);
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
  LQueueAdapter: THefestoSqsQueueAdapter;
  LServer: IHefestoServer;
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

    LQueueAdapter := THefestoSqsQueueAdapter.New
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

    LServer := THefestoServer.New
      .UseQueue(LQueueAdapter)
      .BatchSize(ReadEnvInt('SIDEKIQ_BATCH_SIZE', 10))
      .WaitTimeSeconds(ReadEnvInt('SIDEKIQ_WAIT_TIME_SECONDS', 20))
      .VisibilityTimeout(ReadEnvInt('SIDEKIQ_VISIBILITY_TIMEOUT', 300))
      .IdleDelayMs(ReadEnvInt('SIDEKIQ_IDLE_DELAY_MS', 1000))
      .RetryPolicy(THefestoSimpleRetryPolicy.New(
        ReadEnvInt('SIDEKIQ_RETRY_ATTEMPTS', 2),
        ReadEnvInt('SIDEKIQ_RETRY_DELAY_SECONDS', 5)))
      .Telemetry(THefestoConsoleTelemetry.New)
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
