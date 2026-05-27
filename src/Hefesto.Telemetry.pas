unit Hefesto.Telemetry;

interface

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Hefesto.Job;

type
  THefestoHistoricalMetricSnapshot = record
    BucketStartedAt: TDateTime;
    FetchOperations: Int64;
    FetchedJobs: Int64;
    StartedJobs: Int64;
    SucceededJobs: Int64;
    FailedJobs: Int64;
    AckedJobs: Int64;
    RetriedJobs: Int64;
    DiscardedJobs: Int64;
    DeadLetteredJobs: Int64;
    TotalDurationMs: Int64;

    function AverageDurationMs: Double;
  end;

  IHefestoTelemetry = interface
    ['{6412AF8C-E486-45B5-8422-D4BDF7EB8884}']
    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: IHefestoJobEnvelope);
    procedure JobSucceeded(const AJob: IHefestoJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: IHefestoJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: IHefestoJobEnvelope);
    procedure JobRetried(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: IHefestoJobEnvelope; const AReason: string);
  end;

  IHefestoMetricTransport = interface
    ['{C97492BC-D891-4E18-8E88-B3707E5934B8}']
    procedure Send(const APayload: string);
  end;

  IHefestoMetricsSink = interface
    ['{E02F60EA-E1CE-4F55-9BE7-7CA6F02DBD9D}']
    procedure Increment(
      const AName: string;
      const AValue: Int64 = 1;
      const ATags: TStrings = nil);
    procedure Gauge(
      const AName: string;
      const AValue: Int64;
      const ATags: TStrings = nil);
    procedure Timing(
      const AName: string;
      const AValueMs: Int64;
      const ATags: TStrings = nil);
  end;

  IHefestoHistoricalMetrics = interface
    ['{E2EF98A7-78A4-4231-9D40-4D3401EFE225}']
    function Snapshot: TArray<THefestoHistoricalMetricSnapshot>;
  end;

  THefestoNoopTelemetry = class(TInterfacedObject, IHefestoTelemetry)
  public
    class function New: IHefestoTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: IHefestoJobEnvelope);
    procedure JobSucceeded(const AJob: IHefestoJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: IHefestoJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: IHefestoJobEnvelope);
    procedure JobRetried(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: IHefestoJobEnvelope; const AReason: string);
  end;

  THefestoConsoleTelemetry = class(TInterfacedObject, IHefestoTelemetry)
  public
    class function New: IHefestoTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: IHefestoJobEnvelope);
    procedure JobSucceeded(const AJob: IHefestoJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: IHefestoJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: IHefestoJobEnvelope);
    procedure JobRetried(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: IHefestoJobEnvelope; const AReason: string);
  end;

  THefestoCompositeTelemetry = class(TInterfacedObject, IHefestoTelemetry)
  private
    FItems: TArray<IHefestoTelemetry>;
    procedure ForwardServerStarted;
    procedure ForwardServerStopped;
    procedure ForwardPollingStarted;
    procedure ForwardPollingStopped;
  public
    constructor Create(const ATelemetries: array of IHefestoTelemetry);
    class function New(const ATelemetries: array of IHefestoTelemetry): IHefestoTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: IHefestoJobEnvelope);
    procedure JobSucceeded(const AJob: IHefestoJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: IHefestoJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: IHefestoJobEnvelope);
    procedure JobRetried(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: IHefestoJobEnvelope; const AReason: string);
  end;

  THefestoInMemoryMetricTransport = class(TInterfacedObject, IHefestoMetricTransport)
  private
    FLock: TCriticalSection;
    FPayloads: TList<string>;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoInMemoryMetricTransport;

    procedure Send(const APayload: string);
    function Payloads: TArray<string>;
  end;

  THefestoStatsDMetricsSink = class(TInterfacedObject, IHefestoMetricsSink)
  private
    FTransport: IHefestoMetricTransport;
    FPrefix: string;

    function ComposeMetricName(const AName: string): string;
    function FormatTags(const ATags: TStrings): string;
    procedure Emit(
      const AName, AValue, AKind: string;
      const ATags: TStrings);
  public
    constructor Create(
      const ATransport: IHefestoMetricTransport;
      const APrefix: string = 'sidekiq4delphi');
    class function New(
      const ATransport: IHefestoMetricTransport;
      const APrefix: string = 'sidekiq4delphi'): IHefestoMetricsSink;

    procedure Increment(
      const AName: string;
      const AValue: Int64 = 1;
      const ATags: TStrings = nil);
    procedure Gauge(
      const AName: string;
      const AValue: Int64;
      const ATags: TStrings = nil);
    procedure Timing(
      const AName: string;
      const AValueMs: Int64;
      const ATags: TStrings = nil);
  end;

  THefestoMetricsTelemetry = class(TInterfacedObject, IHefestoTelemetry)
  private
    FSink: IHefestoMetricsSink;

    class function QueueTags(const AQueueName: string): TStringList; static;
    class function JobTags(const AJob: IHefestoJobEnvelope): TStringList; static;
  public
    constructor Create(const ASink: IHefestoMetricsSink);
    class function New(const ASink: IHefestoMetricsSink): IHefestoTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: IHefestoJobEnvelope);
    procedure JobSucceeded(const AJob: IHefestoJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: IHefestoJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: IHefestoJobEnvelope);
    procedure JobRetried(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: IHefestoJobEnvelope; const AReason: string);
  end;

  THefestoHistoricalMetricsTelemetry = class(TInterfacedObject, IHefestoTelemetry, IHefestoHistoricalMetrics)
  private
    FLock: TCriticalSection;
    FBucketSeconds: Integer;
    FMaxBuckets: Integer;
    FBuckets: TList<THefestoHistoricalMetricSnapshot>;

    class function ResolveBucketStart(
      const AValue: TDateTime;
      const ABucketSeconds: Integer): TDateTime; static;
    function EnsureBucketIndexLocked(const AValue: TDateTime): Integer;
    procedure TrimBucketsLocked;
    procedure UpdateFetchLocked(const ACount: Integer);
    procedure UpdateStartLocked;
    procedure UpdateCompletionLocked(
      const ASucceeded: Boolean;
      const ADurationMs: Int64);
    procedure UpdateAckLocked;
    procedure UpdateRetryLocked;
    procedure UpdateDiscardLocked;
    procedure UpdateDeadLetterLocked;
  public
    constructor Create(
      const ABucketSeconds: Integer = 60;
      const AMaxBuckets: Integer = 60);
    destructor Destroy; override;

    class function New(
      const ABucketSeconds: Integer = 60;
      const AMaxBuckets: Integer = 60): IHefestoTelemetry;

    function Snapshot: TArray<THefestoHistoricalMetricSnapshot>;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: IHefestoJobEnvelope);
    procedure JobSucceeded(const AJob: IHefestoJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: IHefestoJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: IHefestoJobEnvelope);
    procedure JobRetried(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: IHefestoJobEnvelope; const AReason: string);
  end;

implementation

{ THefestoHistoricalMetricSnapshot }

function THefestoHistoricalMetricSnapshot.AverageDurationMs: Double;
var
  LCompleted: Integer;
begin
  LCompleted := SucceededJobs + FailedJobs;
  if LCompleted = 0 then
    Exit(0);
  Result := TotalDurationMs / LCompleted;
end;

{ THefestoNoopTelemetry }

procedure THefestoNoopTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
begin
end;

procedure THefestoNoopTelemetry.Idle(const ADelayMs: Integer);
begin
end;

procedure THefestoNoopTelemetry.JobAcked(const AJob: IHefestoJobEnvelope);
begin
end;

procedure THefestoNoopTelemetry.JobDeadLettered(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
end;

procedure THefestoNoopTelemetry.JobDiscarded(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
end;

procedure THefestoNoopTelemetry.JobFailed(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
begin
end;

procedure THefestoNoopTelemetry.JobRetried(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
end;

procedure THefestoNoopTelemetry.JobStarted(const AJob: IHefestoJobEnvelope);
begin
end;

procedure THefestoNoopTelemetry.JobSucceeded(
  const AJob: IHefestoJobEnvelope;
  const ADurationMs: Int64);
begin
end;

class function THefestoNoopTelemetry.New: IHefestoTelemetry;
begin
  Result := THefestoNoopTelemetry.Create;
end;

procedure THefestoNoopTelemetry.PollingStarted;
begin
end;

procedure THefestoNoopTelemetry.PollingStopped;
begin
end;

procedure THefestoNoopTelemetry.ServerStarted;
begin
end;

procedure THefestoNoopTelemetry.ServerStopped;
begin
end;

{ THefestoConsoleTelemetry }

// Removes newlines and caps length to prevent log injection via user-controlled fields.
function SanitizeLogField(const AValue: string; AMaxLen: Integer = 120): string;
begin
  Result := AValue.Replace(#13, ' ').Replace(#10, ' ').Replace(#9, ' ');
  if Length(Result) > AMaxLen then
    Result := Copy(Result, 1, AMaxLen) + '...';
end;

procedure THefestoConsoleTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
begin
  Writeln(Format('sidekiq4delphi fetch queue=%s count=%d', [AQueueName, ACount]));
end;

procedure THefestoConsoleTelemetry.Idle(const ADelayMs: Integer);
begin
  Writeln(Format('sidekiq4delphi idle delay_ms=%d', [ADelayMs]));
end;

procedure THefestoConsoleTelemetry.JobAcked(const AJob: IHefestoJobEnvelope);
begin
  Writeln(Format('sidekiq4delphi ack job=%s action=%s',
    [AJob.Id, SanitizeLogField(AJob.Action)]));
end;

procedure THefestoConsoleTelemetry.JobDeadLettered(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
  Writeln(Format(
    'sidekiq4delphi dead_letter job=%s action=%s reason=%s',
    [AJob.Id, SanitizeLogField(AJob.Action), SanitizeLogField(AReason)]));
end;

procedure THefestoConsoleTelemetry.JobDiscarded(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
var
  LId, LAction: string;
begin
  if Assigned(AJob) then begin LId := AJob.Id; LAction := AJob.Action; end
  else begin LId := '(none)'; LAction := '(none)'; end;
  Writeln(Format(
    'sidekiq4delphi discard job=%s action=%s reason=%s',
    [LId, LAction, AReason]));
end;

procedure THefestoConsoleTelemetry.JobFailed(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
begin
  Writeln(Format(
    'sidekiq4delphi fail job=%s action=%s duration_ms=%d error=%s',
    [AJob.Id, SanitizeLogField(AJob.Action), ADurationMs,
     SanitizeLogField(AError.Message)]));
end;

procedure THefestoConsoleTelemetry.JobRetried(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
  Writeln(Format(
    'sidekiq4delphi retry job=%s action=%s delay_seconds=%d',
    [AJob.Id, SanitizeLogField(AJob.Action), ADelaySeconds]));
end;

procedure THefestoConsoleTelemetry.JobStarted(
  const AJob: IHefestoJobEnvelope);
begin
  Writeln(Format('sidekiq4delphi start job=%s action=%s',
    [AJob.Id, SanitizeLogField(AJob.Action)]));
end;

procedure THefestoConsoleTelemetry.JobSucceeded(
  const AJob: IHefestoJobEnvelope;
  const ADurationMs: Int64);
begin
  Writeln(Format(
    'sidekiq4delphi success job=%s action=%s duration_ms=%d',
    [AJob.Id, AJob.Action, ADurationMs]));
end;

class function THefestoConsoleTelemetry.New: IHefestoTelemetry;
begin
  Result := THefestoConsoleTelemetry.Create;
end;

procedure THefestoConsoleTelemetry.PollingStarted;
begin
  Writeln('sidekiq4delphi polling started');
end;

procedure THefestoConsoleTelemetry.PollingStopped;
begin
  Writeln('sidekiq4delphi polling stopped');
end;

procedure THefestoConsoleTelemetry.ServerStarted;
begin
  Writeln('sidekiq4delphi server started');
end;

procedure THefestoConsoleTelemetry.ServerStopped;
begin
  Writeln('sidekiq4delphi server stopped');
end;

{ THefestoCompositeTelemetry }

constructor THefestoCompositeTelemetry.Create(
  const ATelemetries: array of IHefestoTelemetry);
var
  LIndex: Integer;
begin
  inherited Create;
  SetLength(FItems, Length(ATelemetries));
  for LIndex := 0 to Pred(Length(ATelemetries)) do
    FItems[LIndex] := ATelemetries[LIndex];
end;

procedure THefestoCompositeTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.FetchFinished(AQueueName, ACount);
end;

procedure THefestoCompositeTelemetry.ForwardPollingStarted;
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.PollingStarted;
end;

procedure THefestoCompositeTelemetry.ForwardPollingStopped;
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.PollingStopped;
end;

procedure THefestoCompositeTelemetry.ForwardServerStarted;
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.ServerStarted;
end;

procedure THefestoCompositeTelemetry.ForwardServerStopped;
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.ServerStopped;
end;

procedure THefestoCompositeTelemetry.Idle(const ADelayMs: Integer);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.Idle(ADelayMs);
end;

procedure THefestoCompositeTelemetry.JobAcked(const AJob: IHefestoJobEnvelope);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobAcked(AJob);
end;

procedure THefestoCompositeTelemetry.JobDeadLettered(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobDeadLettered(AJob, AReason);
end;

procedure THefestoCompositeTelemetry.JobDiscarded(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobDiscarded(AJob, AReason);
end;

procedure THefestoCompositeTelemetry.JobFailed(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobFailed(AJob, AError, ADurationMs);
end;

procedure THefestoCompositeTelemetry.JobRetried(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobRetried(AJob, ADelaySeconds);
end;

procedure THefestoCompositeTelemetry.JobStarted(
  const AJob: IHefestoJobEnvelope);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobStarted(AJob);
end;

procedure THefestoCompositeTelemetry.JobSucceeded(
  const AJob: IHefestoJobEnvelope;
  const ADurationMs: Int64);
var
  LItem: IHefestoTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobSucceeded(AJob, ADurationMs);
end;

class function THefestoCompositeTelemetry.New(
  const ATelemetries: array of IHefestoTelemetry): IHefestoTelemetry;
begin
  Result := THefestoCompositeTelemetry.Create(ATelemetries);
end;

procedure THefestoCompositeTelemetry.PollingStarted;
begin
  ForwardPollingStarted;
end;

procedure THefestoCompositeTelemetry.PollingStopped;
begin
  ForwardPollingStopped;
end;

procedure THefestoCompositeTelemetry.ServerStarted;
begin
  ForwardServerStarted;
end;

procedure THefestoCompositeTelemetry.ServerStopped;
begin
  ForwardServerStopped;
end;

{ THefestoInMemoryMetricTransport }

constructor THefestoInMemoryMetricTransport.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FPayloads := TList<string>.Create;
end;

destructor THefestoInMemoryMetricTransport.Destroy;
begin
  FPayloads.Free;
  FLock.Free;
  inherited;
end;

class function THefestoInMemoryMetricTransport.New: THefestoInMemoryMetricTransport;
begin
  Result := THefestoInMemoryMetricTransport.Create;
end;

function THefestoInMemoryMetricTransport.Payloads: TArray<string>;
begin
  FLock.Acquire;
  try
    Result := FPayloads.ToArray;
  finally
    FLock.Release;
  end;
end;

procedure THefestoInMemoryMetricTransport.Send(const APayload: string);
begin
  FLock.Acquire;
  try
    FPayloads.Add(APayload);
  finally
    FLock.Release;
  end;
end;

{ THefestoStatsDMetricsSink }

function THefestoStatsDMetricsSink.ComposeMetricName(const AName: string): string;
begin
  if FPrefix.Trim.IsEmpty then
    Result := AName
  else
    Result := FPrefix + '.' + AName;
end;

constructor THefestoStatsDMetricsSink.Create(
  const ATransport: IHefestoMetricTransport;
  const APrefix: string);
begin
  inherited Create;
  FTransport := ATransport;
  FPrefix := APrefix;
end;

procedure THefestoStatsDMetricsSink.Emit(
  const AName, AValue, AKind: string;
  const ATags: TStrings);
var
  LPayload: string;
begin
  if not Assigned(FTransport) then
    Exit;

  LPayload := ComposeMetricName(AName) + ':' + AValue + '|' + AKind;
  LPayload := LPayload + FormatTags(ATags);
  FTransport.Send(LPayload);
end;

function THefestoStatsDMetricsSink.FormatTags(const ATags: TStrings): string;
var
  LIndex: Integer;
begin
  Result := '';
  if not Assigned(ATags) or (ATags.Count = 0) then
    Exit;

  Result := '|#';
  for LIndex := 0 to Pred(ATags.Count) do
  begin
    if LIndex > 0 then
      Result := Result + ',';
    Result := Result + ATags[LIndex];
  end;
end;

procedure THefestoStatsDMetricsSink.Gauge(
  const AName: string;
  const AValue: Int64;
  const ATags: TStrings);
begin
  Emit(AName, IntToStr(AValue), 'g', ATags);
end;

procedure THefestoStatsDMetricsSink.Increment(
  const AName: string;
  const AValue: Int64;
  const ATags: TStrings);
begin
  Emit(AName, IntToStr(AValue), 'c', ATags);
end;

class function THefestoStatsDMetricsSink.New(
  const ATransport: IHefestoMetricTransport;
  const APrefix: string): IHefestoMetricsSink;
begin
  Result := THefestoStatsDMetricsSink.Create(ATransport, APrefix);
end;

procedure THefestoStatsDMetricsSink.Timing(
  const AName: string;
  const AValueMs: Int64;
  const ATags: TStrings);
begin
  Emit(AName, IntToStr(AValueMs), 'ms', ATags);
end;

{ THefestoMetricsTelemetry }

constructor THefestoMetricsTelemetry.Create(const ASink: IHefestoMetricsSink);
begin
  inherited Create;
  FSink := ASink;
end;

procedure THefestoMetricsTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;

  LTags := QueueTags(AQueueName);
  try
    FSink.Increment('fetch.operations', 1, LTags);
    FSink.Gauge('fetch.jobs', ACount, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.Idle(const ADelayMs: Integer);
begin
  if Assigned(FSink) then
    FSink.Timing('polling.idle_delay_ms', ADelayMs);
end;

class function THefestoMetricsTelemetry.JobTags(
  const AJob: IHefestoJobEnvelope): TStringList;
begin
  Result := TStringList.Create;
  if not Assigned(AJob) then
    Exit;
  Result.Add('queue:' + AJob.QueueName);
  Result.Add('action:' + AJob.Action);
end;

procedure THefestoMetricsTelemetry.JobAcked(const AJob: IHefestoJobEnvelope);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.acked', 1, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.JobDeadLettered(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.dead_lettered', 1, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.JobDiscarded(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.discarded', 1, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.JobFailed(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.failed', 1, LTags);
    FSink.Timing('jobs.duration_ms', ADurationMs, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.JobRetried(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.retried', 1, LTags);
    FSink.Gauge('jobs.retry_delay_seconds', ADelaySeconds, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.JobStarted(
  const AJob: IHefestoJobEnvelope);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.started', 1, LTags);
  finally
    LTags.Free;
  end;
end;

procedure THefestoMetricsTelemetry.JobSucceeded(
  const AJob: IHefestoJobEnvelope;
  const ADurationMs: Int64);
var
  LTags: TStringList;
begin
  if not Assigned(FSink) then
    Exit;
  LTags := JobTags(AJob);
  try
    FSink.Increment('jobs.succeeded', 1, LTags);
    FSink.Timing('jobs.duration_ms', ADurationMs, LTags);
  finally
    LTags.Free;
  end;
end;

class function THefestoMetricsTelemetry.New(
  const ASink: IHefestoMetricsSink): IHefestoTelemetry;
begin
  Result := THefestoMetricsTelemetry.Create(ASink);
end;

procedure THefestoMetricsTelemetry.PollingStarted;
begin
  if Assigned(FSink) then
    FSink.Increment('polling.started');
end;

procedure THefestoMetricsTelemetry.PollingStopped;
begin
  if Assigned(FSink) then
    FSink.Increment('polling.stopped');
end;

class function THefestoMetricsTelemetry.QueueTags(
  const AQueueName: string): TStringList;
begin
  Result := TStringList.Create;
  if not AQueueName.Trim.IsEmpty then
    Result.Add('queue:' + AQueueName);
end;

procedure THefestoMetricsTelemetry.ServerStarted;
begin
  if Assigned(FSink) then
    FSink.Increment('server.started');
end;

procedure THefestoMetricsTelemetry.ServerStopped;
begin
  if Assigned(FSink) then
    FSink.Increment('server.stopped');
end;

{ THefestoHistoricalMetricsTelemetry }

constructor THefestoHistoricalMetricsTelemetry.Create(
  const ABucketSeconds: Integer;
  const AMaxBuckets: Integer);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FBuckets := TList<THefestoHistoricalMetricSnapshot>.Create;
  if ABucketSeconds > 0 then
    FBucketSeconds := ABucketSeconds
  else
    FBucketSeconds := 60;
  if AMaxBuckets > 0 then
    FMaxBuckets := AMaxBuckets
  else
    FMaxBuckets := 60;
end;

destructor THefestoHistoricalMetricsTelemetry.Destroy;
begin
  FBuckets.Free;
  FLock.Free;
  inherited;
end;

function THefestoHistoricalMetricsTelemetry.EnsureBucketIndexLocked(
  const AValue: TDateTime): Integer;
var
  LBucketStart: TDateTime;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LBucketStart := ResolveBucketStart(AValue, FBucketSeconds);
  if (FBuckets.Count = 0)
    or (FBuckets[FBuckets.Count - 1].BucketStartedAt <> LBucketStart) then
  begin
    FillChar(LBucket, SizeOf(LBucket), 0);
    LBucket.BucketStartedAt := LBucketStart;
    FBuckets.Add(LBucket);
    TrimBucketsLocked;
  end;
  Result := FBuckets.Count - 1;
end;

procedure THefestoHistoricalMetricsTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
begin
  FLock.Acquire;
  try
    UpdateFetchLocked(ACount);
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.Idle(const ADelayMs: Integer);
begin
end;

procedure THefestoHistoricalMetricsTelemetry.JobAcked(
  const AJob: IHefestoJobEnvelope);
begin
  FLock.Acquire;
  try
    UpdateAckLocked;
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.JobDeadLettered(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
  FLock.Acquire;
  try
    UpdateDeadLetterLocked;
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.JobDiscarded(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
  FLock.Acquire;
  try
    UpdateDiscardLocked;
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.JobFailed(
  const AJob: IHefestoJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
begin
  FLock.Acquire;
  try
    UpdateCompletionLocked(False, ADurationMs);
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.JobRetried(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
  FLock.Acquire;
  try
    UpdateRetryLocked;
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.JobStarted(
  const AJob: IHefestoJobEnvelope);
begin
  FLock.Acquire;
  try
    UpdateStartLocked;
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.JobSucceeded(
  const AJob: IHefestoJobEnvelope;
  const ADurationMs: Int64);
begin
  FLock.Acquire;
  try
    UpdateCompletionLocked(True, ADurationMs);
  finally
    FLock.Release;
  end;
end;

class function THefestoHistoricalMetricsTelemetry.New(
  const ABucketSeconds: Integer;
  const AMaxBuckets: Integer): IHefestoTelemetry;
begin
  Result := THefestoHistoricalMetricsTelemetry.Create(ABucketSeconds, AMaxBuckets);
end;

procedure THefestoHistoricalMetricsTelemetry.PollingStarted;
begin
end;

procedure THefestoHistoricalMetricsTelemetry.PollingStopped;
begin
end;

class function THefestoHistoricalMetricsTelemetry.ResolveBucketStart(
  const AValue: TDateTime;
  const ABucketSeconds: Integer): TDateTime;
var
  LUnix: Int64;
begin
  LUnix := DateTimeToUnix(AValue);
  LUnix := (LUnix div ABucketSeconds) * ABucketSeconds;
  Result := UnixToDateTime(LUnix);
end;

procedure THefestoHistoricalMetricsTelemetry.ServerStarted;
begin
end;

procedure THefestoHistoricalMetricsTelemetry.ServerStopped;
begin
end;

function THefestoHistoricalMetricsTelemetry.Snapshot: TArray<THefestoHistoricalMetricSnapshot>;
begin
  FLock.Acquire;
  try
    Result := FBuckets.ToArray;
  finally
    FLock.Release;
  end;
end;

procedure THefestoHistoricalMetricsTelemetry.TrimBucketsLocked;
begin
  while FBuckets.Count > FMaxBuckets do
    FBuckets.Delete(0);
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateAckLocked;
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.AckedJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateCompletionLocked(
  const ASucceeded: Boolean;
  const ADurationMs: Int64);
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  if ASucceeded then
    Inc(LBucket.SucceededJobs)
  else
    Inc(LBucket.FailedJobs);
  Inc(LBucket.TotalDurationMs, ADurationMs);
  FBuckets[LIndex] := LBucket;
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateDeadLetterLocked;
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.DeadLetteredJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateDiscardLocked;
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.DiscardedJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateFetchLocked(
  const ACount: Integer);
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.FetchOperations);
  Inc(LBucket.FetchedJobs, ACount);
  FBuckets[LIndex] := LBucket;
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateRetryLocked;
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.RetriedJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure THefestoHistoricalMetricsTelemetry.UpdateStartLocked;
var
  LIndex: Integer;
  LBucket: THefestoHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.StartedJobs);
  FBuckets[LIndex] := LBucket;
end;

{ THefestoHistoricalMetricsTelemetry }

{ repeated comment intentionally omitted }

{ no-op server lifecycle methods intentionally blank }

end.
