unit Sidekiq4D.Telemetry;

interface

uses
  System.SysUtils,
  System.Classes,
  System.DateUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Sidekiq4D.Job;

type
  TSidekiqHistoricalMetricSnapshot = record
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

  ISidekiqTelemetry = interface
    ['{6412AF8C-E486-45B5-8422-D4BDF7EB8884}']
    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

  ISidekiqMetricTransport = interface
    ['{C97492BC-D891-4E18-8E88-B3707E5934B8}']
    procedure Send(const APayload: string);
  end;

  ISidekiqMetricsSink = interface
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

  ISidekiqHistoricalMetrics = interface
    ['{E2EF98A7-78A4-4231-9D40-4D3401EFE225}']
    function Snapshot: TArray<TSidekiqHistoricalMetricSnapshot>;
  end;

  TSidekiqNoopTelemetry = class(TInterfacedObject, ISidekiqTelemetry)
  public
    class function New: ISidekiqTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

  TSidekiqConsoleTelemetry = class(TInterfacedObject, ISidekiqTelemetry)
  public
    class function New: ISidekiqTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

  TSidekiqCompositeTelemetry = class(TInterfacedObject, ISidekiqTelemetry)
  private
    FItems: TArray<ISidekiqTelemetry>;
    procedure ForwardServerStarted;
    procedure ForwardServerStopped;
    procedure ForwardPollingStarted;
    procedure ForwardPollingStopped;
  public
    constructor Create(const ATelemetries: array of ISidekiqTelemetry);
    class function New(const ATelemetries: array of ISidekiqTelemetry): ISidekiqTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

  TSidekiqInMemoryMetricTransport = class(TInterfacedObject, ISidekiqMetricTransport)
  private
    FLock: TCriticalSection;
    FPayloads: TList<string>;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqInMemoryMetricTransport;

    procedure Send(const APayload: string);
    function Payloads: TArray<string>;
  end;

  TSidekiqStatsDMetricsSink = class(TInterfacedObject, ISidekiqMetricsSink)
  private
    FTransport: ISidekiqMetricTransport;
    FPrefix: string;

    function ComposeMetricName(const AName: string): string;
    function FormatTags(const ATags: TStrings): string;
    procedure Emit(
      const AName, AValue, AKind: string;
      const ATags: TStrings);
  public
    constructor Create(
      const ATransport: ISidekiqMetricTransport;
      const APrefix: string = 'sidekiq4delphi');
    class function New(
      const ATransport: ISidekiqMetricTransport;
      const APrefix: string = 'sidekiq4delphi'): ISidekiqMetricsSink;

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

  TSidekiqMetricsTelemetry = class(TInterfacedObject, ISidekiqTelemetry)
  private
    FSink: ISidekiqMetricsSink;

    class function QueueTags(const AQueueName: string): TStringList; static;
    class function JobTags(const AJob: ISidekiqJobEnvelope): TStringList; static;
  public
    constructor Create(const ASink: ISidekiqMetricsSink);
    class function New(const ASink: ISidekiqMetricsSink): ISidekiqTelemetry;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

  TSidekiqHistoricalMetricsTelemetry = class(TInterfacedObject, ISidekiqTelemetry, ISidekiqHistoricalMetrics)
  private
    FLock: TCriticalSection;
    FBucketSeconds: Integer;
    FMaxBuckets: Integer;
    FBuckets: TList<TSidekiqHistoricalMetricSnapshot>;

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
      const AMaxBuckets: Integer = 60): ISidekiqTelemetry;

    function Snapshot: TArray<TSidekiqHistoricalMetricSnapshot>;

    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

implementation

{ TSidekiqHistoricalMetricSnapshot }

function TSidekiqHistoricalMetricSnapshot.AverageDurationMs: Double;
var
  LCompleted: Integer;
begin
  LCompleted := SucceededJobs + FailedJobs;
  if LCompleted = 0 then
    Exit(0);
  Result := TotalDurationMs / LCompleted;
end;

{ TSidekiqNoopTelemetry }

procedure TSidekiqNoopTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
begin
end;

procedure TSidekiqNoopTelemetry.Idle(const ADelayMs: Integer);
begin
end;

procedure TSidekiqNoopTelemetry.JobAcked(const AJob: ISidekiqJobEnvelope);
begin
end;

procedure TSidekiqNoopTelemetry.JobDeadLettered(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
end;

procedure TSidekiqNoopTelemetry.JobDiscarded(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
end;

procedure TSidekiqNoopTelemetry.JobFailed(
  const AJob: ISidekiqJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
begin
end;

procedure TSidekiqNoopTelemetry.JobRetried(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
begin
end;

procedure TSidekiqNoopTelemetry.JobStarted(const AJob: ISidekiqJobEnvelope);
begin
end;

procedure TSidekiqNoopTelemetry.JobSucceeded(
  const AJob: ISidekiqJobEnvelope;
  const ADurationMs: Int64);
begin
end;

class function TSidekiqNoopTelemetry.New: ISidekiqTelemetry;
begin
  Result := TSidekiqNoopTelemetry.Create;
end;

procedure TSidekiqNoopTelemetry.PollingStarted;
begin
end;

procedure TSidekiqNoopTelemetry.PollingStopped;
begin
end;

procedure TSidekiqNoopTelemetry.ServerStarted;
begin
end;

procedure TSidekiqNoopTelemetry.ServerStopped;
begin
end;

{ TSidekiqConsoleTelemetry }

// Removes newlines and caps length to prevent log injection via user-controlled fields.
function SanitizeLogField(const AValue: string; AMaxLen: Integer = 120): string;
begin
  Result := AValue.Replace(#13, ' ').Replace(#10, ' ').Replace(#9, ' ');
  if Length(Result) > AMaxLen then
    Result := Copy(Result, 1, AMaxLen) + '...';
end;

procedure TSidekiqConsoleTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
begin
  Writeln(Format('sidekiq4delphi fetch queue=%s count=%d', [AQueueName, ACount]));
end;

procedure TSidekiqConsoleTelemetry.Idle(const ADelayMs: Integer);
begin
  Writeln(Format('sidekiq4delphi idle delay_ms=%d', [ADelayMs]));
end;

procedure TSidekiqConsoleTelemetry.JobAcked(const AJob: ISidekiqJobEnvelope);
begin
  Writeln(Format('sidekiq4delphi ack job=%s action=%s',
    [AJob.Id, SanitizeLogField(AJob.Action)]));
end;

procedure TSidekiqConsoleTelemetry.JobDeadLettered(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
  Writeln(Format(
    'sidekiq4delphi dead_letter job=%s action=%s reason=%s',
    [AJob.Id, SanitizeLogField(AJob.Action), SanitizeLogField(AReason)]));
end;

procedure TSidekiqConsoleTelemetry.JobDiscarded(
  const AJob: ISidekiqJobEnvelope;
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

procedure TSidekiqConsoleTelemetry.JobFailed(
  const AJob: ISidekiqJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
begin
  Writeln(Format(
    'sidekiq4delphi fail job=%s action=%s duration_ms=%d error=%s',
    [AJob.Id, SanitizeLogField(AJob.Action), ADurationMs,
     SanitizeLogField(AError.Message)]));
end;

procedure TSidekiqConsoleTelemetry.JobRetried(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
begin
  Writeln(Format(
    'sidekiq4delphi retry job=%s action=%s delay_seconds=%d',
    [AJob.Id, SanitizeLogField(AJob.Action), ADelaySeconds]));
end;

procedure TSidekiqConsoleTelemetry.JobStarted(
  const AJob: ISidekiqJobEnvelope);
begin
  Writeln(Format('sidekiq4delphi start job=%s action=%s',
    [AJob.Id, SanitizeLogField(AJob.Action)]));
end;

procedure TSidekiqConsoleTelemetry.JobSucceeded(
  const AJob: ISidekiqJobEnvelope;
  const ADurationMs: Int64);
begin
  Writeln(Format(
    'sidekiq4delphi success job=%s action=%s duration_ms=%d',
    [AJob.Id, AJob.Action, ADurationMs]));
end;

class function TSidekiqConsoleTelemetry.New: ISidekiqTelemetry;
begin
  Result := TSidekiqConsoleTelemetry.Create;
end;

procedure TSidekiqConsoleTelemetry.PollingStarted;
begin
  Writeln('sidekiq4delphi polling started');
end;

procedure TSidekiqConsoleTelemetry.PollingStopped;
begin
  Writeln('sidekiq4delphi polling stopped');
end;

procedure TSidekiqConsoleTelemetry.ServerStarted;
begin
  Writeln('sidekiq4delphi server started');
end;

procedure TSidekiqConsoleTelemetry.ServerStopped;
begin
  Writeln('sidekiq4delphi server stopped');
end;

{ TSidekiqCompositeTelemetry }

constructor TSidekiqCompositeTelemetry.Create(
  const ATelemetries: array of ISidekiqTelemetry);
var
  LIndex: Integer;
begin
  inherited Create;
  SetLength(FItems, Length(ATelemetries));
  for LIndex := 0 to Pred(Length(ATelemetries)) do
    FItems[LIndex] := ATelemetries[LIndex];
end;

procedure TSidekiqCompositeTelemetry.FetchFinished(
  const AQueueName: string;
  const ACount: Integer);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.FetchFinished(AQueueName, ACount);
end;

procedure TSidekiqCompositeTelemetry.ForwardPollingStarted;
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.PollingStarted;
end;

procedure TSidekiqCompositeTelemetry.ForwardPollingStopped;
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.PollingStopped;
end;

procedure TSidekiqCompositeTelemetry.ForwardServerStarted;
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.ServerStarted;
end;

procedure TSidekiqCompositeTelemetry.ForwardServerStopped;
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.ServerStopped;
end;

procedure TSidekiqCompositeTelemetry.Idle(const ADelayMs: Integer);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.Idle(ADelayMs);
end;

procedure TSidekiqCompositeTelemetry.JobAcked(const AJob: ISidekiqJobEnvelope);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobAcked(AJob);
end;

procedure TSidekiqCompositeTelemetry.JobDeadLettered(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobDeadLettered(AJob, AReason);
end;

procedure TSidekiqCompositeTelemetry.JobDiscarded(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobDiscarded(AJob, AReason);
end;

procedure TSidekiqCompositeTelemetry.JobFailed(
  const AJob: ISidekiqJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobFailed(AJob, AError, ADurationMs);
end;

procedure TSidekiqCompositeTelemetry.JobRetried(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobRetried(AJob, ADelaySeconds);
end;

procedure TSidekiqCompositeTelemetry.JobStarted(
  const AJob: ISidekiqJobEnvelope);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobStarted(AJob);
end;

procedure TSidekiqCompositeTelemetry.JobSucceeded(
  const AJob: ISidekiqJobEnvelope;
  const ADurationMs: Int64);
var
  LItem: ISidekiqTelemetry;
begin
  for LItem in FItems do
    if Assigned(LItem) then
      LItem.JobSucceeded(AJob, ADurationMs);
end;

class function TSidekiqCompositeTelemetry.New(
  const ATelemetries: array of ISidekiqTelemetry): ISidekiqTelemetry;
begin
  Result := TSidekiqCompositeTelemetry.Create(ATelemetries);
end;

procedure TSidekiqCompositeTelemetry.PollingStarted;
begin
  ForwardPollingStarted;
end;

procedure TSidekiqCompositeTelemetry.PollingStopped;
begin
  ForwardPollingStopped;
end;

procedure TSidekiqCompositeTelemetry.ServerStarted;
begin
  ForwardServerStarted;
end;

procedure TSidekiqCompositeTelemetry.ServerStopped;
begin
  ForwardServerStopped;
end;

{ TSidekiqInMemoryMetricTransport }

constructor TSidekiqInMemoryMetricTransport.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FPayloads := TList<string>.Create;
end;

destructor TSidekiqInMemoryMetricTransport.Destroy;
begin
  FPayloads.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqInMemoryMetricTransport.New: TSidekiqInMemoryMetricTransport;
begin
  Result := TSidekiqInMemoryMetricTransport.Create;
end;

function TSidekiqInMemoryMetricTransport.Payloads: TArray<string>;
begin
  FLock.Acquire;
  try
    Result := FPayloads.ToArray;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqInMemoryMetricTransport.Send(const APayload: string);
begin
  FLock.Acquire;
  try
    FPayloads.Add(APayload);
  finally
    FLock.Release;
  end;
end;

{ TSidekiqStatsDMetricsSink }

function TSidekiqStatsDMetricsSink.ComposeMetricName(const AName: string): string;
begin
  if FPrefix.Trim.IsEmpty then
    Result := AName
  else
    Result := FPrefix + '.' + AName;
end;

constructor TSidekiqStatsDMetricsSink.Create(
  const ATransport: ISidekiqMetricTransport;
  const APrefix: string);
begin
  inherited Create;
  FTransport := ATransport;
  FPrefix := APrefix;
end;

procedure TSidekiqStatsDMetricsSink.Emit(
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

function TSidekiqStatsDMetricsSink.FormatTags(const ATags: TStrings): string;
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

procedure TSidekiqStatsDMetricsSink.Gauge(
  const AName: string;
  const AValue: Int64;
  const ATags: TStrings);
begin
  Emit(AName, IntToStr(AValue), 'g', ATags);
end;

procedure TSidekiqStatsDMetricsSink.Increment(
  const AName: string;
  const AValue: Int64;
  const ATags: TStrings);
begin
  Emit(AName, IntToStr(AValue), 'c', ATags);
end;

class function TSidekiqStatsDMetricsSink.New(
  const ATransport: ISidekiqMetricTransport;
  const APrefix: string): ISidekiqMetricsSink;
begin
  Result := TSidekiqStatsDMetricsSink.Create(ATransport, APrefix);
end;

procedure TSidekiqStatsDMetricsSink.Timing(
  const AName: string;
  const AValueMs: Int64;
  const ATags: TStrings);
begin
  Emit(AName, IntToStr(AValueMs), 'ms', ATags);
end;

{ TSidekiqMetricsTelemetry }

constructor TSidekiqMetricsTelemetry.Create(const ASink: ISidekiqMetricsSink);
begin
  inherited Create;
  FSink := ASink;
end;

procedure TSidekiqMetricsTelemetry.FetchFinished(
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

procedure TSidekiqMetricsTelemetry.Idle(const ADelayMs: Integer);
begin
  if Assigned(FSink) then
    FSink.Timing('polling.idle_delay_ms', ADelayMs);
end;

class function TSidekiqMetricsTelemetry.JobTags(
  const AJob: ISidekiqJobEnvelope): TStringList;
begin
  Result := TStringList.Create;
  if not Assigned(AJob) then
    Exit;
  Result.Add('queue:' + AJob.QueueName);
  Result.Add('action:' + AJob.Action);
end;

procedure TSidekiqMetricsTelemetry.JobAcked(const AJob: ISidekiqJobEnvelope);
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

procedure TSidekiqMetricsTelemetry.JobDeadLettered(
  const AJob: ISidekiqJobEnvelope;
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

procedure TSidekiqMetricsTelemetry.JobDiscarded(
  const AJob: ISidekiqJobEnvelope;
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

procedure TSidekiqMetricsTelemetry.JobFailed(
  const AJob: ISidekiqJobEnvelope;
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

procedure TSidekiqMetricsTelemetry.JobRetried(
  const AJob: ISidekiqJobEnvelope;
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

procedure TSidekiqMetricsTelemetry.JobStarted(
  const AJob: ISidekiqJobEnvelope);
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

procedure TSidekiqMetricsTelemetry.JobSucceeded(
  const AJob: ISidekiqJobEnvelope;
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

class function TSidekiqMetricsTelemetry.New(
  const ASink: ISidekiqMetricsSink): ISidekiqTelemetry;
begin
  Result := TSidekiqMetricsTelemetry.Create(ASink);
end;

procedure TSidekiqMetricsTelemetry.PollingStarted;
begin
  if Assigned(FSink) then
    FSink.Increment('polling.started');
end;

procedure TSidekiqMetricsTelemetry.PollingStopped;
begin
  if Assigned(FSink) then
    FSink.Increment('polling.stopped');
end;

class function TSidekiqMetricsTelemetry.QueueTags(
  const AQueueName: string): TStringList;
begin
  Result := TStringList.Create;
  if not AQueueName.Trim.IsEmpty then
    Result.Add('queue:' + AQueueName);
end;

procedure TSidekiqMetricsTelemetry.ServerStarted;
begin
  if Assigned(FSink) then
    FSink.Increment('server.started');
end;

procedure TSidekiqMetricsTelemetry.ServerStopped;
begin
  if Assigned(FSink) then
    FSink.Increment('server.stopped');
end;

{ TSidekiqHistoricalMetricsTelemetry }

constructor TSidekiqHistoricalMetricsTelemetry.Create(
  const ABucketSeconds: Integer;
  const AMaxBuckets: Integer);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FBuckets := TList<TSidekiqHistoricalMetricSnapshot>.Create;
  if ABucketSeconds > 0 then
    FBucketSeconds := ABucketSeconds
  else
    FBucketSeconds := 60;
  if AMaxBuckets > 0 then
    FMaxBuckets := AMaxBuckets
  else
    FMaxBuckets := 60;
end;

destructor TSidekiqHistoricalMetricsTelemetry.Destroy;
begin
  FBuckets.Free;
  FLock.Free;
  inherited;
end;

function TSidekiqHistoricalMetricsTelemetry.EnsureBucketIndexLocked(
  const AValue: TDateTime): Integer;
var
  LBucketStart: TDateTime;
  LBucket: TSidekiqHistoricalMetricSnapshot;
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

procedure TSidekiqHistoricalMetricsTelemetry.FetchFinished(
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

procedure TSidekiqHistoricalMetricsTelemetry.Idle(const ADelayMs: Integer);
begin
end;

procedure TSidekiqHistoricalMetricsTelemetry.JobAcked(
  const AJob: ISidekiqJobEnvelope);
begin
  FLock.Acquire;
  try
    UpdateAckLocked;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHistoricalMetricsTelemetry.JobDeadLettered(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
  FLock.Acquire;
  try
    UpdateDeadLetterLocked;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHistoricalMetricsTelemetry.JobDiscarded(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
  FLock.Acquire;
  try
    UpdateDiscardLocked;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHistoricalMetricsTelemetry.JobFailed(
  const AJob: ISidekiqJobEnvelope;
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

procedure TSidekiqHistoricalMetricsTelemetry.JobRetried(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
begin
  FLock.Acquire;
  try
    UpdateRetryLocked;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHistoricalMetricsTelemetry.JobStarted(
  const AJob: ISidekiqJobEnvelope);
begin
  FLock.Acquire;
  try
    UpdateStartLocked;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHistoricalMetricsTelemetry.JobSucceeded(
  const AJob: ISidekiqJobEnvelope;
  const ADurationMs: Int64);
begin
  FLock.Acquire;
  try
    UpdateCompletionLocked(True, ADurationMs);
  finally
    FLock.Release;
  end;
end;

class function TSidekiqHistoricalMetricsTelemetry.New(
  const ABucketSeconds: Integer;
  const AMaxBuckets: Integer): ISidekiqTelemetry;
begin
  Result := TSidekiqHistoricalMetricsTelemetry.Create(ABucketSeconds, AMaxBuckets);
end;

procedure TSidekiqHistoricalMetricsTelemetry.PollingStarted;
begin
end;

procedure TSidekiqHistoricalMetricsTelemetry.PollingStopped;
begin
end;

class function TSidekiqHistoricalMetricsTelemetry.ResolveBucketStart(
  const AValue: TDateTime;
  const ABucketSeconds: Integer): TDateTime;
var
  LUnix: Int64;
begin
  LUnix := DateTimeToUnix(AValue);
  LUnix := (LUnix div ABucketSeconds) * ABucketSeconds;
  Result := UnixToDateTime(LUnix);
end;

procedure TSidekiqHistoricalMetricsTelemetry.ServerStarted;
begin
end;

procedure TSidekiqHistoricalMetricsTelemetry.ServerStopped;
begin
end;

function TSidekiqHistoricalMetricsTelemetry.Snapshot: TArray<TSidekiqHistoricalMetricSnapshot>;
begin
  FLock.Acquire;
  try
    Result := FBuckets.ToArray;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHistoricalMetricsTelemetry.TrimBucketsLocked;
begin
  while FBuckets.Count > FMaxBuckets do
    FBuckets.Delete(0);
end;

procedure TSidekiqHistoricalMetricsTelemetry.UpdateAckLocked;
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.AckedJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure TSidekiqHistoricalMetricsTelemetry.UpdateCompletionLocked(
  const ASucceeded: Boolean;
  const ADurationMs: Int64);
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
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

procedure TSidekiqHistoricalMetricsTelemetry.UpdateDeadLetterLocked;
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.DeadLetteredJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure TSidekiqHistoricalMetricsTelemetry.UpdateDiscardLocked;
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.DiscardedJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure TSidekiqHistoricalMetricsTelemetry.UpdateFetchLocked(
  const ACount: Integer);
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.FetchOperations);
  Inc(LBucket.FetchedJobs, ACount);
  FBuckets[LIndex] := LBucket;
end;

procedure TSidekiqHistoricalMetricsTelemetry.UpdateRetryLocked;
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.RetriedJobs);
  FBuckets[LIndex] := LBucket;
end;

procedure TSidekiqHistoricalMetricsTelemetry.UpdateStartLocked;
var
  LIndex: Integer;
  LBucket: TSidekiqHistoricalMetricSnapshot;
begin
  LIndex := EnsureBucketIndexLocked(Now);
  LBucket := FBuckets[LIndex];
  Inc(LBucket.StartedJobs);
  FBuckets[LIndex] := LBucket;
end;

{ TSidekiqHistoricalMetricsTelemetry }

{ repeated comment intentionally omitted }

{ no-op server lifecycle methods intentionally blank }

end.
