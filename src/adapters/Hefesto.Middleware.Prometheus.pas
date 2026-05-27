unit Hefesto.Middleware.Prometheus;

interface

uses
  System.SysUtils,
  System.Diagnostics,
  System.SyncObjs,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Middleware;

type
  THefestoPrometheusMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
  private
    FJobsTotal: Integer;
    FJobsFailedTotal: Integer;
    FLock: TCriticalSection;
    // Histogram buckets (seconds): 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
    FBuckets: TArray<Double>;
    FBucketCounts: TArray<Integer>;
    FDurationSum: Double;
    FDurationCount: Integer;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoPrometheusMiddleware;

    // IHefestoServerMiddleware
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);

    // Expose metrics in Prometheus text format
    function Expose: string;
  end;

implementation

const
  DEFAULT_BUCKETS: array[0..10] of Double = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
  );

{ THefestoPrometheusMiddleware }

constructor THefestoPrometheusMiddleware.Create;
var
  I: Integer;
begin
  inherited Create;
  FJobsTotal := 0;
  FJobsFailedTotal := 0;
  FDurationSum := 0;
  FDurationCount := 0;
  FLock := TCriticalSection.Create;

  SetLength(FBuckets, Length(DEFAULT_BUCKETS));
  SetLength(FBucketCounts, Length(DEFAULT_BUCKETS));
  for I := Low(DEFAULT_BUCKETS) to High(DEFAULT_BUCKETS) do
  begin
    FBuckets[I] := DEFAULT_BUCKETS[I];
    FBucketCounts[I] := 0;
  end;
end;

destructor THefestoPrometheusMiddleware.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function THefestoPrometheusMiddleware.New: THefestoPrometheusMiddleware;
begin
  Result := THefestoPrometheusMiddleware.Create;
end;

procedure THefestoPrometheusMiddleware.Call(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
var
  LSw: TStopwatch;
  LDurationSec: Double;
  I: Integer;
begin
  TInterlocked.Increment(FJobsTotal);

  LSw := TStopwatch.StartNew;
  try
    ANext;
  except
    TInterlocked.Increment(FJobsFailedTotal);
    LSw.Stop;
    LDurationSec := LSw.ElapsedMilliseconds / 1000.0;

    FLock.Enter;
    try
      FDurationSum := FDurationSum + LDurationSec;
      Inc(FDurationCount);
      for I := Low(FBuckets) to High(FBuckets) do
      begin
        if LDurationSec <= FBuckets[I] then
          Inc(FBucketCounts[I]);
      end;
    finally
      FLock.Leave;
    end;

    raise;
  end;

  LSw.Stop;
  LDurationSec := LSw.ElapsedMilliseconds / 1000.0;

  FLock.Enter;
  try
    FDurationSum := FDurationSum + LDurationSec;
    Inc(FDurationCount);
    for I := Low(FBuckets) to High(FBuckets) do
    begin
      if LDurationSec <= FBuckets[I] then
        Inc(FBucketCounts[I]);
    end;
  finally
    FLock.Leave;
  end;
end;

function THefestoPrometheusMiddleware.Expose: string;
var
  LSb: TStringBuilder;
  I: Integer;
  LJobsTotal, LJobsFailedTotal: Integer;
begin
  LSb := TStringBuilder.Create;
  try
    // Snapshot all mutable state under a single lock for a consistent report.
    FLock.Enter;
    try
      LJobsTotal       := FJobsTotal;
      LJobsFailedTotal := FJobsFailedTotal;

      // Counters
      LSb.AppendLine('# HELP sidekiq_jobs_total Total number of jobs processed.');
      LSb.AppendLine('# TYPE sidekiq_jobs_total counter');
      LSb.AppendLine(Format('sidekiq_jobs_total %d', [LJobsTotal]));
      LSb.AppendLine('# HELP sidekiq_jobs_failed_total Total number of failed jobs.');
      LSb.AppendLine('# TYPE sidekiq_jobs_failed_total counter');
      LSb.AppendLine(Format('sidekiq_jobs_failed_total %d', [LJobsFailedTotal]));

      // Histogram
      LSb.AppendLine('# HELP sidekiq_job_duration_seconds Duration of job execution.');
      LSb.AppendLine('# TYPE sidekiq_job_duration_seconds histogram');
      for I := Low(FBuckets) to High(FBuckets) do
      begin
        LSb.AppendLine(Format('sidekiq_job_duration_seconds_bucket{le="%.3f"} %d',
          [FBuckets[I], FBucketCounts[I]], TFormatSettings.Invariant));
      end;
      LSb.AppendLine(Format('sidekiq_job_duration_seconds_bucket{le="+Inf"} %d',
        [FDurationCount]));
      LSb.AppendLine(Format('sidekiq_job_duration_seconds_sum %.6f',
        [FDurationSum], TFormatSettings.Invariant));
      LSb.AppendLine(Format('sidekiq_job_duration_seconds_count %d',
        [FDurationCount]));
    finally
      FLock.Leave;
    end;

    Result := LSb.ToString;
  finally
    LSb.Free;
  end;
end;

end.
