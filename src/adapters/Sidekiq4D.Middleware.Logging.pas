unit Sidekiq4D.Middleware.Logging;

interface

uses
  System.SysUtils,
  System.DateUtils,
  System.Diagnostics,
  System.JSON,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Middleware;

type
  TSidekiqLoggingMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FOnLog: TProc<string>;

    function BuildJson(const AJobId, AAction, AStatus: string;
      ADurationMs: Int64; const AError: string): string;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqLoggingMiddleware;

    function OnLog(AProc: TProc<string>): TSidekiqLoggingMiddleware;

    // ISidekiqServerMiddleware
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
  end;

implementation

{ TSidekiqLoggingMiddleware }

constructor TSidekiqLoggingMiddleware.Create;
begin
  inherited Create;
  FOnLog := nil;
end;

destructor TSidekiqLoggingMiddleware.Destroy;
begin
  inherited;
end;

class function TSidekiqLoggingMiddleware.New: TSidekiqLoggingMiddleware;
begin
  Result := TSidekiqLoggingMiddleware.Create;
end;

function TSidekiqLoggingMiddleware.OnLog(
  AProc: TProc<string>): TSidekiqLoggingMiddleware;
begin
  Result := Self;
  FOnLog := AProc;
end;

function TSidekiqLoggingMiddleware.BuildJson(const AJobId, AAction,
  AStatus: string; ADurationMs: Int64; const AError: string): string;
var
  LJson: TJSONObject;
begin
  LJson := TJSONObject.Create;
  try
    LJson.AddPair('timestamp', DateToISO8601(Now, False));
    LJson.AddPair('job_id', AJobId);
    LJson.AddPair('action', AAction);
    LJson.AddPair('duration_ms', TJSONNumber.Create(ADurationMs));
    LJson.AddPair('status', AStatus);
    if AError <> '' then
      LJson.AddPair('error', AError);
    Result := LJson.ToJSON;
  finally
    LJson.Free;
  end;
end;

procedure TSidekiqLoggingMiddleware.Call(const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
var
  LSw: TStopwatch;
  LLogLine: string;
begin
  if Assigned(FOnLog) then
  begin
    LLogLine := BuildJson(AJob.Id, AJob.Action, 'started', 0, '');
    FOnLog(LLogLine);
  end;

  LSw := TStopwatch.StartNew;
  try
    ANext;
    LSw.Stop;

    if Assigned(FOnLog) then
    begin
      LLogLine := BuildJson(AJob.Id, AJob.Action, 'succeeded',
        LSw.ElapsedMilliseconds, '');
      FOnLog(LLogLine);
    end;
  except
    on E: Exception do
    begin
      LSw.Stop;

      if Assigned(FOnLog) then
      begin
        LLogLine := BuildJson(AJob.Id, AJob.Action, 'failed',
          LSw.ElapsedMilliseconds, E.Message);
        FOnLog(LLogLine);
      end;

      raise;
    end;
  end;
end;

end.
