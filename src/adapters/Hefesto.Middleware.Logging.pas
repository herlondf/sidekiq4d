unit Hefesto.Middleware.Logging;

interface

uses
  System.SysUtils,
  System.DateUtils,
  System.Diagnostics,
  System.JSON,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Middleware;

type
  THefestoLoggingMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FOnLog: TProc<string>;

    function BuildJson(const AJobId, AAction, AStatus: string;
      ADurationMs: Int64; const AError: string): string;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoLoggingMiddleware;

    function OnLog(AProc: TProc<string>): THefestoLoggingMiddleware;

    // IHefestoServerMiddleware
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
  end;

implementation

{ THefestoLoggingMiddleware }

constructor THefestoLoggingMiddleware.Create;
begin
  inherited Create;
  FOnLog := nil;
end;

destructor THefestoLoggingMiddleware.Destroy;
begin
  inherited;
end;

class function THefestoLoggingMiddleware.New: THefestoLoggingMiddleware;
begin
  Result := THefestoLoggingMiddleware.Create;
end;

function THefestoLoggingMiddleware.OnLog(
  AProc: TProc<string>): THefestoLoggingMiddleware;
begin
  Result := Self;
  FOnLog := AProc;
end;

function THefestoLoggingMiddleware.BuildJson(const AJobId, AAction,
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

procedure THefestoLoggingMiddleware.Call(const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
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
