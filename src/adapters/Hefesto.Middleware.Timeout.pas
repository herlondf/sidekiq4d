unit Hefesto.Middleware.Timeout;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Middleware;

type
  EHefestoTimeoutException = class(Exception);

  THefestoTimeoutMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FTimeoutMs: Cardinal;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoTimeoutMiddleware;

    function TimeoutMs(AValue: Cardinal): THefestoTimeoutMiddleware;

    // IHefestoServerMiddleware
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
  end;

implementation

{ THefestoTimeoutMiddleware }

constructor THefestoTimeoutMiddleware.Create;
begin
  inherited Create;
  FTimeoutMs := 5000;
end;

destructor THefestoTimeoutMiddleware.Destroy;
begin
  inherited;
end;

class function THefestoTimeoutMiddleware.New: THefestoTimeoutMiddleware;
begin
  Result := THefestoTimeoutMiddleware.Create;
end;

function THefestoTimeoutMiddleware.TimeoutMs(
  AValue: Cardinal): THefestoTimeoutMiddleware;
begin
  Result := Self;
  FTimeoutMs := AValue;
end;

procedure THefestoTimeoutMiddleware.Call(const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
var
  LDoneEvent: TEvent;
  LException: Exception;
  LTimedOut: Boolean;
begin
  LDoneEvent := TEvent.Create(nil, True, False, '');
  LException := nil;
  LTimedOut := False;
  try
    TThread.CreateAnonymousThread(
      procedure
      begin
        try
          ANext;
        except
          on E: Exception do
            LException := Exception(AcquireExceptionObject);
        end;
        LDoneEvent.SetEvent;
      end
    ).Start;

    if LDoneEvent.WaitFor(FTimeoutMs) = wrTimeout then
    begin
      LTimedOut := True;
    end;
  finally
    LDoneEvent.Free;
  end;

  if LTimedOut then
    raise EHefestoTimeoutException.CreateFmt(
      'Job %s timed out after %d ms', [AJob.Id, FTimeoutMs]);

  if Assigned(LException) then
    raise LException;
end;

end.
