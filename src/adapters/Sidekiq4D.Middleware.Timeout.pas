unit Sidekiq4D.Middleware.Timeout;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Middleware;

type
  ESidekiqTimeoutException = class(Exception);

  TSidekiqTimeoutMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FTimeoutMs: Cardinal;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqTimeoutMiddleware;

    function TimeoutMs(AValue: Cardinal): TSidekiqTimeoutMiddleware;

    // ISidekiqServerMiddleware
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
  end;

implementation

{ TSidekiqTimeoutMiddleware }

constructor TSidekiqTimeoutMiddleware.Create;
begin
  inherited Create;
  FTimeoutMs := 5000;
end;

destructor TSidekiqTimeoutMiddleware.Destroy;
begin
  inherited;
end;

class function TSidekiqTimeoutMiddleware.New: TSidekiqTimeoutMiddleware;
begin
  Result := TSidekiqTimeoutMiddleware.Create;
end;

function TSidekiqTimeoutMiddleware.TimeoutMs(
  AValue: Cardinal): TSidekiqTimeoutMiddleware;
begin
  Result := Self;
  FTimeoutMs := AValue;
end;

procedure TSidekiqTimeoutMiddleware.Call(const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
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
    raise ESidekiqTimeoutException.CreateFmt(
      'Job %s timed out after %d ms', [AJob.Id, FTimeoutMs]);

  if Assigned(LException) then
    raise LException;
end;

end.
