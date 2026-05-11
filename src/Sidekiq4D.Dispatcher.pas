unit Sidekiq4D.Dispatcher;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Sidekiq4D.Job,
  Sidekiq4D.Handler;

type
  ESidekiqNoHandler = class(Exception);

  ISidekiqDispatcher = interface
    ['{B7734F4B-A585-4534-AE2E-E1A611B4B4AF}']
    function RegisterHandler(
      const AAction: string;
      const AHandler: ISidekiqJobHandler): ISidekiqDispatcher;
    function Resolve(const AJob: ISidekiqJobEnvelope): ISidekiqJobHandler;
  end;

  TSidekiqDispatcher = class(TInterfacedObject, ISidekiqDispatcher)
  private
    FHandlers: TDictionary<string, ISidekiqJobHandler>;
    FLock: TMultiReadExclusiveWriteSynchronizer;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: ISidekiqDispatcher;

    function RegisterHandler(
      const AAction: string;
      const AHandler: ISidekiqJobHandler): ISidekiqDispatcher;
    function Resolve(const AJob: ISidekiqJobEnvelope): ISidekiqJobHandler;
  end;

implementation

{ TSidekiqDispatcher }

constructor TSidekiqDispatcher.Create;
begin
  inherited Create;
  FHandlers := TDictionary<string, ISidekiqJobHandler>.Create;
  FLock := TMultiReadExclusiveWriteSynchronizer.Create;
end;

destructor TSidekiqDispatcher.Destroy;
begin
  FLock.Free;
  FHandlers.Free;
  inherited;
end;

class function TSidekiqDispatcher.New: ISidekiqDispatcher;
begin
  Result := TSidekiqDispatcher.Create;
end;

function TSidekiqDispatcher.RegisterHandler(
  const AAction: string;
  const AHandler: ISidekiqJobHandler): ISidekiqDispatcher;
begin
  Result := Self;
  FLock.BeginWrite;
  try
    FHandlers.AddOrSetValue(AAction, AHandler);
  finally
    FLock.EndWrite;
  end;
end;

function TSidekiqDispatcher.Resolve(
  const AJob: ISidekiqJobEnvelope): ISidekiqJobHandler;
begin
  FLock.BeginRead;
  try
    if FHandlers.TryGetValue(AJob.Action, Result) then
      Exit;

    if FHandlers.TryGetValue('*', Result) then
      Exit;
  finally
    FLock.EndRead;
  end;

  raise ESidekiqNoHandler.CreateFmt(
    'No handler registered for action "%s".',
    [AJob.Action]);
end;

end.
