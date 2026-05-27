unit Hefesto.Dispatcher;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  System.Generics.Collections,
  Hefesto.Job,
  Hefesto.Handler;

type
  EHefestoNoHandler = class(Exception);

  IHefestoDispatcher = interface
    ['{B7734F4B-A585-4534-AE2E-E1A611B4B4AF}']
    function RegisterHandler(
      const AAction: string;
      const AHandler: IHefestoJobHandler): IHefestoDispatcher;
    function Resolve(const AJob: IHefestoJobEnvelope): IHefestoJobHandler;
  end;

  THefestoDispatcher = class(TInterfacedObject, IHefestoDispatcher)
  private
    FHandlers: TDictionary<string, IHefestoJobHandler>;
    FLock: TMultiReadExclusiveWriteSynchronizer;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoDispatcher;

    function RegisterHandler(
      const AAction: string;
      const AHandler: IHefestoJobHandler): IHefestoDispatcher;
    function Resolve(const AJob: IHefestoJobEnvelope): IHefestoJobHandler;
  end;

implementation

{ THefestoDispatcher }

constructor THefestoDispatcher.Create;
begin
  inherited Create;
  FHandlers := TDictionary<string, IHefestoJobHandler>.Create;
  FLock := TMultiReadExclusiveWriteSynchronizer.Create;
end;

destructor THefestoDispatcher.Destroy;
begin
  FLock.Free;
  FHandlers.Free;
  inherited;
end;

class function THefestoDispatcher.New: IHefestoDispatcher;
begin
  Result := THefestoDispatcher.Create;
end;

function THefestoDispatcher.RegisterHandler(
  const AAction: string;
  const AHandler: IHefestoJobHandler): IHefestoDispatcher;
begin
  Result := Self;
  FLock.BeginWrite;
  try
    FHandlers.AddOrSetValue(AAction, AHandler);
  finally
    FLock.EndWrite;
  end;
end;

function THefestoDispatcher.Resolve(
  const AJob: IHefestoJobEnvelope): IHefestoJobHandler;
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

  raise EHefestoNoHandler.CreateFmt(
    'No handler registered for action "%s".',
    [AJob.Action]);
end;

end.
