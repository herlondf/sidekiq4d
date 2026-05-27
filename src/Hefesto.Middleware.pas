unit Hefesto.Middleware;

interface

uses
  System.SysUtils,
  System.Classes,
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  THefestoNextProc = reference to procedure;

  IHefestoClientMiddleware = interface
    ['{91C1A1A3-02D0-4E7A-B77F-0A3D1E2B0F11}']
    procedure Call(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const ANext: THefestoNextProc);
  end;

  IHefestoServerMiddleware = interface
    ['{91C1A1A3-02D0-4E7A-B77F-0A3D1E2B0F12}']
    procedure Call(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ANext: THefestoNextProc);
  end;

  THefestoClientMiddlewareChain = class
  private
    FChain: TArray<IHefestoClientMiddleware>;
  public
    constructor Create(const AChain: TArray<IHefestoClientMiddleware>);

    procedure Invoke(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const AFinalAction: THefestoNextProc);
  end;

  THefestoServerMiddlewareChain = class
  private
    FChain: TArray<IHefestoServerMiddleware>;
  public
    constructor Create(const AChain: TArray<IHefestoServerMiddleware>);

    procedure Invoke(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AFinalAction: THefestoNextProc);
  end;

implementation

{ THefestoClientMiddlewareChain }

constructor THefestoClientMiddlewareChain.Create(
  const AChain: TArray<IHefestoClientMiddleware>);
begin
  inherited Create;
  FChain := AChain;
end;

procedure THefestoClientMiddlewareChain.Invoke(
  const AQueueName: string;
  const AAction: string;
  const ABody: string;
  const AAttributes: TStrings;
  const AFinalAction: THefestoNextProc);
var
  LIndex: Integer;
  LStep: THefestoNextProc;
begin
  LStep := AFinalAction;
  for LIndex := High(FChain) downto Low(FChain) do
  begin
    LStep :=
      (function(AMiddleware: IHefestoClientMiddleware;
                ANext: THefestoNextProc): THefestoNextProc
       begin
         Result :=
           procedure
           begin
             AMiddleware.Call(AQueueName, AAction, ABody, AAttributes, ANext);
           end;
       end)(FChain[LIndex], LStep);
  end;
  LStep();
end;

{ THefestoServerMiddlewareChain }

constructor THefestoServerMiddlewareChain.Create(
  const AChain: TArray<IHefestoServerMiddleware>);
begin
  inherited Create;
  FChain := AChain;
end;

procedure THefestoServerMiddlewareChain.Invoke(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AFinalAction: THefestoNextProc);
var
  LIndex: Integer;
  LStep: THefestoNextProc;
begin
  LStep := AFinalAction;
  for LIndex := High(FChain) downto Low(FChain) do
  begin
    LStep :=
      (function(AMiddleware: IHefestoServerMiddleware;
                ANext: THefestoNextProc): THefestoNextProc
       begin
         Result :=
           procedure
           begin
             AMiddleware.Call(AQueue, AJob, ANext);
           end;
       end)(FChain[LIndex], LStep);
  end;
  LStep();
end;

end.
