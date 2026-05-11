unit Sidekiq4D.Middleware;

interface

uses
  System.SysUtils,
  System.Classes,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces;

type
  TSidekiqNextProc = reference to procedure;

  ISidekiqClientMiddleware = interface
    ['{91C1A1A3-02D0-4E7A-B77F-0A3D1E2B0F11}']
    procedure Call(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const ANext: TSidekiqNextProc);
  end;

  ISidekiqServerMiddleware = interface
    ['{91C1A1A3-02D0-4E7A-B77F-0A3D1E2B0F12}']
    procedure Call(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ANext: TSidekiqNextProc);
  end;

  TSidekiqClientMiddlewareChain = class
  private
    FChain: TArray<ISidekiqClientMiddleware>;
  public
    constructor Create(const AChain: TArray<ISidekiqClientMiddleware>);

    procedure Invoke(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const AFinalAction: TSidekiqNextProc);
  end;

  TSidekiqServerMiddlewareChain = class
  private
    FChain: TArray<ISidekiqServerMiddleware>;
  public
    constructor Create(const AChain: TArray<ISidekiqServerMiddleware>);

    procedure Invoke(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AFinalAction: TSidekiqNextProc);
  end;

implementation

{ TSidekiqClientMiddlewareChain }

constructor TSidekiqClientMiddlewareChain.Create(
  const AChain: TArray<ISidekiqClientMiddleware>);
begin
  inherited Create;
  FChain := AChain;
end;

procedure TSidekiqClientMiddlewareChain.Invoke(
  const AQueueName: string;
  const AAction: string;
  const ABody: string;
  const AAttributes: TStrings;
  const AFinalAction: TSidekiqNextProc);
var
  LIndex: Integer;
  LStep: TSidekiqNextProc;
begin
  LStep := AFinalAction;
  for LIndex := High(FChain) downto Low(FChain) do
  begin
    LStep :=
      (function(AMiddleware: ISidekiqClientMiddleware;
                ANext: TSidekiqNextProc): TSidekiqNextProc
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

{ TSidekiqServerMiddlewareChain }

constructor TSidekiqServerMiddlewareChain.Create(
  const AChain: TArray<ISidekiqServerMiddleware>);
begin
  inherited Create;
  FChain := AChain;
end;

procedure TSidekiqServerMiddlewareChain.Invoke(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AFinalAction: TSidekiqNextProc);
var
  LIndex: Integer;
  LStep: TSidekiqNextProc;
begin
  LStep := AFinalAction;
  for LIndex := High(FChain) downto Low(FChain) do
  begin
    LStep :=
      (function(AMiddleware: ISidekiqServerMiddleware;
                ANext: TSidekiqNextProc): TSidekiqNextProc
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
