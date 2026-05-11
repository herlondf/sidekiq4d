unit Sidekiq4D.Queue.TCP.Indy;

// Indy (TIdTCPServer) implementation of ISidekiqTCPServer.
// THIS IS THE ONLY FILE IN SIDEKIQ4D THAT IMPORTS Indy TCP types.
//
// Uso:
//   TSidekiqTCPAdapter.New
//     .UseTCPServer(TSidekiqIndyTCPServer.New)
//     ...;

interface

uses
  System.SysUtils,
  IdContext,
  IdTCPServer,
  Sidekiq4D.Queue.TCP;

type
  TSidekiqIndyTCPServer = class(TInterfacedObject, ISidekiqTCPServer)
  private
    FServer: TIdTCPServer;
    // Set via SetOnLine() before FServer.Active := True. Thread-safe after init.
    FOnLine: TSidekiqTCPOnLine;

    procedure OnConnect(AContext: TIdContext);
    procedure OnDisconnect(AContext: TIdContext);
    procedure OnExecute(AContext: TIdContext);
  public
    constructor Create;
    destructor Destroy; override;

    class function New: ISidekiqTCPServer; static;

    procedure SetOnLine(const ACallback: TSidekiqTCPOnLine);
    procedure Start(const AHost: string; APort: Word; AMaxConnections: Integer);
    procedure Stop;
  end;

implementation

constructor TSidekiqIndyTCPServer.Create;
begin
  inherited Create;
  FServer := TIdTCPServer.Create(nil);
  FServer.OnConnect    := OnConnect;
  FServer.OnDisconnect := OnDisconnect;
  FServer.OnExecute    := OnExecute;
end;

destructor TSidekiqIndyTCPServer.Destroy;
begin
  if FServer.Active then
    FServer.Active := False;
  FServer.Free;
  inherited;
end;

class function TSidekiqIndyTCPServer.New: ISidekiqTCPServer;
begin
  Result := TSidekiqIndyTCPServer.Create;
end;

procedure TSidekiqIndyTCPServer.SetOnLine(const ACallback: TSidekiqTCPOnLine);
begin
  FOnLine := ACallback;
end;

procedure TSidekiqIndyTCPServer.Start(
  const AHost: string; APort: Word; AMaxConnections: Integer);
begin
  FServer.Bindings.Clear;
  with FServer.Bindings.Add do
  begin
    IP   := AHost;
    Port := APort;
  end;
  FServer.MaxConnections := AMaxConnections;
  FServer.Active := True;
end;

procedure TSidekiqIndyTCPServer.Stop;
begin
  FServer.Active := False;
end;

procedure TSidekiqIndyTCPServer.OnConnect(AContext: TIdContext);
begin
  // Limit line size to 64KB to prevent OOM from unbounded JSONL lines
  AContext.Connection.IOHandler.MaxLineLength := 65536;
end;

procedure TSidekiqIndyTCPServer.OnDisconnect(AContext: TIdContext);
begin
end;

procedure TSidekiqIndyTCPServer.OnExecute(AContext: TIdContext);
var
  LLine, LResponse: string;
begin
  try
    LLine := AContext.Connection.IOHandler.ReadLn(#10, 5000);
  except
    // Line too long or read error — drop connection
    AContext.Connection.IOHandler.WriteLn('{"status":"error","message":"line too long or read error"}');
    AContext.Connection.Disconnect;
    Exit;
  end;
  if LLine.IsEmpty then
    Exit;
  if Assigned(FOnLine) then
  begin
    LResponse := FOnLine(LLine);
    AContext.Connection.IOHandler.WriteLn(LResponse);
  end;
end;

end.
