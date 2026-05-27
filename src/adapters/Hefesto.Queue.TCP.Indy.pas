unit Hefesto.Queue.TCP.Indy;

// Indy (TIdTCPServer) implementation of IHefestoTCPServer.
// THIS IS THE ONLY FILE IN SIDEKIQ4D THAT IMPORTS Indy TCP types.
//
// Uso:
//   THefestoTCPAdapter.New
//     .UseTCPServer(THefestoIndyTCPServer.New)
//     ...;

interface

uses
  System.SysUtils,
  IdContext,
  IdTCPServer,
  Hefesto.Queue.TCP;

type
  THefestoIndyTCPServer = class(TInterfacedObject, IHefestoTCPServer)
  private
    FServer: TIdTCPServer;
    // Set via SetOnLine() before FServer.Active := True. Thread-safe after init.
    FOnLine: THefestoTCPOnLine;

    procedure OnConnect(AContext: TIdContext);
    procedure OnDisconnect(AContext: TIdContext);
    procedure OnExecute(AContext: TIdContext);
  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoTCPServer; static;

    procedure SetOnLine(const ACallback: THefestoTCPOnLine);
    procedure Start(const AHost: string; APort: Word; AMaxConnections: Integer);
    procedure Stop;
  end;

implementation

constructor THefestoIndyTCPServer.Create;
begin
  inherited Create;
  FServer := TIdTCPServer.Create(nil);
  FServer.OnConnect    := OnConnect;
  FServer.OnDisconnect := OnDisconnect;
  FServer.OnExecute    := OnExecute;
end;

destructor THefestoIndyTCPServer.Destroy;
begin
  if FServer.Active then
    FServer.Active := False;
  FServer.Free;
  inherited;
end;

class function THefestoIndyTCPServer.New: IHefestoTCPServer;
begin
  Result := THefestoIndyTCPServer.Create;
end;

procedure THefestoIndyTCPServer.SetOnLine(const ACallback: THefestoTCPOnLine);
begin
  FOnLine := ACallback;
end;

procedure THefestoIndyTCPServer.Start(
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

procedure THefestoIndyTCPServer.Stop;
begin
  FServer.Active := False;
end;

procedure THefestoIndyTCPServer.OnConnect(AContext: TIdContext);
begin
  // Limit line size to 64KB to prevent OOM from unbounded JSONL lines
  AContext.Connection.IOHandler.MaxLineLength := 65536;
end;

procedure THefestoIndyTCPServer.OnDisconnect(AContext: TIdContext);
begin
end;

procedure THefestoIndyTCPServer.OnExecute(AContext: TIdContext);
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
