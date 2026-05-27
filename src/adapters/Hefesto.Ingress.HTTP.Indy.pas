unit Hefesto.Ingress.HTTP.Indy;

// Indy (TIdHTTPServer) implementation of IHefestoHTTPServer.
// THIS IS THE ONLY FILE IN SIDEKIQ4D THAT IMPORTS Indy HTTP types.
//
// Uso:
//   THefestoHTTPIngressAdapter.New
//     .UseHTTPServer(THefestoIndyHTTPServer.New)
//     ...;

interface

uses
  System.SysUtils,
  System.Classes,
  IdContext,
  IdCustomHTTPServer,
  IdHTTPServer,
  Hefesto.Ingress.HTTP;

type
  THefestoIndyHTTPServer = class(TInterfacedObject, IHefestoHTTPServer)
  private
    FServer: TIdHTTPServer;
    // Set via SetOnRequest() before FServer.Active := True. Thread-safe after init.
    FOnRequest: THefestoHTTPOnRequest;

    procedure HandleCommand(
      AContext: TIdContext;
      ARequestInfo: TIdHTTPRequestInfo;
      AResponseInfo: TIdHTTPResponseInfo);
  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoHTTPServer; static;

    procedure SetOnRequest(const ACallback: THefestoHTTPOnRequest);
    procedure Start(const AHost: string; APort: Word);
    procedure Stop;
  end;

implementation

constructor THefestoIndyHTTPServer.Create;
begin
  inherited Create;
  FServer := TIdHTTPServer.Create(nil);
  FServer.OnCommandGet   := HandleCommand;
  FServer.OnCommandOther := HandleCommand;
end;

destructor THefestoIndyHTTPServer.Destroy;
begin
  if FServer.Active then
    FServer.Active := False;
  FServer.Free;
  inherited;
end;

class function THefestoIndyHTTPServer.New: IHefestoHTTPServer;
begin
  Result := THefestoIndyHTTPServer.Create;
end;

procedure THefestoIndyHTTPServer.SetOnRequest(
  const ACallback: THefestoHTTPOnRequest);
begin
  FOnRequest := ACallback;
end;

procedure THefestoIndyHTTPServer.Start(const AHost: string; APort: Word);
begin
  FServer.Bindings.Clear;
  with FServer.Bindings.Add do
  begin
    IP := AHost;
    Port := APort;
  end;
  FServer.Active := True;
end;

procedure THefestoIndyHTTPServer.Stop;
begin
  FServer.Active := False;
end;

procedure THefestoIndyHTTPServer.HandleCommand(
  AContext: TIdContext;
  ARequestInfo: TIdHTTPRequestInfo;
  AResponseInfo: TIdHTTPResponseInfo);
var
  LContext: THefestoHTTPContext;
  LBody: string;
  LStatus: Integer;
  LResponseBody: string;
begin
  if not Assigned(FOnRequest) then
  begin
    AResponseInfo.ResponseNo  := 503;
    AResponseInfo.ContentText := '{"error":"server not ready"}';
    Exit;
  end;

  LContext.Method     := ARequestInfo.Command;
  LContext.Path       := ARequestInfo.Document;
  LContext.AuthHeader := ARequestInfo.RawHeaders.Values['Authorization'];

  LBody := ARequestInfo.UnparsedParams;
  if LBody.IsEmpty and Assigned(ARequestInfo.PostStream) then
  begin
    var LBytes: TBytes;
    SetLength(LBytes, ARequestInfo.PostStream.Size);
    ARequestInfo.PostStream.Position := 0;
    ARequestInfo.PostStream.Read(LBytes[0], Length(LBytes));
    LBody := TEncoding.UTF8.GetString(LBytes);
  end;
  LContext.Body := LBody;

  LStatus       := 500;
  LResponseBody := '';
  FOnRequest(LContext, LStatus, LResponseBody);

  AResponseInfo.ResponseNo  := LStatus;
  AResponseInfo.ContentType := 'application/json';
  AResponseInfo.ContentText := LResponseBody;
end;

end.
