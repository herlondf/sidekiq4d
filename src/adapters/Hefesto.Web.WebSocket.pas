unit Hefesto.Web.WebSocket;

{
  THefestoInMemoryWebSocketHub — Hub thread-safe para clientes WebSocket.

  Mantém uma lista de IOHandlers ativos (um por cliente conectado).
  O broadcast serializa cada frame WebSocket (RFC 6455, opcode 0x01 = text)
  e trata desconexões silenciosamente: clientes mortos são removidos da lista.

  Ciclo de vida esperado:
    1. O handler de upgrade HTTP chama RegisterClient(IOHandler).
    2. A thread de broadcast chama BroadcastMetrics periodicamente.
    3. Quando o cliente desconecta, a próxima tentativa de Write remove-o.
}

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  IdIOHandler,
  IdGlobal,
  Hefesto.Web;

type
  THefestoInMemoryWebSocketHub = class(TInterfacedObject, IHefestoWebSocketHub)
  private
    FLock   : TCriticalSection;
    FClients: TList<TIdIOHandler>;

    { Constrói frame WebSocket RFC 6455 para mensagem de texto (opcode 0x01). }
    class function BuildTextFrame(const AText: string): TBytes; static;

  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoWebSocketHub;

    { Registra um IOHandler recém-promovido a WebSocket (passado como TObject). }
    procedure RegisterClient(const AHandler: TObject);

    { IHefestoWebSocketHub }
    procedure BroadcastMetrics(const AJson: string);
    function ConnectedClients: Integer;
  end;

implementation

{ THefestoInMemoryWebSocketHub }

constructor THefestoInMemoryWebSocketHub.Create;
begin
  inherited Create;
  FLock    := TCriticalSection.Create;
  FClients := TList<TIdIOHandler>.Create;
end;

destructor THefestoInMemoryWebSocketHub.Destroy;
begin
  FClients.Free;
  FLock.Free;
  inherited;
end;

class function THefestoInMemoryWebSocketHub.New: IHefestoWebSocketHub;
begin
  Result := THefestoInMemoryWebSocketHub.Create;
end;

procedure THefestoInMemoryWebSocketHub.RegisterClient(
  const AHandler: TObject);
var
  LIO: TIdIOHandler;
begin
  if not (AHandler is TIdIOHandler) then
    Exit;
  LIO := TIdIOHandler(AHandler);
  FLock.Acquire;
  try
    if not FClients.Contains(LIO) then
      FClients.Add(LIO);
  finally
    FLock.Release;
  end;
end;

{
  Frame WebSocket não-mascarado (servidor → cliente), texto UTF-8.

  Byte 0:  0x81  (FIN=1, RSV=000, opcode=0001 text)
  Byte 1:  comprimento (sem máscara, bit7=0)
    < 126       → 1 byte payload length
    126..65535  → 0x7E + 2 bytes big-endian
    > 65535     → 0x7F + 8 bytes big-endian (não usado aqui)
  Payload: UTF-8 bytes do texto
}
class function THefestoInMemoryWebSocketHub.BuildTextFrame(
  const AText: string): TBytes;
var
  LPayload: TBytes;
  LLen    : Integer;
  LOffset : Integer;
begin
  LPayload := TEncoding.UTF8.GetBytes(AText);
  LLen     := Length(LPayload);

  if LLen < 126 then
  begin
    SetLength(Result, 2 + LLen);
    Result[0] := $81;
    Result[1] := Byte(LLen);
    LOffset   := 2;
  end
  else
  begin
    SetLength(Result, 4 + LLen);
    Result[0] := $81;
    Result[1] := $7E;
    Result[2] := Byte(LLen shr 8);
    Result[3] := Byte(LLen and $FF);
    LOffset   := 4;
  end;

  if LLen > 0 then
    Move(LPayload[0], Result[LOffset], LLen);
end;

procedure THefestoInMemoryWebSocketHub.BroadcastMetrics(const AJson: string);
var
  LFrame   : TBytes;
  LIdFrame : TIdBytes;
  LDead    : TList<TIdIOHandler>;
  LHandler : TIdIOHandler;
  LSnapshot: TArray<TIdIOHandler>;
  I        : Integer;
begin
  if AJson.IsEmpty then
    Exit;

  LFrame := BuildTextFrame(AJson);
  { Converte TBytes → TIdBytes (ambos são TArray<Byte> em Delphi 11+;
    a conversão explica a intenção e garante compatibilidade. }
  SetLength(LIdFrame, Length(LFrame));
  if Length(LFrame) > 0 then
    Move(LFrame[0], LIdFrame[0], Length(LFrame));

  LDead := TList<TIdIOHandler>.Create;
  try
    FLock.Acquire;
    try
      LSnapshot := FClients.ToArray;
    finally
      FLock.Release;
    end;

    for LHandler in LSnapshot do
    begin
      try
        if LHandler.Connected then
          LHandler.Write(LIdFrame, Length(LIdFrame))
        else
          LDead.Add(LHandler);
      except
        LDead.Add(LHandler);
      end;
    end;

    if LDead.Count > 0 then
    begin
      FLock.Acquire;
      try
        for I := 0 to LDead.Count - 1 do
          FClients.Remove(LDead[I]);
      finally
        FLock.Release;
      end;
    end;
  finally
    LDead.Free;
  end;
end;

function THefestoInMemoryWebSocketHub.ConnectedClients: Integer;
begin
  FLock.Acquire;
  try
    Result := FClients.Count;
  finally
    FLock.Release;
  end;
end;

end.
