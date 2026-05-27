unit Hefesto.WebSocket.Tests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  Hefesto.Web,
  Hefesto.Web.WebSocket;

type
  [TestFixture('WebSocket')]
  TWebSocketHubTests = class
  private
    FHub: IHefestoWebSocketHub;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    [Category('Unit')]
    procedure New_Returns_ValidInterface;

    [Test]
    [Category('Unit')]
    procedure ConnectedClients_InitialState_ReturnsZero;

    [Test]
    [Category('Unit')]
    procedure BroadcastMetrics_EmptyHub_DoesNotRaise;

    [Test]
    [Category('Unit')]
    procedure BroadcastMetrics_EmptyString_DoesNotRaise;

    [Test]
    [Category('Unit')]
    procedure BroadcastMetrics_LargePayload_DoesNotRaise;

    [Test]
    [Category('Unit')]
    procedure RegisterClient_Nil_IsIgnoredGracefully;

    [Test]
    [Category('Unit')]
    procedure RegisterClient_NonIOHandler_IsIgnoredGracefully;

    [Test]
    [Category('Unit')]
    procedure RegisterClient_NonIOHandler_ClientCountRemainsZero;
  end;

implementation

{ TWebSocketHubTests }

procedure TWebSocketHubTests.Setup;
begin
  FHub := THefestoInMemoryWebSocketHub.New;
end;

procedure TWebSocketHubTests.TearDown;
begin
  FHub := nil;
end;

procedure TWebSocketHubTests.New_Returns_ValidInterface;
begin
  Assert.IsNotNull(FHub, 'New deve retornar instância não-nula');
end;

procedure TWebSocketHubTests.ConnectedClients_InitialState_ReturnsZero;
begin
  Assert.AreEqual(0, FHub.ConnectedClients, 'Hub recém criado deve ter 0 clientes');
end;

procedure TWebSocketHubTests.BroadcastMetrics_EmptyHub_DoesNotRaise;
begin
  Assert.WillNotRaise(
    procedure begin
      FHub.BroadcastMetrics('{"workers":0,"queued":0}');
    end,
    Exception,
    'BroadcastMetrics com hub vazio não deve lançar exceção');
end;

procedure TWebSocketHubTests.BroadcastMetrics_EmptyString_DoesNotRaise;
begin
  Assert.WillNotRaise(
    procedure begin
      FHub.BroadcastMetrics('');
    end,
    Exception,
    'BroadcastMetrics com string vazia não deve lançar exceção');
end;

procedure TWebSocketHubTests.BroadcastMetrics_LargePayload_DoesNotRaise;
var
  LBigJson: string;
begin
  { Gera payload > 125 bytes para exercitar o branch de extended payload length (0x7E + 2 bytes) }
  LBigJson := '{"data":"' + StringOfChar('x', 200) + '"}';

  Assert.WillNotRaise(
    procedure begin
      FHub.BroadcastMetrics(LBigJson);
    end,
    Exception,
    'BroadcastMetrics com payload > 125 bytes não deve lançar exceção');
end;

procedure TWebSocketHubTests.RegisterClient_Nil_IsIgnoredGracefully;
begin
  Assert.WillNotRaise(
    procedure begin
      FHub.RegisterClient(nil);
    end,
    Exception,
    'RegisterClient(nil) não deve lançar exceção');

  Assert.AreEqual(0, FHub.ConnectedClients, 'nil não deve ser adicionado à lista');
end;

procedure TWebSocketHubTests.RegisterClient_NonIOHandler_IsIgnoredGracefully;
var
  LDummy: TObject;
begin
  LDummy := TObject.Create;
  try
    Assert.WillNotRaise(
      procedure begin
        FHub.RegisterClient(LDummy);
      end,
      Exception,
      'RegisterClient com TObject não-IOHandler não deve lançar exceção');
  finally
    LDummy.Free;
  end;
end;

procedure TWebSocketHubTests.RegisterClient_NonIOHandler_ClientCountRemainsZero;
var
  LDummy: TObject;
begin
  LDummy := TObject.Create;
  try
    FHub.RegisterClient(LDummy);
    Assert.AreEqual(0, FHub.ConnectedClients,
      'TObject não-IOHandler não deve ser contado como cliente conectado');
  finally
    LDummy.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TWebSocketHubTests);

end.
