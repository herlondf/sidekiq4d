unit Sidekiq4D.Outbox.Tests;

{
  Testes unitários para o contrato ISidekiqOutbox / ISidekiqOutboxPoller.
  Como a unit é de interfaces puras (sem implementação no core), os testes
  verificam o contrato via implementações mock inline.
}

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  Sidekiq4D.Outbox;

type
  [TestFixture('OutboxContract')]
  TOutboxContractTests = class
  public
    [Test]
    [Category('Unit')]
    procedure Save_Called_PassesCorrectParameters;

    [Test]
    [Category('Unit')]
    procedure Save_MultipleJobs_EachCallIsIndependent;

    [Test]
    [Category('Unit')]
    procedure PollOnce_NoEntries_ReturnsZero;

    [Test]
    [Category('Unit')]
    procedure PollOnce_WithEntries_ReturnsCount;

    [Test]
    [Category('Unit')]
    procedure PollOnce_CalledTwice_SecondCallReturnsZero;

    [Test]
    [Category('Unit')]
    procedure Start_CanBeCalledWithoutError;

    [Test]
    [Category('Unit')]
    procedure Stop_AfterStart_CanBeCalledWithoutError;
  end;

implementation

type
  { Mock in-memory ISidekiqOutbox para validação de contrato }
  TOutboxSaveRecord = record
    QueueName: string;
    Action: string;
    Payload: string;
  end;

  TInMemoryOutbox = class(TInterfacedObject, ISidekiqOutbox)
  private
    FSaved: TArray<TOutboxSaveRecord>;
  public
    procedure Save(
      const AQueueName: string;
      const AAction   : string;
      const APayload  : string;
      const AConnection: TObject);
    function SavedCount: Integer;
    function SavedAt(const AIndex: Integer): TOutboxSaveRecord;
    procedure Clear;
  end;

  { Mock ISidekiqOutboxPoller }
  TInMemoryOutboxPoller = class(TInterfacedObject, ISidekiqOutboxPoller)
  private
    FPending: Integer;
    FStarted: Boolean;
    FStopped: Boolean;
  public
    constructor Create(const APending: Integer);
    procedure Start;
    procedure Stop;
    function PollOnce: Integer;
    property Started: Boolean read FStarted;
    property Stopped: Boolean read FStopped;
  end;

{ TInMemoryOutbox }

procedure TInMemoryOutbox.Save(
  const AQueueName, AAction, APayload: string;
  const AConnection: TObject);
var
  LRec: TOutboxSaveRecord;
begin
  LRec.QueueName := AQueueName;
  LRec.Action    := AAction;
  LRec.Payload   := APayload;
  SetLength(FSaved, Length(FSaved) + 1);
  FSaved[High(FSaved)] := LRec;
end;

function TInMemoryOutbox.SavedCount: Integer;
begin
  Result := Length(FSaved);
end;

function TInMemoryOutbox.SavedAt(const AIndex: Integer): TOutboxSaveRecord;
begin
  Result := FSaved[AIndex];
end;

procedure TInMemoryOutbox.Clear;
begin
  SetLength(FSaved, 0);
end;

{ TInMemoryOutboxPoller }

constructor TInMemoryOutboxPoller.Create(const APending: Integer);
begin
  inherited Create;
  FPending := APending;
end;

procedure TInMemoryOutboxPoller.Start;
begin
  FStarted := True;
end;

procedure TInMemoryOutboxPoller.Stop;
begin
  FStopped := True;
end;

function TInMemoryOutboxPoller.PollOnce: Integer;
begin
  Result   := FPending;
  FPending := 0;
end;

{ TOutboxContractTests }

procedure TOutboxContractTests.Save_Called_PassesCorrectParameters;
var
  LOutbox: TInMemoryOutbox;
  LIntf: ISidekiqOutbox;
begin
  LOutbox := TInMemoryOutbox.Create;
  LIntf   := LOutbox;
  LIntf.Save('emails', 'SendEmail', '{"to":"a@b.com"}', nil);
  Assert.AreEqual(1, LOutbox.SavedCount);
  Assert.AreEqual('emails',          LOutbox.SavedAt(0).QueueName);
  Assert.AreEqual('SendEmail',       LOutbox.SavedAt(0).Action);
  Assert.AreEqual('{"to":"a@b.com"}', LOutbox.SavedAt(0).Payload);
end;

procedure TOutboxContractTests.Save_MultipleJobs_EachCallIsIndependent;
var
  LOutbox: TInMemoryOutbox;
  LIntf: ISidekiqOutbox;
begin
  LOutbox := TInMemoryOutbox.Create;
  LIntf   := LOutbox;
  LIntf.Save('q1', 'Act1', '{}', nil);
  LIntf.Save('q2', 'Act2', '{}', nil);
  Assert.AreEqual(2, LOutbox.SavedCount,
    'Cada chamada a Save deve ser registrada independentemente');
  Assert.AreEqual('Act1', LOutbox.SavedAt(0).Action);
  Assert.AreEqual('Act2', LOutbox.SavedAt(1).Action);
end;

procedure TOutboxContractTests.PollOnce_NoEntries_ReturnsZero;
var
  LPoller: ISidekiqOutboxPoller;
begin
  LPoller := TInMemoryOutboxPoller.Create(0);
  Assert.AreEqual(0, LPoller.PollOnce,
    'PollOnce sem entradas pendentes deve retornar 0');
end;

procedure TOutboxContractTests.PollOnce_WithEntries_ReturnsCount;
var
  LPoller: ISidekiqOutboxPoller;
begin
  LPoller := TInMemoryOutboxPoller.Create(5);
  Assert.AreEqual(5, LPoller.PollOnce,
    'PollOnce deve retornar o número de jobs publicados');
end;

procedure TOutboxContractTests.PollOnce_CalledTwice_SecondCallReturnsZero;
var
  LPoller: ISidekiqOutboxPoller;
begin
  LPoller := TInMemoryOutboxPoller.Create(3);
  LPoller.PollOnce; // consome as 3 entradas
  Assert.AreEqual(0, LPoller.PollOnce,
    'Segunda chamada deve retornar 0 (sem novas entradas)');
end;

procedure TOutboxContractTests.Start_CanBeCalledWithoutError;
var
  LPoller: TInMemoryOutboxPoller;
  LIntf: ISidekiqOutboxPoller;
begin
  LPoller := TInMemoryOutboxPoller.Create(0);
  LIntf   := LPoller;
  LIntf.Start;
  Assert.IsTrue(LPoller.Started, 'Start deve ser chamado sem erros');
end;

procedure TOutboxContractTests.Stop_AfterStart_CanBeCalledWithoutError;
var
  LPoller: TInMemoryOutboxPoller;
  LIntf: ISidekiqOutboxPoller;
begin
  LPoller := TInMemoryOutboxPoller.Create(0);
  LIntf   := LPoller;
  LIntf.Start;
  LIntf.Stop;
  Assert.IsTrue(LPoller.Stopped, 'Stop deve ser chamado sem erros após Start');
end;

initialization
  TDUnitX.RegisterTestFixture(TOutboxContractTests);

end.
