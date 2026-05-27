unit Hefesto.Idempotency.Tests;

{
  Testes unitários para THefestoStateStoreIdempotency.
  Cobre: TryBegin, MarkCompleted, IsCompleted, Clear e Renew.
}

interface

uses
  DUnitX.TestFramework,
  Hefesto.Store.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Idempotency;

type
  [TestFixture('Idempotency')]
  TIdempotencyTests = class
  private
    FStore: IHefestoStateStore;
    FIdempotency: IHefestoIdempotency;
  public
    [Setup]
    procedure Setup;

    [Test]
    [Category('Unit')]
    procedure TryBegin_FreshKey_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure TryBegin_SameKeyTwice_SecondReturnsFalse;

    [Test]
    [Category('Unit')]
    procedure TryBegin_CompletedKey_ReturnsFalse;

    [Test]
    [Category('Unit')]
    procedure MarkCompleted_SetsIsCompletedTrue;

    [Test]
    [Category('Unit')]
    procedure IsCompleted_BeforeMarkCompleted_ReturnsFalse;

    [Test]
    [Category('Unit')]
    procedure Clear_AfterTryBegin_AllowsNewTryBegin;

    [Test]
    [Category('Unit')]
    procedure Clear_AfterMarkCompleted_AllowsNewTryBegin;

    [Test]
    [Category('Unit')]
    procedure TryBegin_EmptyKey_ReturnsFalse;
  end;

  [TestFixture('RenewableIdempotency')]
  TRenewableIdempotencyTests = class
  private
    FStore: IHefestoStateStore;
    FIdempotency: IHefestoRenewableIdempotency;
  public
    [Setup]
    procedure Setup;

    [Test]
    [Category('Unit')]
    procedure Renew_ProcessingKey_DoesNotRaise;

    [Test]
    [Category('Unit')]
    procedure Renew_CompletedKey_DoesNotChangeToProcessing;
  end;

implementation

uses
  System.SysUtils;

{ TIdempotencyTests }

procedure TIdempotencyTests.Setup;
begin
  FStore := THefestoInMemoryStateStore.New;
  FIdempotency := THefestoStateStoreIdempotency.New(FStore);
end;

procedure TIdempotencyTests.TryBegin_FreshKey_ReturnsTrue;
begin
  Assert.IsTrue(FIdempotency.TryBegin('key-a'),
    'Primeira chamada com chave nova deve retornar True');
end;

procedure TIdempotencyTests.TryBegin_SameKeyTwice_SecondReturnsFalse;
begin
  FIdempotency.TryBegin('key-b');
  Assert.IsFalse(FIdempotency.TryBegin('key-b'),
    'Segunda chamada com chave em "processing" deve retornar False');
end;

procedure TIdempotencyTests.TryBegin_CompletedKey_ReturnsFalse;
begin
  FIdempotency.TryBegin('key-c');
  FIdempotency.MarkCompleted('key-c');
  Assert.IsFalse(FIdempotency.TryBegin('key-c'),
    'Job já completado não deve permitir novo TryBegin');
end;

procedure TIdempotencyTests.MarkCompleted_SetsIsCompletedTrue;
begin
  FIdempotency.TryBegin('key-d');
  FIdempotency.MarkCompleted('key-d');
  Assert.IsTrue(FIdempotency.IsCompleted('key-d'),
    'IsCompleted deve retornar True após MarkCompleted');
end;

procedure TIdempotencyTests.IsCompleted_BeforeMarkCompleted_ReturnsFalse;
begin
  FIdempotency.TryBegin('key-e');
  Assert.IsFalse(FIdempotency.IsCompleted('key-e'),
    'IsCompleted deve retornar False enquanto o job está em "processing"');
end;

procedure TIdempotencyTests.Clear_AfterTryBegin_AllowsNewTryBegin;
begin
  FIdempotency.TryBegin('key-f');
  FIdempotency.Clear('key-f');
  Assert.IsTrue(FIdempotency.TryBegin('key-f'),
    'Após Clear de "processing", a mesma chave deve ser aceita novamente');
end;

procedure TIdempotencyTests.Clear_AfterMarkCompleted_AllowsNewTryBegin;
begin
  FIdempotency.TryBegin('key-g');
  FIdempotency.MarkCompleted('key-g');
  FIdempotency.Clear('key-g');
  Assert.IsTrue(FIdempotency.TryBegin('key-g'),
    'Após Clear de "completed", a mesma chave deve ser aceita novamente');
end;

procedure TIdempotencyTests.TryBegin_EmptyKey_ReturnsFalse;
begin
  Assert.IsFalse(FIdempotency.TryBegin(''),
    'Chave vazia deve retornar False');
end;

{ TRenewableIdempotencyTests }

procedure TRenewableIdempotencyTests.Setup;
var
  LStore: IHefestoStateStore;
begin
  LStore := THefestoInMemoryStateStore.New;
  FStore := LStore;
  FIdempotency := THefestoStateStoreIdempotency.Create(LStore) as IHefestoRenewableIdempotency;
end;

procedure TRenewableIdempotencyTests.Renew_ProcessingKey_DoesNotRaise;
begin
  (FIdempotency as IHefestoIdempotency).TryBegin('key-renew', 300);
  Assert.WillNotRaise(
    procedure begin FIdempotency.Renew('key-renew', 300); end,
    nil,
    'Renew de chave em "processing" não deve levantar exceção');
end;

procedure TRenewableIdempotencyTests.Renew_CompletedKey_DoesNotChangeToProcessing;
begin
  (FIdempotency as IHefestoIdempotency).TryBegin('key-done');
  (FIdempotency as IHefestoIdempotency).MarkCompleted('key-done');
  FIdempotency.Renew('key-done', 300);
  Assert.IsTrue((FIdempotency as IHefestoIdempotency).IsCompleted('key-done'),
    'Renew não deve alterar chave "completed" para "processing"');
end;

initialization
  TDUnitX.RegisterTestFixture(TIdempotencyTests);
  TDUnitX.RegisterTestFixture(TRenewableIdempotencyTests);

end.
