unit Hefesto.DeadLetter.Tests;

{
  Testes unitários para THefestoStateStoreDeadLetterQueue.
  Cobre: Push, Pop, List, Delete, Retry, Count.
  Usa THefestoInMemoryStateStore — sem broker externo.
}

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.DateUtils,
  Hefesto.DeadLetter,
  Hefesto.DeadLetter.Store,
  Hefesto.Store.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Queue.Interfaces;

type
  [TestFixture('DeadLetterQueue')]
  TDeadLetterQueueTests = class
  private
    FDLQ          : IHefestoDeadLetterQueue;
    FRetryRequests: TArray<THefestoPublishRequest>;

    function MakeEntry(
      const AId, AAction, AQueue: string;
      ARetries: Integer = 3): THefestoDeadLetterEntry;
  public
    [Setup]
    procedure Setup;

    [Test][Category('Unit')]
    procedure Push_NewEntry_IncreasesCount;

    [Test][Category('Unit')]
    procedure Push_EmptyJobId_IsIgnored;

    [Test][Category('Unit')]
    procedure Push_MultipleEntries_CountIsCorrect;

    [Test][Category('Unit')]
    procedure List_Empty_ReturnsEmptyArray;

    [Test][Category('Unit')]
    procedure List_AfterPush_ReturnsAllEntries;

    [Test][Category('Unit')]
    procedure List_DoesNotRemoveEntries;

    [Test][Category('Unit')]
    procedure Pop_ExistingJob_ReturnsCorrectFields;

    [Test][Category('Unit')]
    procedure Pop_ExistingJob_RemovesFromDLQ;

    [Test][Category('Unit')]
    procedure Delete_ExistingJob_RemovesEntry;

    [Test][Category('Unit')]
    procedure Delete_NonExistentJob_NoError;

    [Test][Category('Unit')]
    procedure Retry_ExistingJob_InvokesPublishCallback;

    [Test][Category('Unit')]
    procedure Retry_ExistingJob_CallbackReceivesCorrectQueueAndAction;

    [Test][Category('Unit')]
    procedure Retry_ExistingJob_RemovesFromDLQ;

    [Test][Category('Unit')]
    procedure Retry_NonExistentJob_NoCallback;

    [Test][Category('Unit')]
    procedure Count_ReflectsStateAfterPushAndDelete;
  end;

implementation

function TDeadLetterQueueTests.MakeEntry(
  const AId, AAction, AQueue: string;
  ARetries: Integer): THefestoDeadLetterEntry;
begin
  Result := Default(THefestoDeadLetterEntry);
  Result.JobId         := AId;
  Result.JobJson       := '{"id":"' + AId + '"}';
  Result.Action        := AAction;
  Result.OriginalQueue := AQueue;
  Result.ErrorMessage  := 'boom';
  Result.RetryCount    := ARetries;
  Result.FailedAt      := Now;
end;

procedure TDeadLetterQueueTests.Setup;
var
  LStore: IHefestoStateStore;
begin
  FRetryRequests := [];
  LStore := THefestoInMemoryStateStore.New;
  FDLQ := THefestoStateStoreDeadLetterQueue.New(
    LStore,
    procedure(const AReq: THefestoPublishRequest)
    begin
      SetLength(FRetryRequests, Length(FRetryRequests) + 1);
      FRetryRequests[High(FRetryRequests)] := AReq;
    end);
end;

procedure TDeadLetterQueueTests.Push_NewEntry_IncreasesCount;
begin
  FDLQ.Push(MakeEntry('job-1', 'SendEmail', 'emails'));
  Assert.AreEqual(1, FDLQ.Count);
end;

procedure TDeadLetterQueueTests.Push_EmptyJobId_IsIgnored;
begin
  var LEntry := Default(THefestoDeadLetterEntry);
  LEntry.JobId := '';
  FDLQ.Push(LEntry);
  Assert.AreEqual(0, FDLQ.Count, 'JobId vazio deve ser ignorado');
end;

procedure TDeadLetterQueueTests.Push_MultipleEntries_CountIsCorrect;
begin
  FDLQ.Push(MakeEntry('j1', 'A', 'q1'));
  FDLQ.Push(MakeEntry('j2', 'B', 'q2'));
  FDLQ.Push(MakeEntry('j3', 'C', 'q3'));
  Assert.AreEqual(3, FDLQ.Count);
end;

procedure TDeadLetterQueueTests.List_Empty_ReturnsEmptyArray;
begin
  Assert.AreEqual(0, Length(FDLQ.List), 'DLQ vazia deve retornar array vazio');
end;

procedure TDeadLetterQueueTests.List_AfterPush_ReturnsAllEntries;
begin
  FDLQ.Push(MakeEntry('j1', 'A', 'q1'));
  FDLQ.Push(MakeEntry('j2', 'B', 'q2'));
  Assert.AreEqual(2, Length(FDLQ.List));
end;

procedure TDeadLetterQueueTests.List_DoesNotRemoveEntries;
begin
  FDLQ.Push(MakeEntry('j1', 'A', 'q1'));
  FDLQ.List;
  FDLQ.List;
  Assert.AreEqual(1, FDLQ.Count, 'List nao deve remover entradas');
end;

procedure TDeadLetterQueueTests.Pop_ExistingJob_ReturnsCorrectFields;
var
  LEntry: THefestoDeadLetterEntry;
begin
  FDLQ.Push(MakeEntry('job-99', 'ProcessOrder', 'orders'));
  LEntry := FDLQ.Pop('job-99');
  Assert.AreEqual('job-99',       LEntry.JobId);
  Assert.AreEqual('ProcessOrder', LEntry.Action);
  Assert.AreEqual('orders',       LEntry.OriginalQueue);
  Assert.AreEqual(3,              LEntry.RetryCount);
end;

procedure TDeadLetterQueueTests.Pop_ExistingJob_RemovesFromDLQ;
begin
  FDLQ.Push(MakeEntry('job-x', 'A', 'q'));
  FDLQ.Pop('job-x');
  Assert.AreEqual(0, FDLQ.Count, 'Pop deve remover a entrada da DLQ');
end;

procedure TDeadLetterQueueTests.Delete_ExistingJob_RemovesEntry;
begin
  FDLQ.Push(MakeEntry('j1', 'A', 'q'));
  FDLQ.Push(MakeEntry('j2', 'B', 'q'));
  FDLQ.Delete('j1');
  Assert.AreEqual(1, FDLQ.Count);
end;

procedure TDeadLetterQueueTests.Delete_NonExistentJob_NoError;
begin
  try
    FDLQ.Delete('inexistente');
  except
    on E: Exception do
      Assert.Fail('Delete de ID inexistente nao deve levantar excecao: ' + E.Message);
  end;
end;

procedure TDeadLetterQueueTests.Retry_ExistingJob_InvokesPublishCallback;
begin
  FDLQ.Push(MakeEntry('j-retry', 'SendSMS', 'sms'));
  FDLQ.Retry('j-retry');
  Assert.AreEqual(1, Length(FRetryRequests),
    'Retry deve invocar o callback de publicacao');
end;

procedure TDeadLetterQueueTests.Retry_ExistingJob_CallbackReceivesCorrectQueueAndAction;
begin
  FDLQ.Push(MakeEntry('j-retry2', 'SendSMS', 'sms-queue'));
  FDLQ.Retry('j-retry2');
  Assert.AreEqual('sms-queue', FRetryRequests[0].QueueName);
  Assert.AreEqual('SendSMS',   FRetryRequests[0].Action);
end;

procedure TDeadLetterQueueTests.Retry_ExistingJob_RemovesFromDLQ;
begin
  FDLQ.Push(MakeEntry('j-del', 'A', 'q'));
  FDLQ.Retry('j-del');
  Assert.AreEqual(0, FDLQ.Count, 'Retry deve remover a entrada da DLQ');
end;

procedure TDeadLetterQueueTests.Retry_NonExistentJob_NoCallback;
begin
  FDLQ.Retry('nao-existe');
  Assert.AreEqual(0, Length(FRetryRequests),
    'Retry de ID inexistente nao deve chamar o callback');
end;

procedure TDeadLetterQueueTests.Count_ReflectsStateAfterPushAndDelete;
begin
  FDLQ.Push(MakeEntry('j1', 'A', 'q'));
  FDLQ.Push(MakeEntry('j2', 'B', 'q'));
  FDLQ.Delete('j1');
  FDLQ.Push(MakeEntry('j3', 'C', 'q'));
  Assert.AreEqual(2, FDLQ.Count);
end;

initialization
  TDUnitX.RegisterTestFixture(TDeadLetterQueueTests);

end.
