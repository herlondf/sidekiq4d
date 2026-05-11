unit Sidekiq4D.Batch.Tests;

{
  Testes unitários para TSidekiqBatchStateStore (ISidekiqBatchService).
  Cobre: criação de batch, contadores, disparo de callbacks e PopReadyCallbacks.
}

interface

uses
  DUnitX.TestFramework,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Batch;

type
  [TestFixture('BatchService')]
  TBatchServiceTests = class
  private
    FStore: ISidekiqStateStore;
    FService: ISidekiqBatchService;

    function MakeCallbackRequest(
      const AQueue, AAction: string): TSidekiqPublishRequest;
  public
    [Setup]
    procedure Setup;

    [Test]
    [Category('Unit')]
    procedure CreateBatch_ReturnsNonEmptyId;

    [Test]
    [Category('Unit')]
    procedure CreateBatch_TwoCalls_ReturnDifferentIds;

    [Test]
    [Category('Unit')]
    procedure AddJob_IncrementsPending;

    [Test]
    [Category('Unit')]
    procedure RecordSuccess_AllJobs_TriggerCompleteCallback;

    [Test]
    [Category('Unit')]
    procedure RecordFinalFailure_WithFailures_SuccessCallbackNotQueued;

    [Test]
    [Category('Unit')]
    procedure RecordFinalFailure_AllFail_CompleteCallbackIsQueued;

    [Test]
    [Category('Unit')]
    procedure PopReadyCallbacks_AfterSuccess_ReturnsCallbackAndRemovesIt;

    [Test]
    [Category('Unit')]
    procedure PopReadyCallbacks_NoCallbacksReady_ReturnsEmpty;

    [Test]
    [Category('Unit')]
    procedure RecordSuccess_EmptyBatchId_DoesNothing;
  end;

implementation

uses
  System.SysUtils;

{ TBatchServiceTests }

function TBatchServiceTests.MakeCallbackRequest(
  const AQueue, AAction: string): TSidekiqPublishRequest;
begin
  Result.QueueName := AQueue;
  Result.Action := AAction;
  Result.Body := '{}';
  Result.DelaySeconds := 0;
  SetLength(Result.Attributes, 0);
end;

procedure TBatchServiceTests.Setup;
begin
  FStore := TSidekiqInMemoryStateStore.New;
  FService := TSidekiqBatchStateStore.New(FStore);
end;

procedure TBatchServiceTests.CreateBatch_ReturnsNonEmptyId;
var
  LId: string;
begin
  LId := FService.CreateBatch('meu batch');
  Assert.IsFalse(LId.IsEmpty,
    'CreateBatch deve retornar um ID não vazio');
end;

procedure TBatchServiceTests.CreateBatch_TwoCalls_ReturnDifferentIds;
var
  LId1, LId2: string;
begin
  LId1 := FService.CreateBatch('batch 1');
  LId2 := FService.CreateBatch('batch 2');
  Assert.AreNotEqual(LId1, LId2,
    'Duas chamadas a CreateBatch devem retornar IDs distintos');
end;

procedure TBatchServiceTests.AddJob_IncrementsPending;
var
  LId: string;
  LCallbacks: TArray<TSidekiqPublishRequest>;
begin
  LId := FService.CreateBatch('test');
  FService.AddJob(LId);
  FService.AddJob(LId);

  // Verifica indiretamente: com 2 pending, RecordSuccess de 1 não deve disparar callback
  FService.RegisterCallback(LId, bckComplete,
    MakeCallbackRequest('q', 'on.complete'));
  FService.RecordSuccess(LId);

  LCallbacks := FService.PopReadyCallbacks;
  Assert.AreEqual(0, Length(LCallbacks),
    'Com 1 job ainda pendente, o callback não deve estar pronto');
end;

procedure TBatchServiceTests.RecordSuccess_AllJobs_TriggerCompleteCallback;
var
  LId: string;
  LCallbacks: TArray<TSidekiqPublishRequest>;
begin
  LId := FService.CreateBatch('test-complete');
  FService.RegisterCallback(LId, bckComplete,
    MakeCallbackRequest('notifications', 'on.complete'));
  FService.AddJob(LId);
  FService.RecordSuccess(LId);

  LCallbacks := FService.PopReadyCallbacks;
  Assert.AreEqual(1, Length(LCallbacks),
    'Após todos os jobs completarem, o callback OnComplete deve ser disparado');
  Assert.AreEqual('on.complete', LCallbacks[0].Action);
end;

procedure TBatchServiceTests.RecordFinalFailure_WithFailures_SuccessCallbackNotQueued;
var
  LId: string;
  LCallbacks: TArray<TSidekiqPublishRequest>;
begin
  LId := FService.CreateBatch('test-failure');
  FService.RegisterCallback(LId, bckSuccess,
    MakeCallbackRequest('q', 'on.success'));
  FService.AddJob(LId);
  FService.RecordFinalFailure(LId);

  LCallbacks := FService.PopReadyCallbacks;
  Assert.AreEqual(0, Length(LCallbacks),
    'Quando há falhas, o callback OnSuccess não deve ser disparado');
end;

procedure TBatchServiceTests.RecordFinalFailure_AllFail_CompleteCallbackIsQueued;
var
  LId: string;
  LCallbacks: TArray<TSidekiqPublishRequest>;
begin
  LId := FService.CreateBatch('test-failure-complete');
  FService.RegisterCallback(LId, bckComplete,
    MakeCallbackRequest('q', 'on.complete'));
  FService.AddJob(LId);
  FService.RecordFinalFailure(LId);

  LCallbacks := FService.PopReadyCallbacks;
  Assert.AreEqual(1, Length(LCallbacks),
    'OnComplete deve ser disparado mesmo quando todos os jobs falham');
  Assert.AreEqual('on.complete', LCallbacks[0].Action);
end;

procedure TBatchServiceTests.PopReadyCallbacks_AfterSuccess_ReturnsCallbackAndRemovesIt;
var
  LId: string;
  LFirst, LSecond: TArray<TSidekiqPublishRequest>;
begin
  LId := FService.CreateBatch('test-pop');
  FService.RegisterCallback(LId, bckComplete,
    MakeCallbackRequest('q', 'on.complete'));
  FService.AddJob(LId);
  FService.RecordSuccess(LId);

  LFirst := FService.PopReadyCallbacks;
  LSecond := FService.PopReadyCallbacks;

  Assert.AreEqual(1, Length(LFirst),
    'Primeira chamada deve retornar 1 callback');
  Assert.AreEqual(0, Length(LSecond),
    'Segunda chamada deve retornar 0 callbacks (já foram removidos)');
end;

procedure TBatchServiceTests.PopReadyCallbacks_NoCallbacksReady_ReturnsEmpty;
var
  LCallbacks: TArray<TSidekiqPublishRequest>;
begin
  LCallbacks := FService.PopReadyCallbacks;
  Assert.AreEqual(0, Length(LCallbacks),
    'PopReadyCallbacks sem nenhum callback pronto deve retornar array vazio');
end;

procedure TBatchServiceTests.RecordSuccess_EmptyBatchId_DoesNothing;
begin
  Assert.WillNotRaise(
    procedure begin FService.RecordSuccess(''); end,
    nil,
    'RecordSuccess com ID vazio não deve levantar exceção');
end;

initialization
  TDUnitX.RegisterTestFixture(TBatchServiceTests);

end.
