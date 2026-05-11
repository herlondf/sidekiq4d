unit Sidekiq4D.Middleware.Tests;

{
  Testes unitários para middlewares de servidor:
    - TSidekiqCircuitBreakerMiddleware: transições de estado (closed/open/half-open)
    - TSidekiqDeduplicationMiddleware: skip de jobs duplicados via idempotency key

  Utiliza TNullQueueAdapter e TSidekiqJobEnvelope diretamente para isolar o teste
  do broker e do executor reais.
}

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Middleware,
  Sidekiq4D.Middleware.CircuitBreaker,
  Sidekiq4D.Middleware.Deduplication;

type
  // Stub mínimo de ISidekiqQueueAdapter — sem broker real.
  TNullQueueAdapter = class(TInterfacedObject, ISidekiqQueueAdapter)
  private
    FNackCount: Integer;
  public
    function Name: string;
    function Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);
    function NackCount: Integer;
  end;

  [TestFixture('CircuitBreaker')]
  TCircuitBreakerTests = class
  private
    FCB: TSidekiqCircuitBreakerMiddleware;
    FQueue: TNullQueueAdapter;
    FQueueIntf: ISidekiqQueueAdapter;

    procedure CallWithSuccess(const AAction: string);
    procedure CallWithFailure(const AAction: string);
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    [Category('Unit')]
    procedure CircuitStateFor_NewAction_ReturnsClosed;

    [Test]
    [Category('Unit')]
    procedure FailureCountFor_NewAction_ReturnsZero;

    [Test]
    [Category('Unit')]
    procedure RecordSuccess_KeepsCircuitClosed;

    [Test]
    [Category('Unit')]
    procedure FailuresReachThreshold_CircuitOpens;

    [Test]
    [Category('Unit')]
    procedure OpenCircuit_RejectsJobWithNack;

    [Test]
    [Category('Unit')]
    procedure SuccessAfterFailures_ResetsFailureCount;
  end;

  [TestFixture('Deduplication')]
  TDeduplicationTests = class
  private
    FStore: ISidekiqStateStore;
    FDedup: TSidekiqDeduplicationMiddleware;
    FQueue: ISidekiqQueueAdapter;

    function MakeJob(const AId, AAction: string): ISidekiqJobEnvelope;
    function MakeJobWithKey(
      const AId, AAction, AKey: string): ISidekiqJobEnvelope;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    [Category('Unit')]
    procedure Call_NoIdempotencyKey_AlwaysExecutes;

    [Test]
    [Category('Unit')]
    procedure Call_NewKey_Executes;

    [Test]
    [Category('Unit')]
    procedure Call_DuplicateKey_SkipsSecondExecution;

    [Test]
    [Category('Unit')]
    procedure Call_SameKeyDifferentActions_BothExecute;
  end;

implementation

{ TNullQueueAdapter }

function TNullQueueAdapter.Name: string;
begin
  Result := 'null';
end;

function TNullQueueAdapter.Fetch(
  const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
begin
  SetLength(Result, 0);
end;

procedure TNullQueueAdapter.Ack(const AJob: ISidekiqJobEnvelope);
begin
end;

procedure TNullQueueAdapter.Nack(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
begin
  Inc(FNackCount);
end;

procedure TNullQueueAdapter.MoveToDeadLetter(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
end;

function TNullQueueAdapter.NackCount: Integer;
begin
  Result := FNackCount;
end;

{ TCircuitBreakerTests }

procedure TCircuitBreakerTests.Setup;
begin
  FCB := TSidekiqCircuitBreakerMiddleware.New
    .FailureThreshold(2)
    .CooldownSeconds(60);
  FQueue := TNullQueueAdapter.Create;
  FQueueIntf := FQueue as ISidekiqQueueAdapter;
end;

procedure TCircuitBreakerTests.TearDown;
begin
  FCB.Free;
end;

procedure TCircuitBreakerTests.CallWithSuccess(const AAction: string);
var
  LJob: ISidekiqJobEnvelope;
begin
  LJob := TSidekiqJobEnvelope.New('j-s', 'q', 'rh', '{}', AAction);
  FCB.Call(FQueueIntf, LJob, procedure begin end);
end;

procedure TCircuitBreakerTests.CallWithFailure(const AAction: string);
var
  LJob: ISidekiqJobEnvelope;
begin
  LJob := TSidekiqJobEnvelope.New('j-f', 'q', 'rh', '{}', AAction);
  try
    FCB.Call(FQueueIntf, LJob,
      procedure
      begin
        raise Exception.Create('falha simulada');
      end);
  except
    // CircuitBreaker re-levanta — capturado aqui para continuar o teste
  end;
end;

procedure TCircuitBreakerTests.CircuitStateFor_NewAction_ReturnsClosed;
begin
  Assert.AreEqual(Ord(csClosed), Ord(FCB.CircuitStateFor('nova.action')),
    'Circuito deve começar no estado Closed para qualquer nova action');
end;

procedure TCircuitBreakerTests.FailureCountFor_NewAction_ReturnsZero;
begin
  Assert.AreEqual(0, FCB.FailureCountFor('nova.action'),
    'Contador de falhas deve ser 0 para action nunca vista');
end;

procedure TCircuitBreakerTests.RecordSuccess_KeepsCircuitClosed;
begin
  CallWithSuccess('pay.process');
  Assert.AreEqual(Ord(csClosed), Ord(FCB.CircuitStateFor('pay.process')),
    'Sucesso deve manter o circuito Closed');
end;

procedure TCircuitBreakerTests.FailuresReachThreshold_CircuitOpens;
begin
  // FailureThreshold=2: após 2 falhas o circuito deve abrir
  CallWithFailure('send.email');
  CallWithFailure('send.email');
  Assert.AreEqual(Ord(csOpen), Ord(FCB.CircuitStateFor('send.email')),
    'Após atingir o threshold de falhas, o circuito deve abrir');
end;

procedure TCircuitBreakerTests.OpenCircuit_RejectsJobWithNack;
var
  LJob: ISidekiqJobEnvelope;
  LNacksBefore: Integer;
begin
  // Abre o circuito
  CallWithFailure('notify.push');
  CallWithFailure('notify.push');

  // Tenta executar com circuito aberto
  LNacksBefore := (FQueueIntf as TNullQueueAdapter).NackCount;
  LJob := TSidekiqJobEnvelope.New('j-n', 'q', 'rh', '{}', 'notify.push');
  FCB.Call(FQueueIntf, LJob, procedure begin end);

  Assert.AreEqual(LNacksBefore + 1, (FQueueIntf as TNullQueueAdapter).NackCount,
    'Circuito aberto deve chamar Nack em vez de executar o job');
end;

procedure TCircuitBreakerTests.SuccessAfterFailures_ResetsFailureCount;
begin
  CallWithFailure('report.gen');
  CallWithSuccess('report.gen');  // sucesso fecha o circuito e zera contador
  Assert.AreEqual(0, FCB.FailureCountFor('report.gen'),
    'Sucesso deve resetar o contador de falhas');
  Assert.AreEqual(Ord(csClosed), Ord(FCB.CircuitStateFor('report.gen')),
    'Sucesso deve fechar o circuito novamente');
end;

{ TDeduplicationTests }

function TDeduplicationTests.MakeJob(
  const AId, AAction: string): ISidekiqJobEnvelope;
begin
  Result := TSidekiqJobEnvelope.New(AId, 'q', 'rh', '{}', AAction);
end;

function TDeduplicationTests.MakeJobWithKey(
  const AId, AAction, AKey: string): ISidekiqJobEnvelope;
var
  LEnv: TSidekiqJobEnvelope;
begin
  LEnv := TSidekiqJobEnvelope.Create(AId, 'q', 'rh', '{}', AAction);
  LEnv.AddAttribute('idempotency_key', AKey);
  Result := LEnv;
end;

procedure TDeduplicationTests.Setup;
begin
  FStore := TSidekiqInMemoryStateStore.New;
  FDedup := TSidekiqDeduplicationMiddleware.New(FStore);
  FQueue := TNullQueueAdapter.Create as ISidekiqQueueAdapter;
end;

procedure TDeduplicationTests.TearDown;
begin
  FDedup.Free;
end;

procedure TDeduplicationTests.Call_NoIdempotencyKey_AlwaysExecutes;
var
  LCallCount: Integer;
begin
  LCallCount := 0;
  FDedup.Call(FQueue, MakeJob('j-1', 'act'),
    procedure begin Inc(LCallCount); end);
  FDedup.Call(FQueue, MakeJob('j-2', 'act'),
    procedure begin Inc(LCallCount); end);
  Assert.AreEqual(2, LCallCount,
    'Jobs sem idempotency_key nunca devem ser deduplicados');
end;

procedure TDeduplicationTests.Call_NewKey_Executes;
var
  LCallCount: Integer;
begin
  LCallCount := 0;
  FDedup.Call(FQueue, MakeJobWithKey('j-1', 'act', 'key-new'),
    procedure begin Inc(LCallCount); end);
  Assert.AreEqual(1, LCallCount,
    'Primeira chamada com chave nova deve executar o job');
end;

procedure TDeduplicationTests.Call_DuplicateKey_SkipsSecondExecution;
var
  LCallCount: Integer;
begin
  LCallCount := 0;
  FDedup.Call(FQueue, MakeJobWithKey('j-1', 'send.email', 'dup-key'),
    procedure begin Inc(LCallCount); end);
  FDedup.Call(FQueue, MakeJobWithKey('j-2', 'send.email', 'dup-key'),
    procedure begin Inc(LCallCount); end);
  Assert.AreEqual(1, LCallCount,
    'Segundo job com a mesma chave e action deve ser pulado');
end;

procedure TDeduplicationTests.Call_SameKeyDifferentActions_BothExecute;
var
  LCallCount: Integer;
begin
  LCallCount := 0;
  FDedup.Call(FQueue, MakeJobWithKey('j-1', 'action.a', 'shared-key'),
    procedure begin Inc(LCallCount); end);
  FDedup.Call(FQueue, MakeJobWithKey('j-2', 'action.b', 'shared-key'),
    procedure begin Inc(LCallCount); end);
  Assert.AreEqual(2, LCallCount,
    'A mesma chave em actions diferentes não deve causar deduplicação');
end;

initialization
  TDUnitX.RegisterTestFixture(TCircuitBreakerTests);
  TDUnitX.RegisterTestFixture(TDeduplicationTests);

end.
