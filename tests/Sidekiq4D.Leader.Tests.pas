unit Sidekiq4D.Leader.Tests;

{
  Testes unitários para TSidekiqLeaderElection.
  Cobre: EnsureLeadership, IsLeader, Release e exclusão mútua entre instâncias.
}

interface

uses
  DUnitX.TestFramework,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Locking,
  Sidekiq4D.Leader;

type
  [TestFixture('LeaderElection')]
  TLeaderElectionTests = class
  private
    FStore: ISidekiqStateStore;
    FProvider: ISidekiqLockProvider;
    FLeader: ISidekiqLeaderElection;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    [Category('Unit')]
    procedure IsLeader_BeforeEnsureLeadership_ReturnsFalse;

    [Test]
    [Category('Unit')]
    procedure EnsureLeadership_FirstCall_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure IsLeader_AfterEnsureLeadership_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure EnsureLeadership_SecondCall_SameInstance_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure Release_AfterEnsureLeadership_IsLeaderReturnsFalse;

    [Test]
    [Category('Unit')]
    procedure TwoInstances_SameStore_OnlyOneBecomesLeader;

    [Test]
    [Category('Unit')]
    procedure AfterRelease_OtherInstanceCanAcquire;

    [Test]
    [Category('Unit')]
    procedure LeaderName_ReturnsConfiguredName;

    [Test]
    [Category('Unit')]
    procedure OwnerId_IsNonEmpty;
  end;

implementation

procedure TLeaderElectionTests.Setup;
begin
  FStore := TSidekiqInMemoryStateStore.New;
  FProvider := TSidekiqInMemoryLockProvider.New(FStore);
  FLeader := TSidekiqLeaderElection.New(FProvider, 'test-leader', 30);
end;

procedure TLeaderElectionTests.TearDown;
begin
  FLeader.Release;
end;

procedure TLeaderElectionTests.IsLeader_BeforeEnsureLeadership_ReturnsFalse;
begin
  Assert.IsFalse(FLeader.IsLeader,
    'IsLeader deve retornar False antes de qualquer chamada a EnsureLeadership');
end;

procedure TLeaderElectionTests.EnsureLeadership_FirstCall_ReturnsTrue;
begin
  Assert.IsTrue(FLeader.EnsureLeadership,
    'Primeira chamada a EnsureLeadership deve retornar True');
end;

procedure TLeaderElectionTests.IsLeader_AfterEnsureLeadership_ReturnsTrue;
begin
  FLeader.EnsureLeadership;
  Assert.IsTrue(FLeader.IsLeader,
    'IsLeader deve retornar True imediatamente após EnsureLeadership bem-sucedido');
end;

procedure TLeaderElectionTests.EnsureLeadership_SecondCall_SameInstance_ReturnsTrue;
begin
  FLeader.EnsureLeadership;
  Assert.IsTrue(FLeader.EnsureLeadership,
    'Segunda chamada na mesma instância (lease válido) deve retornar True e renovar');
end;

procedure TLeaderElectionTests.Release_AfterEnsureLeadership_IsLeaderReturnsFalse;
begin
  FLeader.EnsureLeadership;
  FLeader.Release;
  Assert.IsFalse(FLeader.IsLeader,
    'Após Release, IsLeader deve retornar False');
end;

procedure TLeaderElectionTests.TwoInstances_SameStore_OnlyOneBecomesLeader;
var
  LLeader2: ISidekiqLeaderElection;
  LResult1, LResult2: Boolean;
begin
  LLeader2 := TSidekiqLeaderElection.New(FProvider, 'test-leader', 30);
  try
    LResult1 := FLeader.EnsureLeadership;
    LResult2 := LLeader2.EnsureLeadership;

    Assert.IsTrue(LResult1 xor LResult2,
      'Com duas instâncias competindo, exatamente uma deve se tornar líder');
  finally
    LLeader2.Release;
  end;
end;

procedure TLeaderElectionTests.AfterRelease_OtherInstanceCanAcquire;
var
  LLeader2: ISidekiqLeaderElection;
begin
  LLeader2 := TSidekiqLeaderElection.New(FProvider, 'test-leader', 30);
  try
    FLeader.EnsureLeadership;
    FLeader.Release;
    Assert.IsTrue(LLeader2.EnsureLeadership,
      'Após o líder liberar, outra instância deve conseguir adquirir liderança');
  finally
    LLeader2.Release;
  end;
end;

procedure TLeaderElectionTests.LeaderName_ReturnsConfiguredName;
begin
  Assert.AreEqual('test-leader', FLeader.LeaderName,
    'LeaderName deve retornar o nome configurado no construtor');
end;

procedure TLeaderElectionTests.OwnerId_IsNonEmpty;
begin
  Assert.IsFalse(FLeader.OwnerId.IsEmpty,
    'OwnerId deve ser um GUID não vazio');
end;

initialization
  TDUnitX.RegisterTestFixture(TLeaderElectionTests);

end.
