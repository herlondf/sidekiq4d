unit Hefesto.Graph.Tests;

{
  Testes unitários para THefestoJobGraph.
  Cobre: topological sort, execução em ordem, propagação de falha,
  cancelamento de descendentes, detecção de ciclo e validações.
}

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.SyncObjs,
  Hefesto.Graph;

type
  [TestFixture('JobGraph')]
  TJobGraphTests = class
  public
    [Test]
    [Category('Unit')]
    procedure Execute_EmptyGraph_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure Execute_SingleNode_RunsIt;

    [Test]
    [Category('Unit')]
    procedure Execute_LinearChain_RunsInOrder;

    [Test]
    [Category('Unit')]
    procedure Execute_Diamond_RunsAllFourNodes;

    [Test]
    [Category('Unit')]
    procedure Execute_NodeFailure_MarksAsFailed;

    [Test]
    [Category('Unit')]
    procedure Execute_NodeFailure_CancelsDirectChild;

    [Test]
    [Category('Unit')]
    procedure Execute_NodeFailure_CancelsTransitiveDescendant;

    [Test]
    [Category('Unit')]
    procedure Execute_NodeFailure_DoesNotCancelIndependentNodes;

    [Test]
    [Category('Unit')]
    procedure Execute_AllSuccess_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure Execute_WithFailure_ReturnsFalse;

    [Test]
    [Category('Unit')]
    procedure Execute_OnNodeFailed_CallbackReceivesNodeId;

    [Test]
    [Category('Unit')]
    procedure Execute_OnNodeCancelled_CallbackInvokedForCancelledNode;

    [Test]
    [Category('Unit')]
    procedure Execute_Cycle_RaisesEHefestoGraphCycle;

    [Test]
    [Category('Unit')]
    procedure Execute_UnknownDependency_RaisesException;

    [Test]
    [Category('Unit')]
    procedure AddNode_DuplicateId_RaisesException;

    [Test]
    [Category('Unit')]
    procedure DependsOn_WithoutPriorAddNode_RaisesException;

    [Test]
    [Category('Unit')]
    procedure Node_AfterExecute_ReturnsCorrectStatus;

    [Test]
    [Category('Unit')]
    procedure Execute_CanBeCalledTwice_ResetsStatus;

    { Parallel mode }

    [Test]
    [Category('Unit')]
    procedure Execute_Parallel_Diamond_RunsAllFourNodes;

    [Test]
    [Category('Unit')]
    procedure Execute_Parallel_IndependentNodes_BothRun;

    [Test]
    [Category('Unit')]
    procedure Execute_Parallel_LinearChain_RespectsOrder;

    [Test]
    [Category('Unit')]
    procedure Execute_Parallel_NodeFailure_CancelsDirectChild;

    [Test]
    [Category('Unit')]
    procedure Execute_Parallel_AllSuccess_ReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure Execute_Parallel_WithFailure_ReturnsFalse;
  end;

implementation

procedure TJobGraphTests.Execute_EmptyGraph_ReturnsTrue;
begin
  Assert.IsTrue(THefestoJobGraph.New.Execute,
    'Grafo vazio deve retornar True');
end;

procedure TJobGraphTests.Execute_SingleNode_RunsIt;
var
  LRan: Boolean;
begin
  LRan := False;
  THefestoJobGraph.New
    .AddNode('only', procedure begin LRan := True; end)
    .Execute;
  Assert.IsTrue(LRan, 'Nó único deve ser executado');
end;

procedure TJobGraphTests.Execute_LinearChain_RunsInOrder;
var
  LLog: string;
begin
  LLog := '';
  THefestoJobGraph.New
    .AddNode('a', procedure begin LLog := LLog + 'A'; end)
    .AddNode('b', procedure begin LLog := LLog + 'B'; end)
      .DependsOn('a')
    .AddNode('c', procedure begin LLog := LLog + 'C'; end)
      .DependsOn('b')
    .Execute;
  Assert.AreEqual('ABC', LLog,
    'Cadeia linear A→B→C deve executar nessa ordem');
end;

procedure TJobGraphTests.Execute_Diamond_RunsAllFourNodes;
var
  LCount: Integer;
begin
  LCount := 0;
  THefestoJobGraph.New
    .AddNode('root',   procedure begin Inc(LCount); end)
    .AddNode('left',   procedure begin Inc(LCount); end).DependsOn('root')
    .AddNode('right',  procedure begin Inc(LCount); end).DependsOn('root')
    .AddNode('merge',  procedure begin Inc(LCount); end)
      .DependsOn('left').DependsOn('right')
    .Execute;
  Assert.AreEqual(4, LCount,
    'Diamante root→(left,right)→merge deve executar todos os 4 nós');
end;

procedure TJobGraphTests.Execute_NodeFailure_MarksAsFailed;
var
  LGraph: IHefestoJobGraph;
begin
  LGraph := THefestoJobGraph.New
    .AddNode('fail', procedure begin raise Exception.Create('boom'); end);
  LGraph.Execute;
  Assert.AreEqual(Ord(nsFailed), Ord(LGraph.Node('fail').NodeStatus),
    'Nó que levantou exceção deve ter status nsFailed');
end;

procedure TJobGraphTests.Execute_NodeFailure_CancelsDirectChild;
var
  LGraph: IHefestoJobGraph;
  LChildRan: Boolean;
begin
  LChildRan := False;
  LGraph := THefestoJobGraph.New
    .AddNode('parent', procedure begin raise Exception.Create('boom'); end)
    .AddNode('child',  procedure begin LChildRan := True; end)
      .DependsOn('parent');
  LGraph.Execute;
  Assert.IsFalse(LChildRan,
    'Filho de nó com falha não deve ser executado');
  Assert.AreEqual(Ord(nsCancelled), Ord(LGraph.Node('child').NodeStatus),
    'Filho de nó com falha deve ter status nsCancelled');
end;

procedure TJobGraphTests.Execute_NodeFailure_CancelsTransitiveDescendant;
var
  LGraph: IHefestoJobGraph;
  LGrandchildRan: Boolean;
begin
  LGrandchildRan := False;
  LGraph := THefestoJobGraph.New
    .AddNode('root',       procedure begin raise Exception.Create('boom'); end)
    .AddNode('child',      procedure begin end).DependsOn('root')
    .AddNode('grandchild', procedure begin LGrandchildRan := True; end).DependsOn('child');
  LGraph.Execute;
  Assert.IsFalse(LGrandchildRan,
    'Descendente transitivo de nó com falha não deve executar');
  Assert.AreEqual(Ord(nsCancelled), Ord(LGraph.Node('grandchild').NodeStatus));
end;

procedure TJobGraphTests.Execute_NodeFailure_DoesNotCancelIndependentNodes;
var
  LGraph: IHefestoJobGraph;
  LIndepRan: Boolean;
begin
  LIndepRan := False;
  LGraph := THefestoJobGraph.New
    .AddNode('fail',  procedure begin raise Exception.Create('boom'); end)
    .AddNode('indep', procedure begin LIndepRan := True; end); // sem DependsOn
  LGraph.Execute;
  Assert.IsTrue(LIndepRan,
    'Nó independente não deve ser cancelado pela falha de outro nó');
  Assert.AreEqual(Ord(nsDone), Ord(LGraph.Node('indep').NodeStatus));
end;

procedure TJobGraphTests.Execute_AllSuccess_ReturnsTrue;
begin
  Assert.IsTrue(
    THefestoJobGraph.New
      .AddNode('a', procedure begin end)
      .AddNode('b', procedure begin end).DependsOn('a')
      .Execute,
    'Todos os nós bem-sucedidos devem retornar True');
end;

procedure TJobGraphTests.Execute_WithFailure_ReturnsFalse;
begin
  Assert.IsFalse(
    THefestoJobGraph.New
      .AddNode('ok',   procedure begin end)
      .AddNode('fail', procedure begin raise Exception.Create('x'); end)
      .Execute,
    'Falha em qualquer nó deve retornar False');
end;

procedure TJobGraphTests.Execute_OnNodeFailed_CallbackReceivesNodeId;
var
  LFailedId: string;
begin
  LFailedId := '';
  THefestoJobGraph.New
    .AddNode('boom', procedure begin raise Exception.Create('err'); end)
    .OnNodeFailed(procedure(const AId, AErr: string) begin LFailedId := AId; end)
    .Execute;
  Assert.AreEqual('boom', LFailedId,
    'OnNodeFailed deve receber o ID do nó com falha');
end;

procedure TJobGraphTests.Execute_OnNodeCancelled_CallbackInvokedForCancelledNode;
var
  LCancelledId: string;
begin
  LCancelledId := '';
  THefestoJobGraph.New
    .AddNode('parent', procedure begin raise Exception.Create('err'); end)
    .AddNode('child',  procedure begin end).DependsOn('parent')
    .OnNodeCancelled(procedure(const AId: string) begin LCancelledId := AId; end)
    .Execute;
  Assert.AreEqual('child', LCancelledId,
    'OnNodeCancelled deve receber o ID do nó cancelado');
end;

procedure TJobGraphTests.Execute_Cycle_RaisesEHefestoGraphCycle;
begin
  Assert.WillRaise(
    procedure
    begin
      THefestoJobGraph.New
        .AddNode('a', procedure begin end).DependsOn('b')
        .AddNode('b', procedure begin end).DependsOn('a')
        .Execute;
    end,
    EHefestoGraphCycle,
    'Ciclo A→B→A deve levantar EHefestoGraphCycle');
end;

procedure TJobGraphTests.Execute_UnknownDependency_RaisesException;
begin
  Assert.WillRaise(
    procedure
    begin
      THefestoJobGraph.New
        .AddNode('child', procedure begin end).DependsOn('nonexistent')
        .Execute;
    end,
    EHefestoGraphUnknownDependency,
    'Dependência de nó inexistente deve levantar EHefestoGraphUnknownDependency');
end;

procedure TJobGraphTests.AddNode_DuplicateId_RaisesException;
begin
  Assert.WillRaise(
    procedure
    begin
      THefestoJobGraph.New
        .AddNode('dup', procedure begin end)
        .AddNode('dup', procedure begin end);
    end,
    EHefestoGraphDuplicateNode,
    'ID duplicado deve levantar EHefestoGraphDuplicateNode');
end;

procedure TJobGraphTests.DependsOn_WithoutPriorAddNode_RaisesException;
begin
  Assert.WillRaise(
    procedure
    begin
      THefestoJobGraph.New.DependsOn('parent');
    end,
    EHefestoJobGraph,
    'DependsOn sem AddNode anterior deve levantar exceção');
end;

procedure TJobGraphTests.Node_AfterExecute_ReturnsCorrectStatus;
var
  LGraph: IHefestoJobGraph;
begin
  LGraph := THefestoJobGraph.New
    .AddNode('ok',   procedure begin end)
    .AddNode('fail', procedure begin raise Exception.Create('x'); end);
  LGraph.Execute;
  Assert.AreEqual(Ord(nsDone),   Ord(LGraph.Node('ok').NodeStatus));
  Assert.AreEqual(Ord(nsFailed), Ord(LGraph.Node('fail').NodeStatus));
end;

procedure TJobGraphTests.Execute_CanBeCalledTwice_ResetsStatus;
var
  LGraph: IHefestoJobGraph;
  LCount: Integer;
begin
  LCount := 0;
  LGraph := THefestoJobGraph.New
    .AddNode('a', procedure begin Inc(LCount); end)
    .AddNode('b', procedure begin Inc(LCount); end).DependsOn('a');
  LGraph.Execute;
  LGraph.Execute;
  Assert.AreEqual(4, LCount,
    'Execute chamado duas vezes deve executar todos os nós duas vezes');
  Assert.AreEqual(Ord(nsDone), Ord(LGraph.Node('b').NodeStatus));
end;

{ Parallel mode tests }

procedure TJobGraphTests.Execute_Parallel_Diamond_RunsAllFourNodes;
var
  LCount: Integer;
begin
  LCount := 0;
  THefestoJobGraph.New
    .AddNode('root',  procedure begin TInterlocked.Increment(LCount); end)
    .AddNode('left',  procedure begin TInterlocked.Increment(LCount); end).DependsOn('root')
    .AddNode('right', procedure begin TInterlocked.Increment(LCount); end).DependsOn('root')
    .AddNode('merge', procedure begin TInterlocked.Increment(LCount); end)
      .DependsOn('left').DependsOn('right')
    .Parallel
    .Execute;
  Assert.AreEqual(4, LCount,
    'Modo paralelo: diamante deve executar todos os 4 nós');
end;

procedure TJobGraphTests.Execute_Parallel_IndependentNodes_BothRun;
var
  LCount: Integer;
begin
  LCount := 0;
  THefestoJobGraph.New
    .AddNode('a', procedure begin TInterlocked.Increment(LCount); end)
    .AddNode('b', procedure begin TInterlocked.Increment(LCount); end)
    .Parallel
    .Execute;
  Assert.AreEqual(2, LCount,
    'Modo paralelo: nós independentes devem executar os dois');
end;

procedure TJobGraphTests.Execute_Parallel_LinearChain_RespectsOrder;
var
  LLog: string;
  LLock: TCriticalSection;
begin
  LLog  := '';
  LLock := TCriticalSection.Create;
  try
    THefestoJobGraph.New
      .AddNode('a', procedure begin LLock.Acquire; try LLog := LLog + 'A'; finally LLock.Release; end; end)
      .AddNode('b', procedure begin LLock.Acquire; try LLog := LLog + 'B'; finally LLock.Release; end; end).DependsOn('a')
      .AddNode('c', procedure begin LLock.Acquire; try LLog := LLog + 'C'; finally LLock.Release; end; end).DependsOn('b')
      .Parallel
      .Execute;
    Assert.AreEqual('ABC', LLog,
      'Modo paralelo: cadeia linear deve respeitar dependências A→B→C');
  finally
    LLock.Free;
  end;
end;

procedure TJobGraphTests.Execute_Parallel_NodeFailure_CancelsDirectChild;
var
  LGraph: IHefestoJobGraph;
  LChildRan: Boolean;
begin
  LChildRan := False;
  LGraph := THefestoJobGraph.New
    .AddNode('parent', procedure begin raise Exception.Create('boom'); end)
    .AddNode('child',  procedure begin LChildRan := True; end).DependsOn('parent')
    .Parallel;
  LGraph.Execute;
  Assert.IsFalse(LChildRan,
    'Modo paralelo: filho de nó com falha não deve executar');
  Assert.AreEqual(Ord(nsCancelled), Ord(LGraph.Node('child').NodeStatus));
end;

procedure TJobGraphTests.Execute_Parallel_AllSuccess_ReturnsTrue;
begin
  Assert.IsTrue(
    THefestoJobGraph.New
      .AddNode('a', procedure begin end)
      .AddNode('b', procedure begin end).DependsOn('a')
      .Parallel
      .Execute,
    'Modo paralelo: todos bem-sucedidos devem retornar True');
end;

procedure TJobGraphTests.Execute_Parallel_WithFailure_ReturnsFalse;
begin
  Assert.IsFalse(
    THefestoJobGraph.New
      .AddNode('ok',   procedure begin end)
      .AddNode('fail', procedure begin raise Exception.Create('x'); end)
      .Parallel
      .Execute,
    'Modo paralelo: falha em qualquer nó deve retornar False');
end;

initialization
  TDUnitX.RegisterTestFixture(TJobGraphTests);

end.
