unit Sidekiq4D.Graph;

{
  Job Graph — execução de jobs em-processo com dependências declarativas (DAG).

  Permite definir um pipeline de tarefas com dependências entre elas.
  A ordem de execução é determinada por Kahn's topological sort.
  Se um nó falha, todos os seus descendentes são marcados como Cancelado.

  Modo sequencial (padrão):
    TSidekiqJobGraph.New
      .AddNode('extract',   procedure begin ... end)
      .AddNode('transform', procedure begin ... end)
        .DependsOn('extract')
      .AddNode('load',      procedure begin ... end)
        .DependsOn('transform')
      .Execute;

  Modo paralelo (nós independentes executam simultaneamente via TTask):
    TSidekiqJobGraph.New
      .Parallel
      .AddNode('extract',   procedure begin ... end)
      .AddNode('validate',  procedure begin ... end).DependsOn('extract')
      .AddNode('enrich',    procedure begin ... end).DependsOn('extract')
      .AddNode('load',      procedure begin ... end)
        .DependsOn('validate').DependsOn('enrich')
      .Execute;

  No exemplo acima, "validate" e "enrich" executam em paralelo após "extract".

  Restrições:
    - Cada ID de nó deve ser único.
    - Dependências cíclicas levantam ESidekiqGraphCycle em Execute.
    - Nós sem dependências são executados primeiro; a ordem relativa entre
      nós do mesmo nível respeita a ordem de AddNode.
    - No modo paralelo, os callbacks OnNodeFailed/OnNodeCancelled são invocados
      de threads de trabalho e devem ser thread-safe.
}

interface

uses
  System.SysUtils,
  System.Classes,
  System.Threading,
  System.Generics.Collections;

type
  ESidekiqJobGraph = class(Exception);
  ESidekiqGraphCycle = class(ESidekiqJobGraph);
  ESidekiqGraphDuplicateNode = class(ESidekiqJobGraph);
  ESidekiqGraphUnknownDependency = class(ESidekiqJobGraph);

  TSidekiqNodeStatus = (nsReady, nsDone, nsFailed, nsCancelled);

  TSidekiqGraphNodeFailedProc = reference to procedure(
    const ANodeId: string;
    const AErrorMessage: string);

  TSidekiqGraphNodeCancelledProc = reference to procedure(
    const ANodeId: string);

  ISidekiqJobNode = interface
    ['{A1B2C3D4-E5F6-4A7B-8C9D-0E1F2A3B4C5D}']
    function NodeId: string;
    function NodeStatus: TSidekiqNodeStatus;
  end;

  ISidekiqJobGraph = interface
    ['{F9E8D7C6-B5A4-4938-8271-605E4F3A2B1C}']
    { Adiciona um nó ao grafo. AId deve ser único. Retorna Self para encadeamento. }
    function AddNode(
      const AId: string;
      const AProc: TProc): ISidekiqJobGraph;

    { Declara que o último nó adicionado depende de AParentId.
      Pode ser chamado múltiplas vezes para declarar múltiplos pais.
      A validação de existência do pai ocorre em Execute. }
    function DependsOn(const AParentId: string): ISidekiqJobGraph;

    { Ativa execução paralela: nós sem dependência mútua executam via TTask.
      Callbacks devem ser thread-safe quando este modo estiver ativo. }
    function Parallel: ISidekiqJobGraph;

    { Callback invocado quando um nó falha (exceção em AProc). }
    function OnNodeFailed(
      const ACallback: TSidekiqGraphNodeFailedProc): ISidekiqJobGraph;

    { Callback invocado quando um nó é cancelado (pai falhou/foi cancelado). }
    function OnNodeCancelled(
      const ACallback: TSidekiqGraphNodeCancelledProc): ISidekiqJobGraph;

    { Retorna o snapshot de status de um nó após Execute. }
    function Node(const AId: string): ISidekiqJobNode;

    { Executa o grafo em ordem topológica.
      Retorna True se todos os nós foram executados com sucesso.
      Levanta ESidekiqGraphCycle se houver ciclo.
      Levanta ESidekiqGraphUnknownDependency se uma dependência não existir. }
    function Execute: Boolean;
  end;

  TSidekiqJobGraph = class(TInterfacedObject, ISidekiqJobGraph)
  private type
    TGraphNode = class
    public
      Id: string;
      Proc: TProc;
      Parents: TStringList;
      Status: TSidekiqNodeStatus;
      constructor Create(const AId: string; const AProc: TProc);
      destructor Destroy; override;
    end;

    TNodeSnapshot = class(TInterfacedObject, ISidekiqJobNode)
    private
      FId: string;
      FStatus: TSidekiqNodeStatus;
    public
      constructor Create(const AId: string; const AStatus: TSidekiqNodeStatus);
      function NodeId: string;
      function NodeStatus: TSidekiqNodeStatus;
    end;
  private
    FNodes: TObjectDictionary<string, TGraphNode>;
    FOrder: TList<string>;
    FLastNodeId: string;
    FParallel: Boolean;
    FOnNodeFailed: TSidekiqGraphNodeFailedProc;
    FOnNodeCancelled: TSidekiqGraphNodeCancelledProc;

    procedure ValidateDependencies;
    function ComputeTopologicalOrder: TArray<string>;
    procedure ExecuteSequential(const AOrder: TArray<string>);
    procedure ExecuteWaves;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: ISidekiqJobGraph;

    function AddNode(
      const AId: string;
      const AProc: TProc): ISidekiqJobGraph;
    function DependsOn(const AParentId: string): ISidekiqJobGraph;
    function Parallel: ISidekiqJobGraph;
    function OnNodeFailed(
      const ACallback: TSidekiqGraphNodeFailedProc): ISidekiqJobGraph;
    function OnNodeCancelled(
      const ACallback: TSidekiqGraphNodeCancelledProc): ISidekiqJobGraph;
    function Node(const AId: string): ISidekiqJobNode;
    function Execute: Boolean;
  end;

implementation

{ TSidekiqJobGraph.TGraphNode }

constructor TSidekiqJobGraph.TGraphNode.Create(
  const AId: string;
  const AProc: TProc);
begin
  inherited Create;
  Id := AId;
  Proc := AProc;
  Parents := TStringList.Create;
  Parents.Duplicates := dupIgnore;
  Parents.Sorted := True;
  Status := nsReady;
end;

destructor TSidekiqJobGraph.TGraphNode.Destroy;
begin
  Parents.Free;
  inherited;
end;

{ TSidekiqJobGraph.TNodeSnapshot }

constructor TSidekiqJobGraph.TNodeSnapshot.Create(
  const AId: string;
  const AStatus: TSidekiqNodeStatus);
begin
  inherited Create;
  FId := AId;
  FStatus := AStatus;
end;

function TSidekiqJobGraph.TNodeSnapshot.NodeId: string;
begin
  Result := FId;
end;

function TSidekiqJobGraph.TNodeSnapshot.NodeStatus: TSidekiqNodeStatus;
begin
  Result := FStatus;
end;

{ TSidekiqJobGraph }

constructor TSidekiqJobGraph.Create;
begin
  inherited Create;
  FNodes    := TObjectDictionary<string, TGraphNode>.Create([doOwnsValues]);
  FOrder    := TList<string>.Create;
  FParallel := False;
  FLastNodeId := EmptyStr;
end;

destructor TSidekiqJobGraph.Destroy;
begin
  FOrder.Free;
  FNodes.Free;
  inherited;
end;

class function TSidekiqJobGraph.New: ISidekiqJobGraph;
begin
  Result := TSidekiqJobGraph.Create;
end;

function TSidekiqJobGraph.AddNode(
  const AId: string;
  const AProc: TProc): ISidekiqJobGraph;
begin
  Result := Self;
  if AId.Trim.IsEmpty then
    raise ESidekiqJobGraph.Create('Graph node ID cannot be empty.');
  if not Assigned(AProc) then
    raise ESidekiqJobGraph.Create('Graph node proc cannot be nil.');
  if FNodes.ContainsKey(AId) then
    raise ESidekiqGraphDuplicateNode.CreateFmt(
      'Graph node "%s" already exists.', [AId]);

  FNodes.Add(AId, TGraphNode.Create(AId, AProc));
  FOrder.Add(AId);
  FLastNodeId := AId;
end;

function TSidekiqJobGraph.DependsOn(const AParentId: string): ISidekiqJobGraph;
begin
  Result := Self;
  if FLastNodeId.IsEmpty then
    raise ESidekiqJobGraph.Create(
      'DependsOn must be called after AddNode.');
  if AParentId.Trim.IsEmpty then
    raise ESidekiqJobGraph.Create('Parent node ID cannot be empty.');
  FNodes[FLastNodeId].Parents.Add(AParentId);
end;

function TSidekiqJobGraph.Parallel: ISidekiqJobGraph;
begin
  FParallel := True;
  Result := Self;
end;

function TSidekiqJobGraph.OnNodeFailed(
  const ACallback: TSidekiqGraphNodeFailedProc): ISidekiqJobGraph;
begin
  Result := Self;
  FOnNodeFailed := ACallback;
end;

function TSidekiqJobGraph.OnNodeCancelled(
  const ACallback: TSidekiqGraphNodeCancelledProc): ISidekiqJobGraph;
begin
  Result := Self;
  FOnNodeCancelled := ACallback;
end;

function TSidekiqJobGraph.Node(const AId: string): ISidekiqJobNode;
var
  LNode: TGraphNode;
begin
  if FNodes.TryGetValue(AId, LNode) then
    Result := TNodeSnapshot.Create(AId, LNode.Status)
  else
    Result := nil;
end;

procedure TSidekiqJobGraph.ValidateDependencies;
var
  LNodeId: string;
  LNode: TGraphNode;
  LParent: string;
begin
  for LNodeId in FOrder do
  begin
    LNode := FNodes[LNodeId];
    for LParent in LNode.Parents do
      if not FNodes.ContainsKey(LParent) then
        raise ESidekiqGraphUnknownDependency.CreateFmt(
          'Node "%s" depends on "%s" which does not exist in the graph.',
          [LNodeId, LParent]);
  end;
end;

function TSidekiqJobGraph.ComputeTopologicalOrder: TArray<string>;
var
  LInDegree: TDictionary<string, Integer>;
  LChildren: TObjectDictionary<string, TList<string>>;
  LQueue: TQueue<string>;
  LResult: TList<string>;
  LNodeId, LParent, LChild: string;
  LNode: TGraphNode;
  LDeg: Integer;
begin
  LInDegree := TDictionary<string, Integer>.Create;
  LChildren := TObjectDictionary<string, TList<string>>.Create([doOwnsValues]);
  LQueue := TQueue<string>.Create;
  LResult := TList<string>.Create;
  try
    for LNodeId in FOrder do
    begin
      LInDegree.AddOrSetValue(LNodeId, 0);
      LChildren.Add(LNodeId, TList<string>.Create);
    end;

    for LNodeId in FOrder do
    begin
      LNode := FNodes[LNodeId];
      for LParent in LNode.Parents do
      begin
        LInDegree[LNodeId] := LInDegree[LNodeId] + 1;
        LChildren[LParent].Add(LNodeId);
      end;
    end;

    for LNodeId in FOrder do
      if LInDegree[LNodeId] = 0 then
        LQueue.Enqueue(LNodeId);

    while LQueue.Count > 0 do
    begin
      LNodeId := LQueue.Dequeue;
      LResult.Add(LNodeId);
      for LChild in LChildren[LNodeId] do
      begin
        LDeg := LInDegree[LChild] - 1;
        LInDegree[LChild] := LDeg;
        if LDeg = 0 then
          LQueue.Enqueue(LChild);
      end;
    end;

    if LResult.Count < FOrder.Count then
      raise ESidekiqGraphCycle.Create(
        'Job graph contains a cycle. Review DependsOn declarations.');

    Result := LResult.ToArray;
  finally
    LResult.Free;
    LQueue.Free;
    LChildren.Free;
    LInDegree.Free;
  end;
end;

procedure TSidekiqJobGraph.ExecuteSequential(const AOrder: TArray<string>);
var
  LNodeId, LParent: string;
  LNode: TGraphNode;
  LShouldCancel: Boolean;
begin
  for LNodeId in AOrder do
  begin
    LNode := FNodes[LNodeId];

    LShouldCancel := False;
    for LParent in LNode.Parents do
      if FNodes[LParent].Status in [nsFailed, nsCancelled] then
      begin
        LShouldCancel := True;
        Break;
      end;

    if LShouldCancel then
    begin
      LNode.Status := nsCancelled;
      if Assigned(FOnNodeCancelled) then
        FOnNodeCancelled(LNodeId);
      Continue;
    end;

    try
      LNode.Proc();
      LNode.Status := nsDone;
    except
      on E: Exception do
      begin
        LNode.Status := nsFailed;
        if Assigned(FOnNodeFailed) then
          FOnNodeFailed(LNodeId, E.Message);
      end;
    end;
  end;
end;

procedure TSidekiqJobGraph.ExecuteWaves;
var
  LInDegree: TDictionary<string, Integer>;
  LChildren: TObjectDictionary<string, TList<string>>;
  LQueue: TQueue<string>;
  LWave: TList<string>;
  LNodeId, LParent, LChild: string;
  LNode: TGraphNode;
  LDeg, LIdx: Integer;
  LTasks: TArray<ITask>;
begin
  LInDegree := TDictionary<string, Integer>.Create;
  LChildren := TObjectDictionary<string, TList<string>>.Create([doOwnsValues]);
  LQueue := TQueue<string>.Create;
  LWave := TList<string>.Create;
  try
    for LNodeId in FOrder do
    begin
      LInDegree.AddOrSetValue(LNodeId, 0);
      LChildren.Add(LNodeId, TList<string>.Create);
    end;

    for LNodeId in FOrder do
    begin
      LNode := FNodes[LNodeId];
      for LParent in LNode.Parents do
      begin
        LInDegree[LNodeId] := LInDegree[LNodeId] + 1;
        LChildren[LParent].Add(LNodeId);
      end;
    end;

    // Verifica ciclo antes de executar
    var LTotal := 0;

    for LNodeId in FOrder do
      if LInDegree[LNodeId] = 0 then
        LQueue.Enqueue(LNodeId);

    while LQueue.Count > 0 do
    begin
      // Coleta onda atual
      LWave.Clear;
      while LQueue.Count > 0 do
        LWave.Add(LQueue.Dequeue);

      Inc(LTotal, LWave.Count);

      // Cria uma task por nó na onda — cada uma captura seu próprio ID
      SetLength(LTasks, LWave.Count);
      for LIdx := 0 to Pred(LWave.Count) do
      begin
        var LCurrentId: string := LWave[LIdx]; // inline var: captura independente por iteração
        LTasks[LIdx] := TTask.Create(
          procedure
          var
            LCurNode: TGraphNode;
            LShouldCancel: Boolean;
            LCurParent: string;
          begin
            LCurNode := FNodes[LCurrentId];

            LShouldCancel := False;
            for LCurParent in LCurNode.Parents do
              if FNodes[LCurParent].Status in [nsFailed, nsCancelled] then
              begin
                LShouldCancel := True;
                Break;
              end;

            if LShouldCancel then
            begin
              LCurNode.Status := nsCancelled;
              if Assigned(FOnNodeCancelled) then
                FOnNodeCancelled(LCurrentId);
              Exit;
            end;

            try
              LCurNode.Proc();
              LCurNode.Status := nsDone;
            except
              on E: Exception do
              begin
                LCurNode.Status := nsFailed;
                if Assigned(FOnNodeFailed) then
                  FOnNodeFailed(LCurrentId, E.Message);
              end;
            end;
          end);
      end;

      // Inicia e aguarda toda a onda
      for var LT in LTasks do
        LT.Start;
      if Length(LTasks) > 0 then
        TTask.WaitForAll(LTasks);

      // Atualiza in-degrees dos filhos para a próxima onda
      for LNodeId in LWave do
        for LChild in LChildren[LNodeId] do
        begin
          LDeg := LInDegree[LChild] - 1;
          LInDegree[LChild] := LDeg;
          if LDeg = 0 then
            LQueue.Enqueue(LChild);
        end;
    end;

    if LTotal < FOrder.Count then
      raise ESidekiqGraphCycle.Create(
        'Job graph contains a cycle. Review DependsOn declarations.');
  finally
    LWave.Free;
    LQueue.Free;
    LChildren.Free;
    LInDegree.Free;
  end;
end;

function TSidekiqJobGraph.Execute: Boolean;
var
  LNodeId: string;
begin
  if FOrder.Count = 0 then
    Exit(True);

  ValidateDependencies;

  for LNodeId in FOrder do
    FNodes[LNodeId].Status := nsReady;

  if FParallel then
    ExecuteWaves
  else
    ExecuteSequential(ComputeTopologicalOrder);

  Result := True;
  for LNodeId in FOrder do
    if FNodes[LNodeId].Status <> nsDone then
    begin
      Result := False;
      Break;
    end;
end;

end.
