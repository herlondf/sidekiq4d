unit Hefesto.Graph;

{
  Job Graph — execução de jobs em-processo com dependências declarativas (DAG).

  Permite definir um pipeline de tarefas com dependências entre elas.
  A ordem de execução é determinada por Kahn's topological sort.
  Se um nó falha, todos os seus descendentes são marcados como Cancelado.

  Modo sequencial (padrão):
    THefestoJobGraph.New
      .AddNode('extract',   procedure begin ... end)
      .AddNode('transform', procedure begin ... end)
        .DependsOn('extract')
      .AddNode('load',      procedure begin ... end)
        .DependsOn('transform')
      .Execute;

  Modo paralelo (nós independentes executam simultaneamente via TTask):
    THefestoJobGraph.New
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
    - Dependências cíclicas levantam EHefestoGraphCycle em Execute.
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
  EHefestoJobGraph = class(Exception);
  EHefestoGraphCycle = class(EHefestoJobGraph);
  EHefestoGraphDuplicateNode = class(EHefestoJobGraph);
  EHefestoGraphUnknownDependency = class(EHefestoJobGraph);

  THefestoNodeStatus = (nsReady, nsDone, nsFailed, nsCancelled);

  THefestoGraphNodeFailedProc = reference to procedure(
    const ANodeId: string;
    const AErrorMessage: string);

  THefestoGraphNodeCancelledProc = reference to procedure(
    const ANodeId: string);

  IHefestoJobNode = interface
    ['{A1B2C3D4-E5F6-4A7B-8C9D-0E1F2A3B4C5D}']
    function NodeId: string;
    function NodeStatus: THefestoNodeStatus;
  end;

  IHefestoJobGraph = interface
    ['{F9E8D7C6-B5A4-4938-8271-605E4F3A2B1C}']
    { Adiciona um nó ao grafo. AId deve ser único. Retorna Self para encadeamento. }
    function AddNode(
      const AId: string;
      const AProc: TProc): IHefestoJobGraph;

    { Declara que o último nó adicionado depende de AParentId.
      Pode ser chamado múltiplas vezes para declarar múltiplos pais.
      A validação de existência do pai ocorre em Execute. }
    function DependsOn(const AParentId: string): IHefestoJobGraph;

    { Ativa execução paralela: nós sem dependência mútua executam via TTask.
      Callbacks devem ser thread-safe quando este modo estiver ativo. }
    function Parallel: IHefestoJobGraph;

    { Callback invocado quando um nó falha (exceção em AProc). }
    function OnNodeFailed(
      const ACallback: THefestoGraphNodeFailedProc): IHefestoJobGraph;

    { Callback invocado quando um nó é cancelado (pai falhou/foi cancelado). }
    function OnNodeCancelled(
      const ACallback: THefestoGraphNodeCancelledProc): IHefestoJobGraph;

    { Retorna o snapshot de status de um nó após Execute. }
    function Node(const AId: string): IHefestoJobNode;

    { Executa o grafo em ordem topológica.
      Retorna True se todos os nós foram executados com sucesso.
      Levanta EHefestoGraphCycle se houver ciclo.
      Levanta EHefestoGraphUnknownDependency se uma dependência não existir. }
    function Execute: Boolean;
  end;

  THefestoJobGraph = class(TInterfacedObject, IHefestoJobGraph)
  private type
    TGraphNode = class
    public
      Id: string;
      Proc: TProc;
      Parents: TStringList;
      Status: THefestoNodeStatus;
      constructor Create(const AId: string; const AProc: TProc);
      destructor Destroy; override;
    end;

    TNodeSnapshot = class(TInterfacedObject, IHefestoJobNode)
    private
      FId: string;
      FStatus: THefestoNodeStatus;
    public
      constructor Create(const AId: string; const AStatus: THefestoNodeStatus);
      function NodeId: string;
      function NodeStatus: THefestoNodeStatus;
    end;
  private
    FNodes: TObjectDictionary<string, TGraphNode>;
    FOrder: TList<string>;
    FLastNodeId: string;
    FParallel: Boolean;
    FOnNodeFailed: THefestoGraphNodeFailedProc;
    FOnNodeCancelled: THefestoGraphNodeCancelledProc;

    procedure ValidateDependencies;
    function ComputeTopologicalOrder: TArray<string>;
    procedure ExecuteSequential(const AOrder: TArray<string>);
    procedure ExecuteWaves;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoJobGraph;

    function AddNode(
      const AId: string;
      const AProc: TProc): IHefestoJobGraph;
    function DependsOn(const AParentId: string): IHefestoJobGraph;
    function Parallel: IHefestoJobGraph;
    function OnNodeFailed(
      const ACallback: THefestoGraphNodeFailedProc): IHefestoJobGraph;
    function OnNodeCancelled(
      const ACallback: THefestoGraphNodeCancelledProc): IHefestoJobGraph;
    function Node(const AId: string): IHefestoJobNode;
    function Execute: Boolean;
  end;

implementation

{ THefestoJobGraph.TGraphNode }

constructor THefestoJobGraph.TGraphNode.Create(
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

destructor THefestoJobGraph.TGraphNode.Destroy;
begin
  Parents.Free;
  inherited;
end;

{ THefestoJobGraph.TNodeSnapshot }

constructor THefestoJobGraph.TNodeSnapshot.Create(
  const AId: string;
  const AStatus: THefestoNodeStatus);
begin
  inherited Create;
  FId := AId;
  FStatus := AStatus;
end;

function THefestoJobGraph.TNodeSnapshot.NodeId: string;
begin
  Result := FId;
end;

function THefestoJobGraph.TNodeSnapshot.NodeStatus: THefestoNodeStatus;
begin
  Result := FStatus;
end;

{ THefestoJobGraph }

constructor THefestoJobGraph.Create;
begin
  inherited Create;
  FNodes    := TObjectDictionary<string, TGraphNode>.Create([doOwnsValues]);
  FOrder    := TList<string>.Create;
  FParallel := False;
  FLastNodeId := EmptyStr;
end;

destructor THefestoJobGraph.Destroy;
begin
  FOrder.Free;
  FNodes.Free;
  inherited;
end;

class function THefestoJobGraph.New: IHefestoJobGraph;
begin
  Result := THefestoJobGraph.Create;
end;

function THefestoJobGraph.AddNode(
  const AId: string;
  const AProc: TProc): IHefestoJobGraph;
begin
  Result := Self;
  if AId.Trim.IsEmpty then
    raise EHefestoJobGraph.Create('Graph node ID cannot be empty.');
  if not Assigned(AProc) then
    raise EHefestoJobGraph.Create('Graph node proc cannot be nil.');
  if FNodes.ContainsKey(AId) then
    raise EHefestoGraphDuplicateNode.CreateFmt(
      'Graph node "%s" already exists.', [AId]);

  FNodes.Add(AId, TGraphNode.Create(AId, AProc));
  FOrder.Add(AId);
  FLastNodeId := AId;
end;

function THefestoJobGraph.DependsOn(const AParentId: string): IHefestoJobGraph;
begin
  Result := Self;
  if FLastNodeId.IsEmpty then
    raise EHefestoJobGraph.Create(
      'DependsOn must be called after AddNode.');
  if AParentId.Trim.IsEmpty then
    raise EHefestoJobGraph.Create('Parent node ID cannot be empty.');
  FNodes[FLastNodeId].Parents.Add(AParentId);
end;

function THefestoJobGraph.Parallel: IHefestoJobGraph;
begin
  FParallel := True;
  Result := Self;
end;

function THefestoJobGraph.OnNodeFailed(
  const ACallback: THefestoGraphNodeFailedProc): IHefestoJobGraph;
begin
  Result := Self;
  FOnNodeFailed := ACallback;
end;

function THefestoJobGraph.OnNodeCancelled(
  const ACallback: THefestoGraphNodeCancelledProc): IHefestoJobGraph;
begin
  Result := Self;
  FOnNodeCancelled := ACallback;
end;

function THefestoJobGraph.Node(const AId: string): IHefestoJobNode;
var
  LNode: TGraphNode;
begin
  if FNodes.TryGetValue(AId, LNode) then
    Result := TNodeSnapshot.Create(AId, LNode.Status)
  else
    Result := nil;
end;

procedure THefestoJobGraph.ValidateDependencies;
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
        raise EHefestoGraphUnknownDependency.CreateFmt(
          'Node "%s" depends on "%s" which does not exist in the graph.',
          [LNodeId, LParent]);
  end;
end;

function THefestoJobGraph.ComputeTopologicalOrder: TArray<string>;
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
      raise EHefestoGraphCycle.Create(
        'Job graph contains a cycle. Review DependsOn declarations.');

    Result := LResult.ToArray;
  finally
    LResult.Free;
    LQueue.Free;
    LChildren.Free;
    LInDegree.Free;
  end;
end;

procedure THefestoJobGraph.ExecuteSequential(const AOrder: TArray<string>);
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

procedure THefestoJobGraph.ExecuteWaves;
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
      raise EHefestoGraphCycle.Create(
        'Job graph contains a cycle. Review DependsOn declarations.');
  finally
    LWave.Free;
    LQueue.Free;
    LChildren.Free;
    LInDegree.Free;
  end;
end;

function THefestoJobGraph.Execute: Boolean;
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
