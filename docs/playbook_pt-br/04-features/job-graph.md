# Job Graph (DAG)

Executa jobs com dependências declaradas, formando um grafo acíclico dirigido (DAG). Nós sem dependências pendentes executam em paralelo.

## Conceito

```
extract ──► transform ──► load
```

Neste exemplo, `transform` só inicia após `extract` concluir, e `load` só após `transform`. Com `.Parallel`, nós independentes executam ao mesmo tempo.

## API

```pascal
uses
  Hefesto.Graph;

THefestoJobGraph.New
  .Node('extract',   TExtractJob.Create)
  .Node('transform', TTransformJob.Create).DependsOn('extract')
  .Node('load',      TLoadJob.Create).DependsOn('transform')
  .Parallel                    // nós sem dependências pendentes rodam em paralelo
  .Execute(LQueue, LServer);
```

## Exemplo com múltiplos ramos paralelos

```
          ┌─► normalize_a ─┐
ingest ───┤                ├──► merge ──► export
          └─► normalize_b ─┘
```

```pascal
THefestoJobGraph.New
  .Node('ingest',      TIngestJob.Create)
  .Node('normalize_a', TNormalizeAJob.Create).DependsOn('ingest')
  .Node('normalize_b', TNormalizeBJob.Create).DependsOn('ingest')
  .Node('merge',       TMergeJob.Create)
    .DependsOn('normalize_a')
    .DependsOn('normalize_b')
  .Node('export',      TExportJob.Create).DependsOn('merge')
  .Parallel
  .Execute(LQueue, LServer);
```

`normalize_a` e `normalize_b` executam em paralelo após `ingest`. `merge` aguarda ambos.

## Referência de métodos

| Método | Descrição |
|--------|-----------|
| `.Node(name, handler)` | Declara um nó do grafo |
| `.DependsOn(nodeName)` | Adiciona dependência ao último nó declarado |
| `.Parallel` | Habilita execução paralela de nós independentes |
| `.Execute(queue, server)` | Inicia a execução do grafo |

## Handlers de nó

Cada nó usa um `IHefestoJobHandler` normal. O grafo gerencia a orquestração:

```pascal
TExtractJob = class(TInterfacedObject, IHefestoJobHandler)
public
  function CanHandle(const AAction: string): Boolean;
  procedure Execute(const AJob: IHefestoJobEnvelope);
end;

function TExtractJob.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'extract';
end;

procedure TExtractJob.Execute(const AJob: IHefestoJobEnvelope);
begin
  // extrai dados e pode gravar resultado para uso nos nós seguintes
  // use StateStore para passar dados entre nós, se necessário
end;
```

## Passagem de dados entre nós

O grafo não passa dados entre nós automaticamente. Padrões recomendados:

1. **StateStore compartilhado:** cada nó grava/lê do store com chaves combinando o ID do grafo
2. **Banco de dados:** nós gravam resultados em tabelas; nós seguintes consultam
3. **Arquivos temporários:** para grandes volumes de dados

## Sem `.Parallel`

Sem `.Parallel`, o grafo executa os nós em sequência na ordem de declaração. Dependências ainda são respeitadas, mas não há paralelismo mesmo que o grafo permita.

Ver receita em [06-receitas/job-graph-dag.md](../06-receitas/job-graph-dag.md).
