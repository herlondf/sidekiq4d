# Batch Jobs

Processamento de múltiplos jobs como uma unidade lógica, com callbacks executados quando o lote completa ou é bem-sucedido.

## Criando um batch

```pascal
uses
  Hefesto.Batch,
  Hefesto.Store.InMemory;

var
  LStore: IHefestoStateStore;
  LBatchStore: IHefestoBatchStore;
begin
  LStore := THefestoInMemoryStateStore.New;
  LBatchStore := THefestoStateStoreBatchStore.New(LStore);

  THefestoBatch.New(LBatchStore)
    .OnComplete(procedure
      begin
        Writeln('Batch concluído (sucesso ou falha)');
      end)
    .OnSuccess(procedure
      begin
        Writeln('Todos os jobs do batch foram bem-sucedidos');
      end)
    .Add('process_item', '{"id": 1}')
    .Add('process_item', '{"id": 2}')
    .Add('process_item', '{"id": 3}')
    .Commit;
end;
```

## Semântica dos callbacks

| Callback | Quando dispara |
|----------|---------------|
| `OnComplete` | Quando todos os jobs terminam (qualquer status) |
| `OnSuccess` | Somente quando todos os jobs terminam sem falha |

- Se algum job falha e é movido para DLQ, `OnSuccess` não dispara
- `OnComplete` sempre dispara ao final do batch, independente de falhas
- Callbacks são executados no thread do último worker que concluir

## Adicionando jobs dinamicamente

```pascal
var
  LBatch: IHefestoBatch;
begin
  LBatch := THefestoBatch.New(LBatchStore)
    .OnComplete(procedure begin ... end);

  // Adicionar jobs de forma condicional
  for var Item in FItems do
    if Item.NeedsProcessing then
      LBatch.Add('process_item', TJson.Serialize(Item));

  LBatch.Commit;
end;
```

`Commit` finaliza o batch e libera os jobs para execução. Não adicione jobs após `Commit`.

## Rastreando o progresso

O batch store mantém contadores de jobs pendentes, concluídos e falhados. Para inspecionar:

```pascal
var LStatus := LBatchStore.GetStatus(BatchId);
Writeln(Format('Pendentes: %d, OK: %d, Falhos: %d',
  [LStatus.Pending, LStatus.Succeeded, LStatus.Failed]));
```

## Batch com StateStore externo (Redis)

Para batches que sobrevivem a restarts do processo:

```pascal
LStore := THefestoRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');
LBatchStore := THefestoStateStoreBatchStore.New(LStore);
```

Ver receita completa em [06-receitas/batch-com-callback.md](../06-receitas/batch-com-callback.md).
