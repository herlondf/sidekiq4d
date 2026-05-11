# Idempotência

Previne que o mesmo job seja processado mais de uma vez, mesmo que seja enfileirado múltiplas vezes ou que o broker entregue duplicatas (at-least-once delivery).

## Implementações disponíveis

### TSidekiqStateStoreIdempotency

Marca jobs como processados com chave permanente no state store. Não expira.

```pascal
TSidekiqStateStoreIdempotency.New(LStore)
```

Comportamento: se a chave já existe no store, o job é descartado silenciosamente.

### TSidekiqRenewableIdempotency

Igual ao anterior, mas com TTL. Após expirar, o mesmo job pode ser processado novamente.

```pascal
TSidekiqRenewableIdempotency.New(
  LStore,      // ISidekiqStateStore
  TTLSeconds   // tempo em segundos até a chave expirar
)
```

Útil quando o mesmo job pode ser reprocessado periodicamente (ex: importação diária com mesmos IDs).

## Interface ISidekiqIdempotency

```pascal
ISidekiqIdempotency = interface
  function TryBegin(const AKey: string): Boolean;
  procedure MarkCompleted(const AKey: string);
  function Exists(const AKey: string): Boolean;
end;
```

- `TryBegin(key)` → `True` se o job pode prosseguir (chave não existia), `False` se é duplicata
- `MarkCompleted(key)` → grava a chave após execução bem-sucedida
- `Exists(key)` → verifica sem marcar (útil para diagnóstico)

## Configurando no servidor

```pascal
var LStore := TSidekiqRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');

TSidekiqServer.New
  .UseQueue(TMyAdapter.New)
  .StateStore(LStore)
  .Idempotency(TSidekiqStateStoreIdempotency.New(LStore))
  ...
```

O servidor usa automaticamente o `ISidekiqIdempotency` configurado antes de despachar o job para o handler.

## Chave de idempotência

Por padrão, a chave é derivada do `JobId` do envelope. Para customizar a chave (ex: usar um campo do payload):

```pascal
// O handler pode implementar ISidekiqIdempotencyKeyProvider
// para retornar uma chave personalizada baseada no payload
```

## Limitação com InMemory

`TSidekiqInMemoryStateStore` perde estado ao reiniciar o processo. Para idempotência persistente entre restarts, use Redis ou PostgreSQL.

## Interação com retry

Quando um job falha e é retentado, o `TryBegin` é chamado novamente. Como a chave ainda não foi marcada com `MarkCompleted` (falhou antes), o retry é permitido — comportamento correto.

Ver receita em [06-receitas/idempotencia.md](../06-receitas/idempotencia.md).
