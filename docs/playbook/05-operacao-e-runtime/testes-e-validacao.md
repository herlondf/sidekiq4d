# Testes e Validação

## Suite unitária (sem broker externo)

11 fixtures DUnitX em `tests/Sidekiq4D.UnitTests.dpr`. Todos usam `TSidekiqInMemoryStateStore` e `TSidekiqInMemoryQueueAdapter` — nenhum serviço externo necessário.

### Fixtures cobertas

| Fixture | O que testa |
|---------|-------------|
| Retry (Simple + Exponential) | Contagem de tentativas, delays, escalação |
| DeadLetter | Movimentação para DLQ após esgotamento |
| Scheduled | Agendamento, `PopDue`, listagem, cancelamento |
| Idempotency | `TryBegin`, `MarkCompleted`, duplicatas |
| RateLimit | Token bucket, rejeição por rate, reabastecimento |
| Leader | Eleição, renovação de lease, failover |
| Batch | Callbacks `OnComplete`/`OnSuccess`, contadores |
| Middleware | Encadeamento, `Call`/`ANext`, circuit breaker |
| Periodic | Expressões cron, próxima ocorrência, registro |
| Outbox | `Save`, `Entries`, `Remove`, `Count` |
| Graph | DAG, `DependsOn`, execução paralela, ordem |

### Compilar e rodar

```
delphi-build sidekiq4delphi-tests
tests\Sidekiq4D.UnitTests.Runner.exe
```

Saída esperada: todos os testes verdes, sem warnings de memory leak.

### Padrão de fixture

```pascal
uses
  DUnitX.TestFramework,
  Sidekiq4D.Store.InMemory;

[TestFixture('MinhaFeature')]
TMinhaFixture = class
private
  FStore: ISidekiqStateStore;
public
  [Setup]
  procedure Setup;
  [TearDown]
  procedure TearDown;

  [Test]
  [Category('Unit')]
  procedure Metodo_Cenario_ResultadoEsperado;
end;

procedure TMinhaFixture.Setup;
begin
  FStore := TSidekiqInMemoryStateStore.New;
end;

procedure TMinhaFixture.TearDown;
begin
  FStore := nil;
end;

procedure TMinhaFixture.Metodo_Cenario_ResultadoEsperado;
begin
  Assert.IsTrue(SomCondition);
end;
```

### Adicionando um novo teste

1. Criar `tests/Sidekiq4D.<Feature>.Tests.pas`
2. Adicionar ao runner `tests/Sidekiq4D.UnitTests.dpr`
3. Seguir o padrão de fixture acima

## Stress test de concorrência

Verifica race conditions e deadlocks sob carga.

```
delphi-build sidekiq4delphi-threadsafety
tests\Sidekiq4D.ThreadSafety.Tests.Runner.exe
```

O teste usa múltiplas threads para enfileirar e processar jobs simultaneamente, verificando consistência dos contadores e ausência de exceções de acesso concorrente.

## Smoke test com Redis real

Requer Redis local em `localhost:6379`.

```
delphi-build sidekiq4delphi-redis4d-real-smoke
```

Testa a integração completa com Redis: enfileiramento, fetch, ack, nack, DLQ e scheduled jobs usando o adapter real `TSidekiqRedis4DStateStore`.

## Verificação de memory leaks

Delphi detecta vazamentos automaticamente quando `ReportMemoryLeaksOnShutdown := True` está no `.dpr`:

```pascal
program Sidekiq4D.UnitTests.Runner;

{$APPTYPE CONSOLE}

uses
  // ...
begin
  ReportMemoryLeaksOnShutdown := True;
  // ...
end.
```

Se aparecerem leaks no output, verificar:
- Interfaces sem `TInterfacedObject` (contagem de referência)
- Objetos criados no `Setup` e não liberados no `TearDown`
- Handlers registrados que não implementam `TInterfacedObject`
