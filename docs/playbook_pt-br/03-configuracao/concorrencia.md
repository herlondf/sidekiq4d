# Concorrência e Comportamento do Worker Pool

## Parâmetros principais

### Concurrency

```pascal
.Concurrency(N)
```

Número de threads de worker rodando em paralelo. Cada worker tem seu próprio contexto de execução.

- **Padrão:** 1 (sequencial)
- **CPU-bound:** use `Concurrency = núcleos de CPU`
- **I/O-bound:** pode usar `Concurrency > núcleos` (ex: 8–16 para jobs que esperam rede/banco)
- **Handlers não são thread-safe por padrão:** se o handler acessa estado compartilhado, proteja com `TCriticalSection`

### BatchSize

```pascal
.BatchSize(N)
```

Quantos jobs o servidor tenta buscar da fila por ciclo de fetch.

- **Padrão:** 1
- Aumentar reduz round-trips ao broker, útil para filas de alto volume
- Deve ser compatível com o que o adapter suporta (alguns adapters retornam 1 por vez independente do BatchSize)

### IdleDelayMs

```pascal
.IdleDelayMs(N)
```

Tempo de espera em milissegundos quando a fila está vazia antes de tentar buscar novamente.

- **Padrão:** 500ms
- Aumentar reduz carga no broker em sistemas de baixo volume
- Diminuir reduz latência de processar novos jobs

### StopWhenIdle

```pascal
.StopWhenIdle
```

Para o servidor automaticamente quando a fila esvazia. Útil para processamento em batch programado:

```pascal
LServer := THefestoServer.New
  .UseQueue(TMyAdapter.New)
  .Concurrency(4)
  .StopWhenIdle
  .RegisterHandler('process', TMyHandler.Create)
  .Run;

// Enfileirar jobs...
EnqueueJobs(LServer);

// Aguardar processamento completo
LServer.WaitForIdle;
```

## Dimensionamento recomendado

| Tipo de job | Concurrency sugerido |
|-------------|---------------------|
| CPU-bound (cálculo, compressão) | = número de núcleos |
| I/O-bound (banco, HTTP externo) | 2× a 4× número de núcleos |
| Misto | Testar com 4–8, ajustar por métricas |
| Processamento sequencial obrigatório | 1 |

## Thread-safety dos handlers

O framework garante que cada job é processado por exatamente uma thread ao mesmo tempo. Mas múltiplas instâncias do handler podem executar em paralelo quando `Concurrency > 1`.

Se seu handler mantém estado entre execuções (ex: cache, conexão de banco), proteja:

```pascal
TMyHandler = class(TInterfacedObject, IHefestoJobHandler)
private
  FLock: TCriticalSection;
  FCache: TDictionary<string, string>;
public
  constructor Create;
  destructor Destroy; override;
  // ...
end;

constructor TMyHandler.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FCache := TDictionary<string, string>.Create;
end;
```

Ver detalhes em [thread-safety.md](../05-operacao-e-runtime/thread-safety.md).
