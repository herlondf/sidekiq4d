# Thread-Safety

## Modelo de threading

O servidor Sidekiq4D usa um pool de threads fixo definido por `.Concurrency(N)`. Cada worker thread opera de forma independente:

- **Fetch:** cada worker faz seu próprio fetch da fila (sem competição explícita — o adapter deve ser thread-safe)
- **Execução:** cada job é executado em exatamente uma thread ao mesmo tempo
- **Handlers:** podem ter múltiplas instâncias executando em paralelo quando `Concurrency > 1`

## Garantias do framework

| O que o framework garante | O que você é responsável |
|--------------------------|--------------------------|
| Um job processado por no máximo uma thread | Thread-safety do seu handler |
| Operações de Ack/Nack atômicas no adapter | Thread-safety de estado compartilhado no handler |
| `TCriticalSection` no histórico de métricas | Thread-safety de recursos externos (conexões de banco) |
| `TInterlocked` em contadores internos | Sincronização de caches em handlers |

## Proteção de estado compartilhado

### No handler (estado compartilhado entre execuções)

```pascal
TMyHandler = class(TInterfacedObject, ISidekiqJobHandler)
private
  FLock: TCriticalSection;
  FSharedCache: TDictionary<string, string>;
public
  constructor Create;
  destructor Destroy; override;
  procedure Execute(const AJob: ISidekiqJobEnvelope);
end;

constructor TMyHandler.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FSharedCache := TDictionary<string, string>.Create;
end;

destructor TMyHandler.Destroy;
begin
  FSharedCache.Free;
  FLock.Free;
  inherited;
end;

procedure TMyHandler.Execute(const AJob: ISidekiqJobEnvelope);
var
  LValue: string;
begin
  FLock.Enter;
  try
    if not FSharedCache.TryGetValue(AJob.Body, LValue) then
    begin
      LValue := ComputeExpensive(AJob.Body);
      FSharedCache.Add(AJob.Body, LValue);
    end;
  finally
    FLock.Leave;
  end;
  
  ProcessWithValue(LValue);
end;
```

### Usando TMonitor (alternativa mais leve)

```pascal
procedure TMyHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  TMonitor.Enter(FSharedObject);
  try
    // seção crítica
  finally
    TMonitor.Exit(FSharedObject);
  end;
end;
```

## HTTP Ingress Adapter

O `TSidekiqHTTPIngressAdapter` usa `TThreadedQueue<ISidekiqJobEnvelope>` internamente para receber jobs via HTTP e entregá-los ao worker pool de forma thread-safe.

Se a fila interna encher (burst de requests), novos jobs são rejeitados com timeout. Para aumentar a capacidade:

```pascal
// Capacidade e timeout configuráveis no construtor do adapter
TSidekiqHTTPIngressAdapter.New(
  Port,        // porta HTTP
  Capacity,    // capacidade da fila interna (padrão varia)
  PushTimeoutMs  // timeout de push (ms)
)
```

## Telemetria e métricas históricas

`TSidekiqHistoricalMetricsTelemetry` usa `TCriticalSection` internamente para proteger os buckets de métricas. Sem necessidade de sincronização adicional ao usá-lo.

## Conexões de banco de dados

FireDAC e outros ORMs não são thread-safe por conexão. Padrões recomendados:

1. **Connection pool:** use o pool do FireDAC configurado para múltiplas conexões
2. **Conexão por thread:** criar e destruir conexão dentro de `Execute` (overhead por job)
3. **TThreadLocalStorage:** uma conexão por thread de worker

Prefira o connection pool do FireDAC — é o approach mais eficiente e correto.
