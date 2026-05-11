# Rate Limiting

Controla a taxa de processamento de jobs para proteger recursos downstream (APIs externas, bancos de dados, serviços terceiros).

## Implementações disponíveis

### TSidekiqNoopRateLimiter

Não limita nada. Comportamento padrão quando nenhum rate limiter é configurado.

```pascal
TSidekiqNoopRateLimiter.New
```

### TSidekiqTokenBucketRateLimiter

Algoritmo token bucket: um bucket começa cheio e é drenado a cada job. Reabastece continuamente até o limite.

```pascal
TSidekiqTokenBucketRateLimiter.New(
  LStore,           // ISidekiqStateStore (persiste estado do bucket)
  BucketSize,       // capacidade máxima do bucket (burst máximo)
  RefillPerSecond   // tokens adicionados por segundo
)
```

Exemplo: permite no máximo 10 jobs por segundo com burst de 50:
```pascal
TSidekiqTokenBucketRateLimiter.New(LStore, 50, 10)
```

## Interface ISidekiqRateLimiter

```pascal
ISidekiqRateLimiter = interface
  function TryAcquire(const AKey: string; ACost: Integer): Boolean;
end;
```

- `AKey` — identifica o bucket (ex: `'api_calls'`, `'user:42'`, `'queue:emails'`)
- `ACost` — quantos tokens este job consome (normalmente 1)
- Retorna `True` se pode prosseguir, `False` se deve aguardar

## Rate limiting por recurso

Usando chaves diferentes é possível ter buckets independentes:

```pascal
// No handler ou middleware
if not FRateLimiter.TryAcquire('external_api', 1) then
  raise ESidekiqRateLimitExceeded.Create('Rate limit atingido');
```

## Configurando como middleware

O rate limiter pode ser usado diretamente no handler ou encapsulado em um middleware customizado:

```pascal
TRateLimitedHandler = class(TInterfacedObject, ISidekiqJobHandler)
private
  FRateLimiter: ISidekiqRateLimiter;
public
  constructor Create(const ARateLimiter: ISidekiqRateLimiter);
  procedure Execute(const AJob: ISidekiqJobEnvelope);
end;

procedure TRateLimitedHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  if not FRateLimiter.TryAcquire('my_resource', 1) then
  begin
    // Rejeitar para retry posterior
    raise ESidekiqRateLimitExceeded.Create('Rate limit');
  end;
  // processar job
end;
```

## Persistência do estado do bucket

O `TSidekiqTokenBucketRateLimiter` usa o `ISidekiqStateStore` para persistir o estado do bucket. Com `InMemoryStateStore`, o bucket é reiniciado ao reiniciar o processo. Com Redis, persiste entre restarts.

Ver receita em [06-receitas/rate-limiting.md](../06-receitas/rate-limiting.md).
