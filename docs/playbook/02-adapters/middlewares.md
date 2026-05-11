# Middlewares

Implementam `ISidekiqServerMiddleware` (`src/Sidekiq4D.Middleware.pas`). Executados em cadeia para cada job antes do handler.

## Tabela

| Middleware | Unit | O que faz |
|-----------|------|-----------|
| `TSidekiqCircuitBreakerMiddleware` | `Sidekiq4D.Middleware.CircuitBreaker` | Abre o circuito após N falhas consecutivas, bloqueia jobs temporariamente |
| `TSidekiqCompressionMiddleware` | `Sidekiq4D.Middleware.Compression` | Descomprime payload ZLib antes do handler |
| `TSidekiqDeduplicationMiddleware` | `Sidekiq4D.Middleware.Deduplication` | Descarta jobs duplicados com base em chave hash do payload |
| `TSidekiqLoggingMiddleware` | `Sidekiq4D.Middleware.Logging` | Loga início e fim de cada job em JSON estruturado |
| `TSidekiqPrometheusMiddleware` | `Sidekiq4D.Middleware.Prometheus` | Incrementa contadores Prometheus por queue e status |
| `TSidekiqTimeoutMiddleware` | `Sidekiq4D.Middleware.Timeout` | Aborta jobs que excedem o tempo máximo configurado |
| `TSidekiqHorseMiddleware` | `Sidekiq4D.Middleware.Horse` | Integração com o framework Horse para contexto HTTP |

## Como encadear

Middlewares são registrados via `.Use(...)` na configuração do servidor. A ordem de registro define a ordem de execução (FIFO):

```pascal
TSidekiqServer.New
  .Use(TSidekiqLoggingMiddleware.New)
  .Use(TSidekiqTimeoutMiddleware.New(30000))       // 30 segundos
  .Use(TSidekiqCircuitBreakerMiddleware.New(5, 60)) // 5 falhas, 60s aberto
  .Use(TSidekiqDeduplicationMiddleware.New(LStore))
  ...
```

Neste exemplo a ordem de execução para cada job é:
```
Logging → Timeout → CircuitBreaker → Deduplication → Handler
```

## Interface ISidekiqServerMiddleware

```pascal
ISidekiqServerMiddleware = interface
  procedure Call(
    const AQueue: string;
    const AJob: TSidekiqJobEnvelope;
    const ANext: TProc);
end;
```

O middleware deve chamar `ANext` para continuar a cadeia. Se não chamar, o job é descartado silenciosamente (útil para deduplicação).

## Exemplo de middleware próprio

```pascal
TMyAuditMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
public
  procedure Call(
    const AQueue: string;
    const AJob: TSidekiqJobEnvelope;
    const ANext: TProc);
end;

procedure TMyAuditMiddleware.Call(
  const AQueue: string;
  const AJob: TSidekiqJobEnvelope;
  const ANext: TProc);
begin
  LogAudit(AQueue, AJob.Action);
  ANext();  // sem isso o job não executa
end;
```

## Como implementar um middleware próprio

Ver [CLAUDE.md](../../CLAUDE.md) — seção "Adicionando um Middleware".
