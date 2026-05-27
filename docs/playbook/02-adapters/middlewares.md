# Middlewares

Implementam `IHefestoServerMiddleware` (`src/Hefesto.Middleware.pas`). Executados em cadeia para cada job antes do handler.

## Tabela

| Middleware | Unit | O que faz |
|-----------|------|-----------|
| `THefestoCircuitBreakerMiddleware` | `Hefesto.Middleware.CircuitBreaker` | Abre o circuito após N falhas consecutivas, bloqueia jobs temporariamente |
| `THefestoCompressionMiddleware` | `Hefesto.Middleware.Compression` | Descomprime payload ZLib antes do handler |
| `THefestoDeduplicationMiddleware` | `Hefesto.Middleware.Deduplication` | Descarta jobs duplicados com base em chave hash do payload |
| `THefestoLoggingMiddleware` | `Hefesto.Middleware.Logging` | Loga início e fim de cada job em JSON estruturado |
| `THefestoPrometheusMiddleware` | `Hefesto.Middleware.Prometheus` | Incrementa contadores Prometheus por queue e status |
| `THefestoTimeoutMiddleware` | `Hefesto.Middleware.Timeout` | Aborta jobs que excedem o tempo máximo configurado |
| `THefestoHorseMiddleware` | `Hefesto.Middleware.Horse` | Integração com o framework Horse para contexto HTTP |

## Como encadear

Middlewares são registrados via `.Use(...)` na configuração do servidor. A ordem de registro define a ordem de execução (FIFO):

```pascal
THefestoServer.New
  .Use(THefestoLoggingMiddleware.New)
  .Use(THefestoTimeoutMiddleware.New(30000))       // 30 segundos
  .Use(THefestoCircuitBreakerMiddleware.New(5, 60)) // 5 falhas, 60s aberto
  .Use(THefestoDeduplicationMiddleware.New(LStore))
  ...
```

Neste exemplo a ordem de execução para cada job é:
```
Logging → Timeout → CircuitBreaker → Deduplication → Handler
```

## Interface IHefestoServerMiddleware

```pascal
IHefestoServerMiddleware = interface
  procedure Call(
    const AQueue: string;
    const AJob: THefestoJobEnvelope;
    const ANext: TProc);
end;
```

O middleware deve chamar `ANext` para continuar a cadeia. Se não chamar, o job é descartado silenciosamente (útil para deduplicação).

## Exemplo de middleware próprio

```pascal
TMyAuditMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
public
  procedure Call(
    const AQueue: string;
    const AJob: THefestoJobEnvelope;
    const ANext: TProc);
end;

procedure TMyAuditMiddleware.Call(
  const AQueue: string;
  const AJob: THefestoJobEnvelope;
  const ANext: TProc);
begin
  LogAudit(AQueue, AJob.Action);
  ANext();  // sem isso o job não executa
end;
```

## Como implementar um middleware próprio

Ver [CLAUDE.md](../../CLAUDE.md) — seção "Adicionando um Middleware".
