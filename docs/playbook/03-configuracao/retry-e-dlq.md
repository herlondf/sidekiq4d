# Retry e Dead Letter Queue

## Políticas disponíveis

### THefestoSimpleRetryPolicy

Delay fixo entre tentativas.

```pascal
THefestoSimpleRetryPolicy.New(
  MaxAttempts,   // número máximo de tentativas
  DelaySeconds   // segundos de espera entre tentativas
)
```

Exemplo: 3 tentativas com 30 segundos de intervalo:
```pascal
.RetryPolicy(THefestoSimpleRetryPolicy.New(3, 30))
```

### THefestoExponentialRetryPolicy

Delay cresce com o quadrado do número da tentativa.

```pascal
THefestoExponentialRetryPolicy.New(
  MaxAttempts,      // número máximo de tentativas
  BaseSeconds,      // base do cálculo exponencial
  MaxDelaySeconds   // teto máximo do delay
)
```

**Fórmula:** `delay = BaseSeconds × tentativa²`

| Tentativa | Base=15s | Base=15s (com teto 3600s) |
|-----------|----------|--------------------------|
| 1 | 15s | 15s |
| 2 | 60s | 60s |
| 3 | 135s | 135s |
| 4 | 240s | 240s |
| 5 | 375s | 375s |
| 10 | 1500s | 1500s |
| 20 | 6000s | 3600s (teto) |

Configuração recomendada para produção:
```pascal
.RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
```

## Dead Letter Queue (DLQ)

Quando um job esgota todas as tentativas de retry, o servidor chama `MoveToDeadLetter` no queue adapter. O comportamento depende do adapter:

- **InMemory**: jobs descartados (sem persistência de DLQ)
- **Redis Streams**: movido para stream separado
- **RabbitMQ**: roteado para dead letter exchange
- **SQS**: movido para fila DLQ configurada no AWS

Para inspecionar e reprocessar jobs da DLQ, use o [Dashboard Web](../04-features/dashboard-web.md) (`DELETE /api/dlq` reprocessa).

## Semântica de tentativas

- A primeira execução conta como tentativa 1
- `MaxAttempts = 5` significa: 1 execução original + 4 retentativas
- O job é marcado com o número de tentativas no envelope (`AJob.Attempt`)
- Após a última tentativa, `MoveToDeadLetter` é chamado e o job não é mais reprocessado automaticamente

## Sem retry

Para desabilitar retry (falhas vão direto para DLQ):
```pascal
// Não configurar .RetryPolicy — padrão é sem retry
// OU usar MaxAttempts = 1:
.RetryPolicy(THefestoSimpleRetryPolicy.New(1, 0))
```

## Telemetria de retry

Os eventos `JobRetried` e `JobDiscarded` são disparados automaticamente. Ver [telemetria.md](../05-operacao-e-runtime/telemetria.md).
