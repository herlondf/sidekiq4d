# Retry and Dead Letter Queue

## Available policies

### THefestoSimpleRetryPolicy

Fixed delay between attempts.

```pascal
THefestoSimpleRetryPolicy.New(
  MaxAttempts,   // maximum number of attempts
  DelaySeconds   // seconds to wait between attempts
)
```

Example: 3 attempts with 30-second intervals:
```pascal
.RetryPolicy(THefestoSimpleRetryPolicy.New(3, 30))
```

### THefestoExponentialRetryPolicy

Delay grows with the square of the attempt number.

```pascal
THefestoExponentialRetryPolicy.New(
  MaxAttempts,      // maximum number of attempts
  BaseSeconds,      // base for the exponential calculation
  MaxDelaySeconds   // maximum delay ceiling
)
```

**Formula:** `delay = BaseSeconds × attempt²`

| Attempt | Base=15s | Base=15s (with 3600s ceiling) |
|---------|----------|-------------------------------|
| 1 | 15s | 15s |
| 2 | 60s | 60s |
| 3 | 135s | 135s |
| 4 | 240s | 240s |
| 5 | 375s | 375s |
| 10 | 1500s | 1500s |
| 20 | 6000s | 3600s (ceiling) |

Recommended production configuration:
```pascal
.RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
```

## Dead Letter Queue (DLQ)

When a job exhausts all retry attempts, the server calls `MoveToDeadLetter` on the queue adapter. The behavior depends on the adapter:

- **InMemory**: jobs are discarded (no DLQ persistence)
- **Redis Streams**: moved to a separate stream
- **RabbitMQ**: routed to the dead letter exchange
- **SQS**: moved to the DLQ queue configured in AWS

To inspect and reprocess jobs from the DLQ, use the [Web Dashboard](../04-features/dashboard-web.md) (`DELETE /api/dlq` reprocesses).

## Attempt semantics

- The first execution counts as attempt 1
- `MaxAttempts = 5` means: 1 original execution + 4 retries
- The job is tagged with the attempt number in the envelope (`AJob.Attempt`)
- After the last attempt, `MoveToDeadLetter` is called and the job is no longer automatically reprocessed

## No retry

To disable retry (failures go directly to DLQ):
```pascal
// Do not configure .RetryPolicy — default is no retry
// OR use MaxAttempts = 1:
.RetryPolicy(THefestoSimpleRetryPolicy.New(1, 0))
```

## Retry telemetry

The `JobRetried` and `JobDiscarded` events are fired automatically. See [telemetria.md](../05-operations-and-runtime/telemetria.md).
