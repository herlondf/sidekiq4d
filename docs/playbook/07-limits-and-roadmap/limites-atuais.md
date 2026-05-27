# Current Limits

## Platform

- **Windows only:** build and tests require Windows 10/11. No support for Linux or macOS even with Delphi 12 (FireMonkey is not the focus; Synapse is bundled but not tested on Linux)
- **Delphi 11+ required:** not compatible with earlier versions due to use of modern generics and inline vars

## Broker and protocol

- **Kafka via REST Proxy only:** the Kafka adapter uses the Confluent REST Proxy HTTP, not the native Kafka protocol (librdkafka). This adds latency and an extra hop, but eliminates the native DLL dependency
- **RabbitMQ via Management API:** uses HTTP REST, not direct AMQP. Works but with higher latency than a native AMQP client
- **No AMQP, gRPC, or native Kafka support:** all external adapters use HTTP REST

## Processing

- **One queue per server:** each `THefestoServer` instance processes a single queue. For multiple queues with different priorities, multiple server instances are required
- **No job priority within the queue:** jobs are processed in the order the broker delivers them (FIFO for InMemory and Redis Streams, no guarantee for SQS)
- **No queue circuit breaker:** if the broker goes down, the server continuously attempts fetch with `IdleDelayMs` delay

## Scheduled Jobs

- **Scheduler depends on the process being active:** there is no external persistent scheduler. If the server stops, jobs scheduled for the downtime period are processed with a delay when the server returns (depending on the store implementation)
- **Minimum cron resolution: 1 minute:** expressions with seconds are not supported

## State Store and persistence

- **InMemory does not persist across restarts:** jobs, idempotency, rate limiting, and batches are lost if the process restarts
- **MongoDB via HTTP Atlas:** does not support standalone MongoDB without Atlas (uses the Atlas HTTP Data API)
- **No automatic sharding:** the state store does not distribute data across multiple Redis or database instances

## Dashboard and observability

- **Dashboard without authentication:** the web dashboard has no native authentication mechanism. In production, use a reverse proxy (Nginx, Caddy) to protect access
- **SSE without automatic server-side reconnection:** if the SSE connection drops, the client must reconnect manually
- **Historical metrics in memory:** `THefestoHistoricalMetricsTelemetry` loses data on restart; does not persist to disk or database

## Testing

- **Redis smoke test requires localhost:6379:** not configurable via environment variable in the current state
- **No integration tests for all adapters:** only InMemory and Redis have automated test coverage

## Known design limitations

- **Handlers are stateful if not careful:** the framework does not create new handler instances per job; if the handler has mutable state, manual synchronization is required
- **No in-flight job cancellation:** once the handler starts executing, there is no mechanism to interrupt it (except the `Timeout` middleware which can abort)
- **Dead letter without automatic replay:** jobs in the DLQ require manual action (via dashboard or API) to be reprocessed
