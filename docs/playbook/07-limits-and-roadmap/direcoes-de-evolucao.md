# Evolution Directions

Possible directions based on current limits and observed use cases. These are not roadmap commitments.

## Broker and protocol

**Multiple queue support per server**
Allow an `IHefestoServer` to consume from multiple queues with weights or priorities. The dispatcher would select the queue based on configuration.

**Native AMQP adapter for RabbitMQ**
Eliminate the Management HTTP API dependency by using a pure Delphi AMQP client or a C library wrapper.

**Native Kafka adapter**
Direct integration via librdkafka (DLL) or a pure Delphi Kafka protocol implementation, eliminating the Confluent REST Proxy dependency.

## Scheduled Jobs

**External persistent scheduler**
A dedicated scheduling process that survives restarts and ensures jobs scheduled during downtime are executed with the smallest possible delay after the service returns.

**Seconds support in cron expressions**
6-field expressions (`sec min hour day month weekday`) for sub-minute scheduling.

## Reliability

**Automatic adapter reconnection**
Adapters with retry and backoff logic for reconnecting to the broker after a temporary outage.

**Server-level circuit breaker**
Automatically pause fetch when the broker signals overload or unavailability.

**Graceful shutdown with drain**
Wait for all in-flight jobs to complete before stopping, with a configurable timeout and persistence of unstarted jobs back to the queue.

## Observability

**Dashboard authentication**
Basic Auth or configurable token to protect web endpoints without requiring a reverse proxy.

**Historical metrics persistence**
Write metrics to the state store (Redis) for access after restart and temporal correlation.

**Structured logs with levels**
Configurable log level (DEBUG, INFO, WARN, ERROR) integrated into `IHefestoTelemetry`.

## Developer Experience

**Delphi Package (BPL)**
Package the core and adapters as IDE-installable BPLs, simplifying library path configuration.

**Handler generator**
Template wizard or script that creates the scaffold for a new handler, tests, and example with a single command.

**File-based configuration**
Support for `sidekiq4d.json` or `.env` to configure the server without recompilation (useful in containerized environments).

## Platform

**Linux (Delphi 12 + Linux compiler)**
Test and adjust adapters for compilation with the Delphi 12 Linux compiler, replacing Synapse with something more portable where needed.

## Contributing

To implement any of these directions, follow the process in [CLAUDE.md](../../CLAUDE.md):
1. Create the interface unit first
2. Implement with DUnitX tests
3. Add an example in `examples/`
4. Update `AGENTS.md` with the new alias
