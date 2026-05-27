# What is Hefesto

Hefesto is a Delphi framework for asynchronous job processing, inspired by Sidekiq from the Ruby ecosystem. It solves the problem of executing heavy work outside the request/response cycle without requiring a mandatory external broker.

## Motivation

Delphi applications that need asynchronous processing typically resort to manual threads, timers, or ORM-proprietary queues. Hefesto provides a complete abstraction layer:

- Fluent and declarative API
- Pluggable: swap brokers without changing business logic
- Production-ready: retry, dead letter, idempotency, rate limiting, leader election
- Observable: telemetry with OTLP/Jaeger, Prometheus, web dashboard

## What it is not

- Not a message broker — it uses existing brokers via adapters
- Does not replace FireDAC or other ORMs for data persistence
- Does not have a native GUI in the Delphi IDE (the dashboard is web-based)

## Quick comparison

| Criterion | Manual Thread | Timer + DB | Hefesto |
|-----------|--------------|------------|---------|
| Automatic retry | No | Manual | Yes |
| Dead letter queue | No | Manual | Yes |
| Broker swap | N/A | Difficult | Pluggable |
| Idempotency | Manual | Manual | Built-in |
| Observability | None | Logs | OTLP, Prometheus |
| Configurable concurrency | Manual | Manual | `.Concurrency(N)` |

## Requirements

- Delphi 11 Alexandria or higher
- Windows 10/11
- No mandatory external broker dependency (InMemory is sufficient to get started)

External brokers (Redis, RabbitMQ, Kafka, SQS, Azure, Google Pub/Sub) are optional and activated via specific adapters.
