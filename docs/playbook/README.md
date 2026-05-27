# Hefesto — Playbook

Technical reference guide for the Hefesto framework for asynchronous job processing in Delphi.

## Sections

| # | Section | What it covers |
|---|---------|----------------|
| 01 | [Overview](01-overview/README.md) | What it is, layered architecture, how to navigate |
| 02 | [Adapters](02-adapters/README.md) | Queue adapters, state stores, middlewares |
| 03 | [Configuration](03-configuration/README.md) | Fluent server API, retry, concurrency |
| 04 | [Features](04-features/README.md) | Scheduled, Batch, Job Graph, Idempotency, Rate Limiting, Leader, Outbox, Dashboard |
| 05 | [Operations and Runtime](05-operations-and-runtime/README.md) | Thread-safety, telemetry, testing, troubleshooting |
| 06 | [Recipes](06-recipes/README.md) | Complete ready-to-use code |
| 07 | [Limits and Roadmap](07-limits-and-roadmap/README.md) | Current limits and evolution directions |

## Suggested reading paths

**Starting from scratch:** 01 → 03 → 06/servidor-basico.md  
**Integrating an external broker:** 02 → 06/servidor-com-redis.md or 06/servidor-com-sqs.md  
**Production reliability:** 03/retry-e-dlq.md → 04/idempotency.md → 04/leader-election.md  
**Observability:** 05/telemetria.md → 06/telemetria-otlp.md → 04/dashboard-web.md  
**Complex workflows:** 04/batch-jobs.md → 04/job-graph.md  

## Repository

```
src/              Core units
src/adapters/     Pluggable adapters
examples/         25 runnable examples
tests/            DUnitX + thread-safety + Redis smoke
docker/           docker-compose (Redis, Postgres, Jaeger)
```

Minimum requirement: **Delphi 11 Alexandria**. Delphi 12 Athens recommended.
