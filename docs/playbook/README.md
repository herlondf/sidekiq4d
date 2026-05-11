# Sidekiq4D — Playbook

Guia de referência técnica do framework Sidekiq4D para processamento assíncrono de jobs em Delphi.

## Seções

| # | Seção | O que cobre |
|---|-------|-------------|
| 01 | [Visão Geral](01-visao-geral/README.md) | O que é, arquitetura em camadas, como navegar |
| 02 | [Adapters](02-adapters/README.md) | Queue adapters, state stores, middlewares |
| 03 | [Configuração](03-configuracao/README.md) | API fluente do servidor, retry, concorrência |
| 04 | [Features](04-features/README.md) | Scheduled, Batch, Job Graph, Idempotency, Rate Limiting, Leader, Outbox, Dashboard |
| 05 | [Operação e Runtime](05-operacao-e-runtime/README.md) | Thread-safety, telemetria, testes, troubleshooting |
| 06 | [Receitas](06-receitas/README.md) | Código completo pronto para usar |
| 07 | [Limites e Roadmap](07-limites-e-roadmap/README.md) | Limites atuais e direções de evolução |

## Leitura sugerida

**Começando do zero:** 01 → 03 → 06/servidor-basico.md  
**Integrando broker externo:** 02 → 06/servidor-com-redis.md ou 06/servidor-com-sqs.md  
**Confiabilidade em produção:** 03/retry-e-dlq.md → 04/idempotency.md → 04/leader-election.md  
**Observabilidade:** 05/telemetria.md → 06/telemetria-otlp.md → 04/dashboard-web.md  
**Workflows complexos:** 04/batch-jobs.md → 04/job-graph.md  

## Repositório

```
src/              Units core
src/adapters/     Adapters plugáveis
examples/         25 exemplos executáveis
tests/            DUnitX + thread-safety + smoke Redis
docker/           docker-compose (Redis, Postgres, Jaeger)
```

Requisito mínimo: **Delphi 11 Alexandria**. Delphi 12 Athens recomendado.
