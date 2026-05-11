# O que é o Sidekiq4D

Sidekiq4D é um framework Delphi para processamento assíncrono de jobs, inspirado no Sidekiq do ecossistema Ruby. Resolve o problema de executar trabalho pesado fora do ciclo request/response sem depender de um broker externo obrigatório.

## Motivação

Aplicações Delphi que precisam de processamento assíncrono geralmente recorrem a threads manuais, timers ou filas proprietárias de ORMs. Sidekiq4D oferece uma camada de abstração completa:

- API fluente e declarativa
- Plugável: troque de broker sem mudar a lógica de negócio
- Produção-ready: retry, dead letter, idempotência, rate limiting, leader election
- Observável: telemetria com OTLP/Jaeger, Prometheus, dashboard web

## O que não é

- Não é um message broker — usa brokers existentes via adapters
- Não substitui FireDAC ou outros ORMs para persistência de dados
- Não tem interface gráfica nativa no Delphi IDE (o dashboard é web)

## Comparação rápida

| Critério | Thread manual | Timer + DB | Sidekiq4D |
|----------|--------------|------------|-----------|
| Retry automático | Não | Manual | Sim |
| Dead letter queue | Não | Manual | Sim |
| Troca de broker | N/A | Difícil | Plugável |
| Idempotência | Manual | Manual | Built-in |
| Observabilidade | Nenhuma | Logs | OTLP, Prometheus |
| Concorrência configurável | Manual | Manual | `.Concurrency(N)` |

## Requisitos

- Delphi 11 Alexandria ou superior
- Windows 10/11
- Sem dependência obrigatória de broker externo (InMemory basta para começar)

Brokers externos (Redis, RabbitMQ, Kafka, SQS, Azure, Google Pub/Sub) são opcionais e ativados via adapters específicos.
