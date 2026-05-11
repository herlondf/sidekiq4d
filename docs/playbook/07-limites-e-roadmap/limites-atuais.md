# Limites Atuais

## Plataforma

- **Windows only:** build e testes exigem Windows 10/11. Não há suporte a Linux ou macOS mesmo com Delphi 12 (FireMonkey não é o foco; Synapse é bundled mas não testado em Linux)
- **Delphi 11+ obrigatório:** não compatível com versões anteriores por uso de generics modernos e inline vars

## Broker e protocolo

- **Kafka via REST Proxy apenas:** o adapter Kafka usa o Confluent REST Proxy HTTP, não o protocolo nativo Kafka (librdkafka). Isso adiciona latência e um hop extra, mas elimina dependência de DLL nativa
- **RabbitMQ via Management API:** usa HTTP REST, não AMQP direto. Funciona mas com latência maior que um cliente AMQP nativo
- **Sem suporte a AMQP, gRPC ou Kafka nativo:** todos os adapters externos usam HTTP REST

## Processamento

- **Uma fila por servidor:** cada instância de `TSidekiqServer` processa uma única fila. Para múltiplas filas com prioridades diferentes, são necessárias múltiplas instâncias de servidor
- **Sem prioridade de jobs dentro da fila:** os jobs são processados na ordem que o broker os entrega (FIFO para InMemory e Redis Streams, sem garantia para SQS)
- **Sem circuit breaker de fila:** se o broker cair, o servidor tenta fetch continuamente com `IdleDelayMs` de espera

## Scheduled Jobs

- **Scheduler depende do processo estar ativo:** não há scheduler externo persistente. Se o servidor parar, jobs agendados para o período de inatividade são processados com atraso quando o servidor voltar (dependendo da implementação do store)
- **Resolução mínima de cron: 1 minuto:** expressões com segundos não são suportadas

## State Store e persistência

- **InMemory não persiste entre restarts:** jobs, idempotência, rate limiting e batches são perdidos se o processo reiniciar
- **MongoDB via HTTP Atlas:** não suporta MongoDB standalone sem Atlas (usa a HTTP Data API do Atlas)
- **Sem sharding automático:** o state store não distribui dados entre múltiplas instâncias de Redis ou banco

## Dashboard e observabilidade

- **Dashboard sem autenticação:** o web dashboard não tem mecanismo de autenticação nativo. Em produção, use um reverse proxy (Nginx, Caddy) para proteger o acesso
- **SSE sem reconexão automática no servidor:** se a conexão SSE cair, o cliente precisa reconectar manualmente
- **Métricas históricas em memória:** `TSidekiqHistoricalMetricsTelemetry` perde dados ao reiniciar; não persiste em disco ou banco

## Testes

- **Smoke test Redis requer localhost:6379:** não é configurável via variável de ambiente no estado atual
- **Sem testes de integração para todos os adapters:** apenas InMemory e Redis têm cobertura de teste automatizada

## Limitações de design conhecidas

- **Handlers são stateful se não cuidado:** o framework não cria novas instâncias do handler por job; se o handler tiver estado mutável, precisa de sincronização manual
- **Sem cancelamento de job em execução:** uma vez que o handler começa a executar, não há mecanismo para interrompê-lo (além do middleware `Timeout` que pode abortar)
- **Dead letter sem replay automático:** jobs na DLQ precisam de ação manual (via dashboard ou API) para serem reprocessados
