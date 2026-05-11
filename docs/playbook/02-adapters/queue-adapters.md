# Queue Adapters

Implementam `ISidekiqQueueAdapter` (`src/Sidekiq4D.Queue.Interfaces.pas`).

## Tabela

| Adapter | Unit | Dependência externa | Protocolo |
|---------|------|--------------------|-----------| 
| `TSidekiqInMemoryQueueAdapter` | `Sidekiq4D.Queue.InMemory` | Nenhuma | In-process |
| `TSidekiqSQSQueueAdapter` | `Sidekiq4D.Queue.SQS` | AWS (HTTP + SigV4) | HTTPS REST |
| `TSidekiqRabbitMQQueueAdapter` | `Sidekiq4D.Queue.RabbitMQ` | RabbitMQ Management API | HTTPS REST |
| `TSidekiqKafkaQueueAdapter` | `Sidekiq4D.Queue.Kafka` | Confluent REST Proxy | HTTPS REST |
| `TSidekiqAzureServiceBusAdapter` | `Sidekiq4D.Queue.AzureServiceBus` | Azure Service Bus | HTTPS REST |
| `TSidekiqGooglePubSubAdapter` | `Sidekiq4D.Queue.GooglePubSub` | Google Cloud PubSub | HTTPS REST |
| `TSidekiqRedisStreamsAdapter` | `Sidekiq4D.Queue.RedisStreams` | Redis4D + XREADGROUP | TCP |
| `TSidekiqHTTPIngressAdapter` | `Sidekiq4D.Queue.HTTPIngress` | Indy (bundled) | HTTP POST |
| `TSidekiqTCPIngressAdapter` | `Sidekiq4D.Queue.TCPIngress` | Indy/Synapse (bundled) | TCP |

## Quando usar cada um

**InMemory** — desenvolvimento, testes unitários, protótipos. Dados perdidos ao reiniciar o processo. Não compartilha estado entre processos.

**Redis Streams** — produção com Redis já na infraestrutura. Persistência, consumer groups, replay. Requer Redis4D.

**SQS** — infraestrutura AWS. Gerenciado, escalável, sem manutenção de broker. Latência maior que Redis.

**RabbitMQ** — roteamento avançado com exchanges, bindings e dead letter nativa. Requer RabbitMQ Management API habilitado.

**Kafka** — alto volume, retenção de eventos, múltiplos consumers independentes. Requer Confluent REST Proxy (não conecta direto ao broker Kafka).

**Azure Service Bus** — infraestrutura Azure. Sessions, filas com lock, dead letter gerenciada.

**Google Pub/Sub** — infraestrutura GCP. Push e pull, subscriptions gerenciadas.

**HTTP Ingress** — receber jobs via HTTP POST de outros sistemas. Útil para webhooks e integrações sem broker.

**TCP Ingress** — receber jobs via TCP de sistemas internos com baixa latência.

## Interface ISidekiqQueueAdapter

```pascal
ISidekiqQueueAdapter = interface
  function Name: string;
  function Fetch(out AJob: ISidekiqJobEnvelope): Boolean;
  procedure Ack(const AJob: ISidekiqJobEnvelope);
  procedure Nack(const AJob: ISidekiqJobEnvelope);
  procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope);
end;
```

- `Fetch` deve ser bloqueante com timeout ou retornar `False` imediatamente se a fila estiver vazia
- `Ack` confirma o processamento bem-sucedido
- `Nack` devolve o job para a fila (retry)
- `MoveToDeadLetter` move para DLQ após esgotar as tentativas

## Como implementar um adapter próprio

Ver [CLAUDE.md](../../CLAUDE.md) — seção "Adicionando um Queue Adapter".
