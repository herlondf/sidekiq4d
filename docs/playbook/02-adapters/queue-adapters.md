# Queue Adapters

Implement `IHefestoQueueAdapter` (`src/Hefesto.Queue.Interfaces.pas`).

## Table

| Adapter | Unit | External dependency | Protocol |
|---------|------|--------------------|-----------| 
| `THefestoInMemoryQueueAdapter` | `Hefesto.Queue.InMemory` | None | In-process |
| `THefestoSQSQueueAdapter` | `Hefesto.Queue.SQS` | AWS (HTTP + SigV4) | HTTPS REST |
| `THefestoRabbitMQQueueAdapter` | `Hefesto.Queue.RabbitMQ` | RabbitMQ Management API | HTTPS REST |
| `THefestoKafkaQueueAdapter` | `Hefesto.Queue.Kafka` | Confluent REST Proxy | HTTPS REST |
| `THefestoAzureServiceBusAdapter` | `Hefesto.Queue.AzureServiceBus` | Azure Service Bus | HTTPS REST |
| `THefestoGooglePubSubAdapter` | `Hefesto.Queue.GooglePubSub` | Google Cloud PubSub | HTTPS REST |
| `THefestoRedisStreamsAdapter` | `Hefesto.Queue.RedisStreams` | Redis4D + XREADGROUP | TCP |
| `THefestoHTTPIngressAdapter` | `Hefesto.Queue.HTTPIngress` | Indy (bundled) | HTTP POST |
| `THefestoTCPIngressAdapter` | `Hefesto.Queue.TCPIngress` | Indy/Synapse (bundled) | TCP |

## When to use each

**InMemory** — development, unit tests, prototypes. Data is lost on process restart. Does not share state between processes.

**Redis Streams** — production with Redis already in the infrastructure. Persistence, consumer groups, replay. Requires Redis4D.

**SQS** — AWS infrastructure. Managed, scalable, no broker maintenance. Higher latency than Redis.

**RabbitMQ** — advanced routing with exchanges, bindings, and native dead letter. Requires RabbitMQ Management API enabled.

**Kafka** — high volume, event retention, multiple independent consumers. Requires Confluent REST Proxy (does not connect directly to the Kafka broker).

**Azure Service Bus** — Azure infrastructure. Sessions, queues with lock, managed dead letter.

**Google Pub/Sub** — GCP infrastructure. Push and pull, managed subscriptions.

**HTTP Ingress** — receive jobs via HTTP POST from other systems. Useful for webhooks and integrations without a broker.

**TCP Ingress** — receive jobs via TCP from internal systems with low latency.

## IHefestoQueueAdapter interface

```pascal
IHefestoQueueAdapter = interface
  function Name: string;
  function Fetch(out AJob: IHefestoJobEnvelope): Boolean;
  procedure Ack(const AJob: IHefestoJobEnvelope);
  procedure Nack(const AJob: IHefestoJobEnvelope);
  procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope);
end;
```

- `Fetch` should be blocking with a timeout or return `False` immediately if the queue is empty
- `Ack` confirms successful processing
- `Nack` returns the job to the queue (retry)
- `MoveToDeadLetter` moves to DLQ after all attempts are exhausted

## How to implement a custom adapter

See [CLAUDE.md](../../CLAUDE.md) — section "Adding a Queue Adapter".
