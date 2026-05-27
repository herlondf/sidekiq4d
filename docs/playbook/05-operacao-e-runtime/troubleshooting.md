# Troubleshooting

## Handler não é chamado

**Sintoma:** job enfileirado mas nada acontece, sem erro.

**Causa mais comum:** `CanHandle` retorna `False` para a action do job.

**Diagnóstico:**
```pascal
// Verificar se o handler responde pela action
Assert(TMyHandler.Create.CanHandle('minha_action'));

// Verificar se a action do job bate com a string registrada
// RegisterHandler('minha_action', ...) deve bater com AJob.Action
```

**Solução:** Garantir que a string passada em `.RegisterHandler('action', ...)` e a string em `CanHandle` são idênticas (case-sensitive).

---

## Job processado mais de uma vez

**Sintoma:** handler executa múltiplas vezes para o mesmo job.

**Causa:** broker com at-least-once delivery (RabbitMQ, SQS, Kafka) e sem idempotência configurada.

**Solução:** ativar idempotência:
```pascal
.Idempotency(THefestoStateStoreIdempotency.New(LStore))
```

Ver [idempotency.md](../04-features/idempotency.md).

---

## Fila HTTP Ingress cheia (jobs rejeitados)

**Sintoma:** HTTP Ingress retorna erro ou timeout ao receber novos jobs.

**Causa:** `TThreadedQueue<IHefestoJobEnvelope>` interno chegou à capacidade máxima.

**Solução:** aumentar capacidade e/ou `PushTimeoutMs` no construtor do adapter, e/ou aumentar `.Concurrency(N)` para processar mais rápido.

---

## Leader election não funciona em múltiplos hosts

**Sintoma:** todos os processos se consideram líderes, ou nenhum assume a liderança.

**Causa:** `LockProvider` configurado com InMemory — não compartilha estado entre processos.

**Solução:** usar `THefestoRedis4DLockProvider`:
```pascal
.LockProvider(
  THefestoRedis4DLockProvider.New
    .ConnectionString('redis://host:6379')
)
.UseLeaderElection
```

---

## Traces não aparecem no Jaeger

**Sintoma:** servidor enviando OTLP mas Jaeger não mostra spans.

**Causas e soluções:**

1. **Timezone errado nos timestamps:** verificar uso de `DateTimeToUnix(Now, False)` (False = converter de local para UTC)

2. **Content-Type incorreto:** o POST OTLP deve usar `Content-Type: application/json` para o endpoint HTTP

3. **Endpoint errado:** verificar que o endpoint aponta para a porta OTLP HTTP (4318), não a UI (16686) ou gRPC (4317)

4. **Jaeger não iniciado:** `docker-compose up -d jaeger`

---

## Scheduled jobs não disparam

**Sintoma:** jobs agendados com `Schedule()` nunca executam.

**Causas e soluções:**

1. **ScheduledStore não configurado:** o servidor precisa de um `IHefestoScheduledStore` ativo
2. **Scheduler não rodando:** verificar configuração do servidor
3. **Timezone:** o `DueAt` usa horário local da máquina; verificar se o horário do servidor está correto
4. **PopDue retorna vazio:** o `DueAt` pode estar no futuro relativo ao horário da máquina

**Diagnóstico:**
```pascal
var LList := LScheduledStore.List;
for var Entry in LList do
  Writeln(Format('Action=%s DueAt=%s Now=%s',
    [Entry.Action, DateTimeToStr(Entry.DueAt), DateTimeToStr(Now)]));
```

---

## Memory leaks ao fechar

**Sintoma:** FastMM reporta leaks de objetos Hefesto.

**Causas comuns:**

1. Handler não implementa `TInterfacedObject` (sem contagem de referência)
2. Adapter criado com `New` mas sem atribuição a interface (referência perdida)
3. `LServer.Stop` não chamado antes do fim do programa

**Solução:** sempre atribuir adapters e stores a variáveis de interface:
```pascal
var
  LStore: IHefestoStateStore;  // interface, não objeto
  LServer: IHefestoServer;     // interface, não objeto
begin
  LStore := THefestoInMemoryStateStore.New;
  LServer := THefestoServer.New...
  // ao sair do bloco, interfaces são liberadas automaticamente
end;
```

---

## Jobs não chegam ao handler após restart

**Sintoma:** jobs que estavam na fila antes do restart não são processados.

**Causa:** `THefestoInMemoryQueueAdapter` — dados em memória são perdidos ao reiniciar.

**Solução:** usar adapter com persistência (Redis, SQS, RabbitMQ) para jobs que precisam sobreviver a restarts.
