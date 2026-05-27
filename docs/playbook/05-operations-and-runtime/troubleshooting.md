# Troubleshooting

## Handler is not called

**Symptom:** job enqueued but nothing happens, no error.

**Most common cause:** `CanHandle` returns `False` for the job's action.

**Diagnosis:**
```pascal
// Check if the handler responds to the action
Assert(TMyHandler.Create.CanHandle('my_action'));

// Check that the job's action matches the registered string
// RegisterHandler('my_action', ...) must match AJob.Action
```

**Solution:** Ensure the string passed in `.RegisterHandler('action', ...)` and the string in `CanHandle` are identical (case-sensitive).

---

## Job processed more than once

**Symptom:** handler executes multiple times for the same job.

**Cause:** broker with at-least-once delivery (RabbitMQ, SQS, Kafka) and no idempotency configured.

**Solution:** enable idempotency:
```pascal
.Idempotency(THefestoStateStoreIdempotency.New(LStore))
```

See [idempotency.md](../04-features/idempotency.md).

---

## HTTP Ingress queue full (jobs rejected)

**Symptom:** HTTP Ingress returns error or timeout when receiving new jobs.

**Cause:** internal `TThreadedQueue<IHefestoJobEnvelope>` reached maximum capacity.

**Solution:** increase capacity and/or `PushTimeoutMs` in the adapter constructor, and/or increase `.Concurrency(N)` to process faster.

---

## Leader election does not work on multiple hosts

**Symptom:** all processes consider themselves leaders, or no process assumes leadership.

**Cause:** `LockProvider` configured with InMemory — does not share state between processes.

**Solution:** use `THefestoRedis4DLockProvider`:
```pascal
.LockProvider(
  THefestoRedis4DLockProvider.New
    .ConnectionString('redis://host:6379')
)
.UseLeaderElection
```

---

## Traces do not appear in Jaeger

**Symptom:** server sending OTLP but Jaeger shows no spans.

**Causes and solutions:**

1. **Wrong timezone in timestamps:** check usage of `DateTimeToUnix(Now, False)` (False = convert from local to UTC)

2. **Incorrect Content-Type:** the OTLP POST must use `Content-Type: application/json` for the HTTP endpoint

3. **Wrong endpoint:** verify the endpoint points to the OTLP HTTP port (4318), not the UI (16686) or gRPC (4317)

4. **Jaeger not started:** `docker-compose up -d jaeger`

---

## Scheduled jobs do not fire

**Symptom:** jobs scheduled with `Schedule()` never execute.

**Causes and solutions:**

1. **ScheduledStore not configured:** the server needs an active `IHefestoScheduledStore`
2. **Scheduler not running:** check server configuration
3. **Timezone:** `DueAt` uses the machine's local time; verify the server time is correct
4. **PopDue returns empty:** `DueAt` may be in the future relative to the machine's time

**Diagnosis:**
```pascal
var LList := LScheduledStore.List;
for var Entry in LList do
  Writeln(Format('Action=%s DueAt=%s Now=%s',
    [Entry.Action, DateTimeToStr(Entry.DueAt), DateTimeToStr(Now)]));
```

---

## Memory leaks on shutdown

**Symptom:** FastMM reports leaks of Hefesto objects.

**Common causes:**

1. Handler does not implement `TInterfacedObject` (no reference counting)
2. Adapter created with `New` but not assigned to an interface (reference lost)
3. `LServer.Stop` not called before the program ends

**Solution:** always assign adapters and stores to interface variables:
```pascal
var
  LStore: IHefestoStateStore;  // interface, not object
  LServer: IHefestoServer;     // interface, not object
begin
  LStore := THefestoInMemoryStateStore.New;
  LServer := THefestoServer.New...
  // on block exit, interfaces are released automatically
end;
```

---

## Jobs do not arrive at handler after restart

**Symptom:** jobs that were in the queue before the restart are not processed.

**Cause:** `THefestoInMemoryQueueAdapter` — in-memory data is lost on restart.

**Solution:** use an adapter with persistence (Redis, SQS, RabbitMQ) for jobs that need to survive restarts.
