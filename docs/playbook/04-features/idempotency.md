# Idempotency

Prevents the same job from being processed more than once, even if it is enqueued multiple times or the broker delivers duplicates (at-least-once delivery).

## Available implementations

### THefestoStateStoreIdempotency

Marks jobs as processed with a permanent key in the state store. Does not expire.

```pascal
THefestoStateStoreIdempotency.New(LStore)
```

Behavior: if the key already exists in the store, the job is silently discarded.

### THefestoRenewableIdempotency

Same as above, but with a TTL. After expiration, the same job can be processed again.

```pascal
THefestoRenewableIdempotency.New(
  LStore,      // IHefestoStateStore
  TTLSeconds   // time in seconds until the key expires
)
```

Useful when the same job can be reprocessed periodically (e.g. daily import with the same IDs).

## IHefestoIdempotency interface

```pascal
IHefestoIdempotency = interface
  function TryBegin(const AKey: string): Boolean;
  procedure MarkCompleted(const AKey: string);
  function Exists(const AKey: string): Boolean;
end;
```

- `TryBegin(key)` → `True` if the job can proceed (key did not exist), `False` if it is a duplicate
- `MarkCompleted(key)` → records the key after successful execution
- `Exists(key)` → checks without marking (useful for diagnostics)

## Configuring on the server

```pascal
var LStore := THefestoRedis4DStateStore.New
  .ConnectionString('redis://localhost:6379');

THefestoServer.New
  .UseQueue(TMyAdapter.New)
  .StateStore(LStore)
  .Idempotency(THefestoStateStoreIdempotency.New(LStore))
  ...
```

The server automatically uses the configured `IHefestoIdempotency` before dispatching the job to the handler.

## Idempotency key

By default, the key is derived from the `JobId` of the envelope. To customize the key (e.g. use a field from the payload):

```pascal
// The handler can implement IHefestoIdempotencyKeyProvider
// to return a custom key based on the payload
```

## Limitation with InMemory

`THefestoInMemoryStateStore` loses state on process restart. For persistent idempotency across restarts, use Redis or PostgreSQL.

## Interaction with retry

When a job fails and is retried, `TryBegin` is called again. Since the key was not yet marked with `MarkCompleted` (it failed before), the retry is allowed — correct behavior.

See recipe in [06-recipes/idempotencia.md](../06-recipes/idempotencia.md).
