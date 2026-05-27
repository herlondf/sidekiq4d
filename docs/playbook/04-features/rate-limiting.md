# Rate Limiting

Controls the job processing rate to protect downstream resources (external APIs, databases, third-party services).

## Available implementations

### THefestoNoopRateLimiter

Does not limit anything. Default behavior when no rate limiter is configured.

```pascal
THefestoNoopRateLimiter.New
```

### THefestoTokenBucketRateLimiter

Token bucket algorithm: a bucket starts full and is drained with each job. It refills continuously up to the limit.

```pascal
THefestoTokenBucketRateLimiter.New(
  LStore,           // IHefestoStateStore (persists bucket state)
  BucketSize,       // maximum bucket capacity (maximum burst)
  RefillPerSecond   // tokens added per second
)
```

Example: allows at most 10 jobs per second with a burst of 50:
```pascal
THefestoTokenBucketRateLimiter.New(LStore, 50, 10)
```

## IHefestoRateLimiter interface

```pascal
IHefestoRateLimiter = interface
  function TryAcquire(const AKey: string; ACost: Integer): Boolean;
end;
```

- `AKey` — identifies the bucket (e.g. `'api_calls'`, `'user:42'`, `'queue:emails'`)
- `ACost` — how many tokens this job consumes (normally 1)
- Returns `True` if it can proceed, `False` if it should wait

## Rate limiting by resource

Using different keys allows independent buckets:

```pascal
// In the handler or middleware
if not FRateLimiter.TryAcquire('external_api', 1) then
  raise EHefestoRateLimitExceeded.Create('Rate limit reached');
```

## Configuring as middleware

The rate limiter can be used directly in the handler or encapsulated in a custom middleware:

```pascal
TRateLimitedHandler = class(TInterfacedObject, IHefestoJobHandler)
private
  FRateLimiter: IHefestoRateLimiter;
public
  constructor Create(const ARateLimiter: IHefestoRateLimiter);
  procedure Execute(const AJob: IHefestoJobEnvelope);
end;

procedure TRateLimitedHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  if not FRateLimiter.TryAcquire('my_resource', 1) then
  begin
    // Reject for later retry
    raise EHefestoRateLimitExceeded.Create('Rate limit');
  end;
  // process job
end;
```

## Bucket state persistence

`THefestoTokenBucketRateLimiter` uses `IHefestoStateStore` to persist the bucket state. With `InMemoryStateStore`, the bucket is reset on process restart. With Redis, it persists across restarts.

See recipe in [06-recipes/rate-limiting.md](../06-recipes/rate-limiting.md).
