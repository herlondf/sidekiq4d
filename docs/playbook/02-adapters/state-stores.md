# State Stores

Implement `IHefestoStateStore` (`src/Hefesto.Store.Interfaces.pas`). Used by features such as idempotency, rate limiting, leader election, and scheduled jobs.

## Table

| Store | Unit | External dependency | Persistence |
|-------|------|--------------------|----|
| `THefestoInMemoryStateStore` | `Hefesto.Store.InMemory` | None | No |
| `THefestoRedis4DStateStore` | `Hefesto.Store.Redis4D` | Redis4D | Yes |
| `THefestoRedisSentinelStateStore` | `Hefesto.Store.RedisSentinel` | Redis Sentinel | Yes |
| `THefestoPostgreSQLStateStore` | `Hefesto.Store.PostgreSQL` | FireDAC + PostgreSQL | Yes |
| `THefestoMongoDBStateStore` | `Hefesto.Store.MongoDB` | MongoDB Atlas HTTP API | Yes |
| `THefestoFireDACStateStore` | `Hefesto.Store.FireDAC` | FireDAC (any database) | Yes |

## When to use each

**InMemory** — tests, development, single-process. No persistence. Ideal for isolating DUnitX fixtures.

**Redis4D** — standard production. High performance, native TTL, atomic operations. Required for distributed leader election.

**Redis Sentinel** — Redis in high availability with automatic failover. Same capabilities as Redis4D with added resilience.

**PostgreSQL** — when Redis is not in the infrastructure but PostgreSQL is. Lower performance than Redis for high-frequency operations.

**MongoDB** — infrastructure with MongoDB Atlas. Uses HTTP API (no native driver).

**FireDAC** — generic for any database supported by FireDAC (SQLite, Oracle, MySQL, Interbase). SQLite is a lightweight option for single-process with persistence.

## IHefestoStateStore interface

```pascal
IHefestoStateStore = interface
  function Get(const AKey: string): string;
  procedure Put(const AKey, AValue: string);
  procedure Delete(const AKey: string);
  function TryPutIfAbsent(const AKey, AValue: string): Boolean;
  function Keys(const APrefix: string): TArray<string>;
end;
```

- `TryPutIfAbsent` is the atomic operation used by idempotency and leader election — must be atomic in the backend (Redis: SET NX, PostgreSQL: INSERT ON CONFLICT)
- `Keys(prefix)` returns all keys with the given prefix — used for listing scheduled jobs and metrics

## Note on leader election

Distributed leader election requires atomic `TryPutIfAbsent` across multiple processes. `THefestoInMemoryStateStore` only works for leader election within the same process. For multiple hosts, use Redis4D or PostgreSQL.

## How to implement a custom store

See [CLAUDE.md](../../CLAUDE.md) — section "Adding a State Store".
