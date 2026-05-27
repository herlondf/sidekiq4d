# Contributing to Hefesto

## Scope

Hefesto aims to be a Delphi background job processing framework inspired by Ruby's Sidekiq, focused on:

- **Adapter-based queue backends** — Redis, SQS, and others via `IHefestoQueueAdapter`; swap brokers without touching worker code
- **Fluent configuration API** — method chaining returning `Self` for readable, composable setup
- **Thread-safe worker pool** — job dispatch and state tracking protected with `TCriticalSection` / `TInterlocked`
- **Pluggable middleware** — server-side pipeline via `IHefestoServerMiddleware` for logging, retries, and metrics
- **Pluggable state store** — persist job state, retries, and history via `IHefestoStateStore`

## Technical guidelines

- All queue backends must implement `IHefestoQueueAdapter` (5 methods: `Enqueue`, `Dequeue`, `Acknowledge`, `Reject`, `Peek`).
- All server middleware must implement `IHefestoServerMiddleware` (1 method: `Call`).
- State persistence must implement `IHefestoStateStore`.
- Factories use `class function New` — callers must never instantiate concrete classes directly.
- Fluent API methods must return `Self` to support chaining.
- All `class var` or shared state accessed from multiple threads must be protected with `TCriticalSection` or `TInterlocked`.
- `try/finally` is mandatory whenever an object is allocated and must be freed.
- No empty `except` blocks. Log or re-raise.
- Unit names follow the convention `Hefesto.<Module>.pas` (e.g., `Hefesto.Queue.Redis.pas`).

## Suggested flow

1. Open an issue describing the bug, feature, or new adapter before starting work on major changes.
2. Branch from `main`.
3. Add or adjust tests in `tests/` — unit tests require no external broker; integration/smoke tests require Redis or SQS (spin up with `docker-compose` in `docker/`).
4. New adapters go in `src/adapters/`; new middleware in `src/middleware/`; new state stores in `src/store/`.
5. If adding a new sample, place it in `samples/NN-name/` with its own `.dpr` / `.dproj`.
6. Update the playbook in `docs/playbook/` when the change affects usage, options, or observable behavior.
7. Open a pull request with an objective description of the problem and solution.

## Minimum validation

Always validate before opening a PR:

- Build the test suite:
  ```
  msbuild tests\Hefesto.Tests.dproj /p:Config=Release /p:Platform=Win32
  ```
- Run unit tests (no broker required).
- Run integration/smoke tests when the change touches a queue adapter, state store, or the dispatch loop — requires `docker-compose up` in `docker/`.

## Adding a new queue adapter

1. Create `src/adapters/Hefesto.Queue.<Backend>.pas`.
2. Implement all 5 methods of `IHefestoQueueAdapter`: `Enqueue`, `Dequeue`, `Acknowledge`, `Reject`, `Peek`.
3. Expose via `class function New` — no direct instantiation by callers.
4. Add integration tests under `tests/` (can be skipped if the broker is unavailable in CI, but must pass locally).
5. Add a sample in `samples/` and document it in `docs/playbook/`.

## Adding a new middleware

1. Create `src/middleware/Hefesto.Middleware.<Name>.pas`.
2. Implement `IHefestoServerMiddleware` — a single `Call` method that receives the job context and a `next` continuation.
3. Add unit tests covering the middleware behavior in isolation.
4. Document configuration and order-of-execution notes in `docs/playbook/`.

## Adding a new state store

1. Create `src/store/Hefesto.Store.<Backend>.pas`.
2. Implement `IHefestoStateStore`.
3. Add tests covering persistence, retry counters, and history retrieval.

## Conventions

- Public documentation (`playbook/`) in English; `playbook_pt-br/` is the direct translation — keep them in sync.
- Code and identifiers follow the current project style (prefix `THefesto`, `IHefesto`, `EHefesto`).
- Commit messages in pt-BR, format `type(scope): short description` (Conventional Commits).

> 🇧🇷 Leia em português: [CONTRIBUTING_pt-br.md](./CONTRIBUTING_pt-br.md)
