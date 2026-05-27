# How to Navigate the Repository

## Directory layout

```
Hefesto/
├── src/                    Core units (~35 units)
│   ├── Hefesto.*.pas     Core engine and features
│   ├── adapters/           Pluggable adapters (~35 units)
│   │   ├── Hefesto.Queue.*.pas
│   │   ├── Hefesto.Store.*.pas
│   │   ├── Hefesto.Middleware.*.pas
│   │   └── Hefesto.Telemetry.*.pas
│   └── vendor/             Synapse (bundled, no installation needed)
├── examples/               25 examples with their own .dpr
├── tests/
│   ├── Hefesto.UnitTests.dpr        11 DUnitX fixtures
│   ├── Hefesto.ThreadSafety.Tests.dpr
│   └── Hefesto.Redis4D.RealSmoke.dpr
├── docker/
│   └── docker-compose.yml  Redis, Postgres, Jaeger
├── docs/
│   └── playbook/           This playbook
├── CLAUDE.md               Contribution guide
└── AGENTS.md               delphi-build aliases
```

## How to find what you need

**I need an adapter for X:**
→ `src/adapters/Hefesto.Queue.<X>.pas` or `Hefesto.Store.<X>.pas`

**I need to understand a feature:**
→ `src/Hefesto.<Feature>.pas` + example in `examples/<Feature>/`

**I need working code to get started:**
→ `examples/BasicConsole/` or `examples/InMemory/`

**I need to run tests:**
→ See [testes-e-validacao.md](../05-operations-and-runtime/testes-e-validacao.md)

**I need to add to the Delphi IDE library path:**
→ Add `src/` and `src/adapters/` to the project library path

## Compilation via delphi-build

```
delphi-build sidekiq4delphi-tests          # unit tests
delphi-build sidekiq4delphi-basic-console  # basic example
delphi-build sidekiq4delphi-job-graph      # Job Graph example
```

Full list of aliases in `AGENTS.md`.

## Unit naming convention

| Prefix | Type |
|--------|------|
| `Hefesto.Queue.*` | Queue adapters |
| `Hefesto.Store.*` | State stores |
| `Hefesto.Middleware.*` | Server middlewares |
| `Hefesto.Telemetry.*` | Telemetry providers |
| `Hefesto.<Feature>` | Core features |

Class naming convention: `THefesto<Name>`, interfaces `IHefesto<Name>`, exceptions `EHefesto<Name>`.
