# Como Navegar no Repositório

## Layout de diretórios

```
Sidekiq4D/
├── src/                    Units core (~35 units)
│   ├── Sidekiq4D.*.pas     Core engine e features
│   ├── adapters/           Adapters plugáveis (~35 units)
│   │   ├── Sidekiq4D.Queue.*.pas
│   │   ├── Sidekiq4D.Store.*.pas
│   │   ├── Sidekiq4D.Middleware.*.pas
│   │   └── Sidekiq4D.Telemetry.*.pas
│   └── vendor/             Synapse (bundled, sem instalação)
├── examples/               25 exemplos com .dpr próprio
├── tests/
│   ├── Sidekiq4D.UnitTests.dpr        11 fixtures DUnitX
│   ├── Sidekiq4D.ThreadSafety.Tests.dpr
│   └── Sidekiq4D.Redis4D.RealSmoke.dpr
├── docker/
│   └── docker-compose.yml  Redis, Postgres, Jaeger
├── docs/
│   └── playbook/           Este playbook
├── CLAUDE.md               Guia de contribuição
└── AGENTS.md               Aliases do delphi-build
```

## Como encontrar o que precisa

**Preciso de um adapter para X:**
→ `src/adapters/Sidekiq4D.Queue.<X>.pas` ou `Sidekiq4D.Store.<X>.pas`

**Preciso entender uma feature:**
→ `src/Sidekiq4D.<Feature>.pas` + exemplo em `examples/<Feature>/`

**Preciso de código funcional para começar:**
→ `examples/BasicConsole/` ou `examples/InMemory/`

**Preciso rodar testes:**
→ Ver [testes-e-validacao.md](../05-operacao-e-runtime/testes-e-validacao.md)

**Preciso adicionar ao library path do Delphi IDE:**
→ Adicionar `src/` e `src/adapters/` ao library path do projeto

## Compilação via delphi-build

```
delphi-build sidekiq4delphi-tests          # testes unitários
delphi-build sidekiq4delphi-basic-console  # exemplo básico
delphi-build sidekiq4delphi-job-graph      # exemplo Job Graph
```

Lista completa de aliases em `AGENTS.md`.

## Convenção de nomes de units

| Prefixo | Tipo |
|---------|------|
| `Sidekiq4D.Queue.*` | Queue adapters |
| `Sidekiq4D.Store.*` | State stores |
| `Sidekiq4D.Middleware.*` | Middlewares de servidor |
| `Sidekiq4D.Telemetry.*` | Providers de telemetria |
| `Sidekiq4D.<Feature>` | Features do core |

Convenção de nomes de classes: `TSidekiq<Nome>`, interfaces `ISidekiq<Nome>`, exceções `ESidekiq<Nome>`.
