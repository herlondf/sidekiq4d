# Como Navegar no Repositório

## Layout de diretórios

```
Hefesto/
├── src/                    Units core (~35 units)
│   ├── Hefesto.*.pas     Core engine e features
│   ├── adapters/           Adapters plugáveis (~35 units)
│   │   ├── Hefesto.Queue.*.pas
│   │   ├── Hefesto.Store.*.pas
│   │   ├── Hefesto.Middleware.*.pas
│   │   └── Hefesto.Telemetry.*.pas
│   └── vendor/             Synapse (bundled, sem instalação)
├── examples/               25 exemplos com .dpr próprio
├── tests/
│   ├── Hefesto.UnitTests.dpr        11 fixtures DUnitX
│   ├── Hefesto.ThreadSafety.Tests.dpr
│   └── Hefesto.Redis4D.RealSmoke.dpr
├── docker/
│   └── docker-compose.yml  Redis, Postgres, Jaeger
├── docs/
│   └── playbook/           Este playbook
├── CLAUDE.md               Guia de contribuição
└── AGENTS.md               Aliases do delphi-build
```

## Como encontrar o que precisa

**Preciso de um adapter para X:**
→ `src/adapters/Hefesto.Queue.<X>.pas` ou `Hefesto.Store.<X>.pas`

**Preciso entender uma feature:**
→ `src/Hefesto.<Feature>.pas` + exemplo em `examples/<Feature>/`

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
| `Hefesto.Queue.*` | Queue adapters |
| `Hefesto.Store.*` | State stores |
| `Hefesto.Middleware.*` | Middlewares de servidor |
| `Hefesto.Telemetry.*` | Providers de telemetria |
| `Hefesto.<Feature>` | Features do core |

Convenção de nomes de classes: `THefesto<Nome>`, interfaces `IHefesto<Nome>`, exceções `EHefesto<Nome>`.
