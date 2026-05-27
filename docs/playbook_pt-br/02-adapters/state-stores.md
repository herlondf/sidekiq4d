# State Stores

Implementam `IHefestoStateStore` (`src/Hefesto.Store.Interfaces.pas`). Usados por features como idempotência, rate limiting, leader election e scheduled jobs.

## Tabela

| Store | Unit | Dependência externa | Persistência |
|-------|------|--------------------|----|
| `THefestoInMemoryStateStore` | `Hefesto.Store.InMemory` | Nenhuma | Não |
| `THefestoRedis4DStateStore` | `Hefesto.Store.Redis4D` | Redis4D | Sim |
| `THefestoRedisSentinelStateStore` | `Hefesto.Store.RedisSentinel` | Redis Sentinel | Sim |
| `THefestoPostgreSQLStateStore` | `Hefesto.Store.PostgreSQL` | FireDAC + PostgreSQL | Sim |
| `THefestoMongoDBStateStore` | `Hefesto.Store.MongoDB` | MongoDB Atlas HTTP API | Sim |
| `THefestoFireDACStateStore` | `Hefesto.Store.FireDAC` | FireDAC (qualquer banco) | Sim |

## Quando usar cada um

**InMemory** — testes, desenvolvimento, single-process. Sem persistência. Ideal para isolar fixtures DUnitX.

**Redis4D** — produção padrão. Alta performance, TTL nativo, atomic operations. Necessário para leader election distribuída.

**Redis Sentinel** — Redis em alta disponibilidade com failover automático. Mesmas capacidades do Redis4D com resiliência.

**PostgreSQL** — quando Redis não está na infraestrutura mas PostgreSQL sim. Menor performance que Redis para operações de alta frequência.

**MongoDB** — infraestrutura com MongoDB Atlas. Usa HTTP API (sem driver nativo).

**FireDAC** — genérico para qualquer banco suportado pelo FireDAC (SQLite, Oracle, MySQL, Interbase). SQLite é opção leve para single-process com persistência.

## Interface IHefestoStateStore

```pascal
IHefestoStateStore = interface
  function Get(const AKey: string): string;
  procedure Put(const AKey, AValue: string);
  procedure Delete(const AKey: string);
  function TryPutIfAbsent(const AKey, AValue: string): Boolean;
  function Keys(const APrefix: string): TArray<string>;
end;
```

- `TryPutIfAbsent` é a operação atômica usada por idempotência e leader election — deve ser atômica no backend (Redis: SET NX, PostgreSQL: INSERT ON CONFLICT)
- `Keys(prefix)` retorna todas as chaves com o prefixo dado — usado para listagem de scheduled jobs e métricas

## Nota sobre leader election

Leader election distribuída requer `TryPutIfAbsent` atômica em múltiplos processos. `THefestoInMemoryStateStore` só funciona para leader election dentro do mesmo processo. Para múltiplos hosts, use Redis4D ou PostgreSQL.

## Como implementar um store próprio

Ver [CLAUDE.md](../../CLAUDE.md) — seção "Adicionando um State Store".
