# Hefesto — Docker

## Setup recomendado (desenvolvimento local)

O compilador Delphi gera binários **PE para Windows** (Win32/Win64).  
A forma mais simples de desenvolver com Hefesto é rodar o **worker nativamente no Windows** e subir apenas a infraestrutura (Redis, Postgres) via Docker.

```bash
# Sobe Redis + Postgres em background
docker compose up -d

# Só o Redis (caso não precise do Postgres)
docker compose up -d redis
```

O worker Delphi conecta em:

| Serviço  | URL / string                                                      |
|----------|-------------------------------------------------------------------|
| Redis    | `redis://localhost:6379`                                          |
| Postgres | `host=localhost port=5432 dbname=sidekiq4d user=sidekiq4d password=sidekiq4d` |

---

## Opção A — Windows Containers (produção)

Se o seu ambiente de produção suportar **Windows Containers** (Docker Desktop no modo Windows, ou Windows Server com Containers):

```dockerfile
FROM mcr.microsoft.com/windows/servercore:ltsc2022
WORKDIR C:\\app
COPY bin\\Release\\HefestoWorker.exe .
ENTRYPOINT ["HefestoWorker.exe"]
```

```yaml
# docker-compose.override.yml
services:
  sidekiq4d-worker:
    build: .
    environment:
      - SIDEKIQ_REDIS_URL=redis://redis:6379
    depends_on:
      redis:
        condition: service_healthy
```

---

## Opção B — Wine no Linux (apenas dev/testes)

O `Dockerfile` inclui suporte a Wine para quem quiser testar em Linux.  
**Não use em produção** — Wine não é suportado oficialmente e pode apresentar comportamentos inesperados com código multithreaded.

```bash
# Build da imagem com Wine
docker build -t sidekiq4d-worker .

# Roda o container (precisa do binário em bin/Release/)
docker run --rm \
  -e SIDEKIQ_REDIS_URL=redis://redis:6379 \
  --network host \
  sidekiq4d-worker
```

---

## Parando e limpando

```bash
docker compose down          # para containers
docker compose down -v       # remove containers + volumes
```

---

## Variáveis de ambiente

| Variável               | Padrão                  | Descrição                        |
|------------------------|-------------------------|----------------------------------|
| `SIDEKIQ_REDIS_URL`    | `redis://redis:6379`    | URL de conexão Redis             |
| `SIDEKIQ_CONCURRENCY`  | `4`                     | Workers paralelos                |

> Leia as variáveis no código Delphi com `GetEnvironmentVariable('NOME')`.
