# ── Sidekiq4D Worker — Dockerfile ────────────────────────────────────────
#
# ATENÇÃO: O compilador Delphi (DCC32/DCC64) gera binários PE para
# Windows (Win32 / Win64). Não há compilador Delphi oficial para Linux.
#
# Este Dockerfile serve para dois cenários:
#
#   Opção A — Windows Containers (recomendado para produção)
#     Requer Docker Desktop no modo "Windows containers".
#     O binário roda nativamente sem Wine ou emulação.
#     Use a imagem mcr.microsoft.com/windows/servercore:ltsc2022.
#
#   Opção B — Wine no Linux (desenvolvimento/testes)
#     Permite rodar o binário .exe em Linux via Wine.
#     NÃO recomendado para produção.

# ── Opção A: Windows Container ───────────────────────────────────────────
# FROM mcr.microsoft.com/windows/servercore:ltsc2022
# WORKDIR C:\\app
# COPY bin\\Release\\SidekiqWorker.exe .
# ENTRYPOINT ["SidekiqWorker.exe"]

# ── Opção B: Wine no Linux ────────────────────────────────────────────────
FROM scottyhardy/docker-wine:latest

LABEL maintainer="herlonfilgueira"
LABEL description="Sidekiq4D worker (Wine/Linux — dev/test only)"

# Variáveis de ambiente configuradas pelo caller (docker compose / k8s)
ENV SIDEKIQ_REDIS_URL=redis://redis:6379
ENV SIDEKIQ_CONCURRENCY=4
ENV WINE_DISPLAY_MANAGER=off

WORKDIR /app

# Copie o binário compilado pelo Delphi para bin/Release/
COPY bin/Release/SidekiqWorker.exe .

# Porta do dashboard web (padrão 9999)
EXPOSE 9999

ENTRYPOINT ["wine", "SidekiqWorker.exe"]
