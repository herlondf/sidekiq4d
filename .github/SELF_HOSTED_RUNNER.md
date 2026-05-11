# Self-Hosted Runner Setup — Sidekiq4D CI

## Pre-requisitos na maquina

1. **RAD Studio / Delphi 11+** instalado
2. **Python 3.10+** no PATH
3. **delphi-build** framework configurado em `D:\IA\Framework\delphi-build`
4. **Git** no PATH
5. **Aliases** registrados no `delphi-build/config/config.json`:
   - `sidekiq4delphi-tests`
   - `sidekiq4delphi-basic-console`
   - `sidekiq4delphi-scheduled`
   - `sidekiq4delphi-batch`
   - `sidekiq4delphi-retry`
   - (demais aliases para release completa)

## Instalar o runner

```powershell
# 1. Criar diretorio
mkdir C:\actions-runner && cd C:\actions-runner

# 2. Baixar runner (verificar versao mais recente)
Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v2.322.0/actions-runner-win-x64-2.322.0.zip -OutFile runner.zip
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory("$PWD\runner.zip", "$PWD")

# 3. Configurar (obter token em Settings > Actions > Runners > New)
.\config.cmd --url https://github.com/herlondf/sidekiq4d --token YOUR_TOKEN

# 4. Instalar como servico Windows
.\svc.cmd install
.\svc.cmd start
```

## Verificar

```powershell
# O runner deve aparecer em:
# https://github.com/herlondf/sidekiq4d/settings/actions/runners
```

## Workflows

| Workflow | Trigger | O que faz |
|----------|---------|-----------|
| `ci.yml` | Push/PR em main | Build + testes + lint |
| `release.yml` | Tag `v*` | Build + testes + package + GitHub Release |
| `redis-smoke.yml` | Push em units Redis4D | Smoke real contra Redis local |
