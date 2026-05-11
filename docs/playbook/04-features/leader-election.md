# Leader Election

Garante que apenas um processo (entre múltiplos rodando em paralelo) execute determinadas tarefas exclusivas — como scheduled jobs, manutenção periódica ou tarefas singleton.

## Configuração

```pascal
uses
  Sidekiq4D.Server,
  Sidekiq4D.Locking,
  Sidekiq4D.Store.Redis4D;

var
  LServer: ISidekiqServer;
begin
  LServer := TSidekiqServer.New
    .UseQueue(TMyAdapter.New)
    .LockProvider(
      TSidekiqRedis4DLockProvider.New
        .ConnectionString('redis://localhost:6379')
    )
    .LeaderName('meu-cluster')          // nome do grupo de election
    .LeaderLeaseTtlSeconds(30)          // TTL do lease
    .UseLeaderElection                  // ativa o mecanismo
    .RegisterHandler('cron_job', TCronHandler.Create)
    .Run;
end;
```

## Verificando se é líder

```pascal
if LServer.IsLeader then
  ExecutarTarefaExclusiva;
```

O servidor tenta renovar o lease periodicamente. Se o líder atual falhar, outro processo assume após o TTL expirar.

## Mecanismo interno

1. Ao iniciar, o servidor tenta gravar uma chave de lock no state store via `TryPutIfAbsent`
2. Se bem-sucedido, torna-se líder e renova o lease antes do TTL expirar
3. Se falhar (outro processo já é líder), fica em modo follower e tenta periodicamente
4. Quando o líder para ou o lease expira sem renovação, outro processo vence a próxima tentativa

## Requisitos

**O `LockProvider` deve ser Redis-based para funcionar em múltiplos processos.**

`TSidekiqInMemoryStateStore` garante exclusão apenas dentro do mesmo processo — não funciona em ambientes com múltiplos hosts.

Providers suportados:
- `TSidekiqRedis4DLockProvider` — recomendado para produção
- InMemory — apenas para testes com processo único

## Tarefas exclusivas do líder

Exemplo: apenas o líder executa scheduled jobs

```pascal
TSchedulerHandler = class(TInterfacedObject, ISidekiqJobHandler)
private
  FServer: ISidekiqServer;
public
  procedure Execute(const AJob: ISidekiqJobEnvelope);
end;

procedure TSchedulerHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  if not FServer.IsLeader then
    Exit;  // followers ignoram tarefas exclusivas
  
  ProcessarTarefaExclusiva;
end;
```

## TTL recomendado

| Cenário | TTL sugerido |
|---------|-------------|
| Renovação rápida (baixa latência) | 15–30s |
| Tolerância a falhas de rede | 60s |
| Jobs longos que não devem interromper | 120s+ |

TTL muito curto: risco de falsa expiração durante GC ou pico de CPU. TTL muito longo: delay maior para failover.

Ver receita em [06-receitas/leader-election.md](../06-receitas/leader-election.md).
