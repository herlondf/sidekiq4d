# Receita: Leader Election

Garantir que apenas uma instância em um cluster execute tarefas exclusivas.

```pascal
program LeaderElection;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Threading,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.RedisStreams,
  Sidekiq4D.Store.Redis4D,
  Sidekiq4D.Locking,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry.Console;

const
  REDIS_URL = 'redis://localhost:6379';

type
  TManutencaoHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    FServer: ISidekiqServer;
  public
    constructor Create(const AServer: ISidekiqServer);
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

  TJobNormalHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

constructor TManutencaoHandler.Create(const AServer: ISidekiqServer);
begin
  inherited Create;
  FServer := AServer;
end;

function TManutencaoHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'manutencao';
end;

procedure TManutencaoHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  // Apenas o líder executa manutenção
  if not FServer.IsLeader then
  begin
    Writeln('Não sou o líder. Ignorando tarefa de manutenção.');
    Exit;
  end;

  Writeln('SOU O LÍDER. Executando manutenção...');
  // limpeza de logs, compactação, relatórios, etc.
end;

function TJobNormalHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'processar';
end;

procedure TJobNormalHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  // Todos os workers processam jobs normais
  Writeln('Processando: ', AJob.Body);
end;

var
  LServer: ISidekiqServer;
begin
  // LServer é declarado antes de ser passado ao handler
  LServer := TSidekiqServer.New
    .UseQueue(
      TSidekiqRedisStreamsAdapter.New
        .ConnectionString(REDIS_URL)
        .StreamName('sidekiq4d:jobs')
        .ConsumerGroup('cluster-workers')
        .ConsumerName('worker-' + IntToStr(GetCurrentProcessId))
    )
    .Concurrency(4)
    .StateStore(
      TSidekiqRedis4DStateStore.New
        .ConnectionString(REDIS_URL)
    )
    .LockProvider(
      TSidekiqRedis4DLockProvider.New
        .ConnectionString(REDIS_URL)
    )
    .LeaderName('meu-cluster')
    .LeaderLeaseTtlSeconds(30)
    .UseLeaderElection
    .RetryPolicy(TSidekiqExponentialRetryPolicy.New(3, 10, 60))
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('manutencao', TManutencaoHandler.Create(LServer))
    .RegisterHandler('processar',  TJobNormalHandler.Create)
    .Run;

  Writeln(Format('Worker PID=%d iniciado. É líder? %s',
    [GetCurrentProcessId, BoolToStr(LServer.IsLeader, True)]));

  ReadLn;
  LServer.Stop;
end.
```

**Simulando failover:**
1. Iniciar este programa em 2 terminais diferentes
2. Apenas um será líder
3. Fechar o líder — após 30s (TTL), o outro assume

**Iniciando Redis para teste:**
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

Ver [leader-election.md](../04-features/leader-election.md) para detalhes do mecanismo.
