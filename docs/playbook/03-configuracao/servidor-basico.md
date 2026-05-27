# Configuração do Servidor

## API fluente — visão geral

```pascal
uses
  Hefesto.Server,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console;

var
  LStore: IHefestoStateStore;
  LServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;

  LServer := THefestoServer.New
    // --- Queue ---
    .UseQueue(THefestoInMemoryQueueAdapter.New)

    // --- Concorrência ---
    .Concurrency(4)        // threads de worker
    .BatchSize(10)         // jobs buscados por ciclo
    .IdleDelayMs(1000)     // ms de espera quando fila vazia

    // --- State store ---
    .StateStore(LStore)

    // --- Idempotência (opcional) ---
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))

    // --- Retry + Dead Letter ---
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))

    // --- Telemetria ---
    .Telemetry(THefestoConsoleTelemetry.New)

    // --- Handlers ---
    .RegisterHandler('send_email', TSendEmailHandler.Create)
    .RegisterHandler('process_report', TProcessReportHandler.Create)

    // --- Iniciar ---
    .Run;

  // ... aguardar sinal de parada ...

  LServer.Stop;
end;
```

## Referência de métodos

| Método | Tipo | Padrão | Descrição |
|--------|------|--------|-----------|
| `.UseQueue(adapter)` | Obrigatório | — | Queue adapter a usar |
| `.Concurrency(N)` | Integer | 1 | Número de workers paralelos |
| `.BatchSize(N)` | Integer | 1 | Jobs buscados por ciclo de fetch |
| `.IdleDelayMs(N)` | Integer | 500 | Pausa (ms) quando fila está vazia |
| `.StateStore(store)` | Opcional | InMemory | State store para features |
| `.LockProvider(lock)` | Opcional | — | Necessário para Leader Election |
| `.Idempotency(impl)` | Opcional | — | Previne reprocessamento |
| `.RetryPolicy(policy)` | Opcional | Sem retry | Política de retry |
| `.Telemetry(tel)` | Opcional | Noop | Provider de telemetria |
| `.Use(middleware)` | Opcional | — | Adiciona middleware (encadeável) |
| `.RegisterHandler(action, handler)` | Obrigatório | — | Mapeia action → handler |
| `.LeaderName(name)` | Opcional | — | Nome do grupo de leader election |
| `.LeaderLeaseTtlSeconds(N)` | Opcional | 30 | TTL do lease de liderança |
| `.UseLeaderElection` | Opcional | — | Ativa leader election |
| `.StopWhenIdle` | Opcional | — | Para o servidor ao esvaziar a fila |
| `.Run` | — | — | Inicia workers (não bloqueia) |
| `.Stop` | — | — | Para workers graciosamente |

## Handler mínimo

```pascal
type
  TSendEmailHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TSendEmailHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'send_email';
end;

procedure TSendEmailHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  // AJob.Body contém o JSON do payload
  // AJob.Action contém a action string
  SendEmail(AJob.Body);
end;
```

## Parando o servidor

`.Run` não bloqueia. Para aguardar indefinidamente até sinal externo:

```pascal
LServer.Run;
ReadLn;  // aguarda Enter no console
LServer.Stop;
```

Para Windows Service, substituir `ReadLn` pelo loop do service.

Ver receita completa em [06-receitas/windows-service.md](../06-receitas/windows-service.md).
