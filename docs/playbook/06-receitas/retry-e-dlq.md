# Receita: Retry Exponencial e Dead Letter Queue

Servidor com retry exponencial e inspeção de jobs na DLQ.

```pascal
program RetryEDLQ;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

type
  TFalhavelHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FTentativa: Integer;
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TFalhavelHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'falha_simulada';
end;

procedure TFalhavelHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Inc(FTentativa);
  Writeln(Format('Tentativa %d para job %s', [FTentativa, AJob.JobId]));

  // Falha nas primeiras 2 tentativas
  if FTentativa < 3 then
    raise Exception.Create('Erro simulado: serviço indisponível');

  Writeln('Sucesso na tentativa 3!');
end;

var
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(1)
    .StateStore(THefestoInMemoryStateStore.New)
    // 5 tentativas: delay = 15 × n² segundos (máx 3600s)
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('falha_simulada', TFalhavelHandler.Create)
    .Run;

  // Enfileirar job que vai falhar e depois ter sucesso
  LQueue.Enqueue('falha_simulada', '{"id": 1}');

  Writeln('Job enfileirado. Observe as tentativas de retry...');
  ReadLn;
  LServer.Stop;
end.
```

**Delays esperados com THefestoExponentialRetryPolicy.New(5, 15, 3600):**
```
Tentativa 1: execução imediata
Tentativa 2: aguarda 15s  (15 × 1²)
Tentativa 3: aguarda 60s  (15 × 2²)
Tentativa 4: aguarda 135s (15 × 3²)
Tentativa 5: aguarda 240s (15 × 4²)
→ Após 5 tentativas: MoveToDeadLetter
```

**Usando delay fixo:**
```pascal
// 3 tentativas com 30 segundos entre cada
.RetryPolicy(THefestoSimpleRetryPolicy.New(3, 30))
```

**Inspecionando a DLQ via dashboard:**
- Acesse `GET /api/dlq` — lista jobs na dead letter queue
- `POST /api/dlq/reprocess` — recoloca jobs na fila principal

Ver [retry-e-dlq.md](../03-configuracao/retry-e-dlq.md) para referência de fórmulas.
