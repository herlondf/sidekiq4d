# Receita: Outbox Pattern

Publicar mensagens de forma confiável junto com uma operação de banco de dados.

```pascal
program OutboxPattern;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Outbox,
  Sidekiq4D.Telemetry.Console;

type
  TRelayHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    FOutbox: ISidekiqClientOutbox;
    FQueue: ISidekiqQueueAdapter;
  public
    constructor Create(
      const AOutbox: ISidekiqClientOutbox;
      const AQueue: ISidekiqQueueAdapter);
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

  TProcessarPedidoHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

constructor TRelayHandler.Create(
  const AOutbox: ISidekiqClientOutbox;
  const AQueue: ISidekiqQueueAdapter);
begin
  inherited Create;
  FOutbox := AOutbox;
  FQueue := AQueue;
end;

function TRelayHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'relay_outbox';
end;

procedure TRelayHandler.Execute(const AJob: ISidekiqJobEnvelope);
var
  LEntries: TArray<TSidekiqOutboxEntry>;
  LEntry: TSidekiqOutboxEntry;
begin
  LEntries := FOutbox.Entries;
  Writeln(Format('[relay] %d entradas pendentes no outbox', [Length(LEntries)]));

  for LEntry in LEntries do
  begin
    try
      FQueue.Enqueue(LEntry.Request.Action, LEntry.Request.Body);
      FOutbox.Remove(LEntry.Id);
      Writeln('[relay] Publicada: ', LEntry.Request.Action);
    except on E: Exception do
      Writeln('[relay] Falha ao publicar: ', E.Message);
      // mantém no outbox para próxima execução
    end;
  end;
end;

function TProcessarPedidoHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'processar_pedido';
end;

procedure TProcessarPedidoHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('Processando pedido: ', AJob.Body);
end;

// --- Simulação de escrita transacional ---

procedure CriarPedidoComOutbox(
  const AOutbox: ISidekiqClientOutbox;
  const APedidoId: Integer);
var
  LRequest: TSidekiqPublishRequest;
begin
  // Em produção: dentro de uma transação de banco

  // 1. Gravar pedido no banco (simulado)
  Writeln(Format('Gravando pedido %d no banco...', [APedidoId]));

  // 2. Gravar mensagem no outbox (mesma transação)
  LRequest.Queue := 'default';
  LRequest.Action := 'processar_pedido';
  LRequest.Body := Format('{"pedido_id": %d}', [APedidoId]);
  AOutbox.Save(LRequest);

  Writeln(Format('Mensagem salva no outbox. Total: %d', [AOutbox.Count]));
  // Transação commita aqui — banco e outbox consistentes
end;

var
  LStore: ISidekiqStateStore;
  LOutbox: ISidekiqClientOutbox;
  LQueue: ISidekiqQueueAdapter;
  LServer: ISidekiqServer;
begin
  LStore := TSidekiqInMemoryStateStore.New;
  LOutbox := TSidekiqStateStoreOutbox.New(LStore);
  LQueue := TSidekiqInMemoryQueueAdapter.New;

  LServer := TSidekiqServer.New
    .UseQueue(LQueue)
    .Concurrency(2)
    .StateStore(LStore)
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('relay_outbox',     TRelayHandler.Create(LOutbox, LQueue))
    .RegisterHandler('processar_pedido', TProcessarPedidoHandler.Create)
    .Run;

  // Simular criação de pedidos
  CriarPedidoComOutbox(LOutbox, 1001);
  CriarPedidoComOutbox(LOutbox, 1002);
  CriarPedidoComOutbox(LOutbox, 1003);

  // Acionar relay manualmente (em produção: job periódico)
  LQueue.Enqueue('relay_outbox', '{}');

  ReadLn;
  LServer.Stop;
end.
```

**Relay como job periódico (produção):**
```pascal
// Registrar relay para rodar a cada 30 segundos
TSidekiqPeriodicJob.Register(
  'relay_outbox', '*/1 * * * *',  // a cada minuto
  'default', '{}', LScheduledStore
);
```

Ver [outbox.md](../04-features/outbox.md) para detalhes da interface.
