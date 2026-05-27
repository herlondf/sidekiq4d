# Receita: Outbox Pattern

Publicar mensagens de forma confiável junto com uma operação de banco de dados.

```pascal
program OutboxPattern;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Outbox,
  Hefesto.Telemetry.Console;

type
  TRelayHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FOutbox: IHefestoClientOutbox;
    FQueue: IHefestoQueueAdapter;
  public
    constructor Create(
      const AOutbox: IHefestoClientOutbox;
      const AQueue: IHefestoQueueAdapter);
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

  TProcessarPedidoHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

constructor TRelayHandler.Create(
  const AOutbox: IHefestoClientOutbox;
  const AQueue: IHefestoQueueAdapter);
begin
  inherited Create;
  FOutbox := AOutbox;
  FQueue := AQueue;
end;

function TRelayHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'relay_outbox';
end;

procedure TRelayHandler.Execute(const AJob: IHefestoJobEnvelope);
var
  LEntries: TArray<THefestoOutboxEntry>;
  LEntry: THefestoOutboxEntry;
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

procedure TProcessarPedidoHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processando pedido: ', AJob.Body);
end;

// --- Simulação de escrita transacional ---

procedure CriarPedidoComOutbox(
  const AOutbox: IHefestoClientOutbox;
  const APedidoId: Integer);
var
  LRequest: THefestoPublishRequest;
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
  LStore: IHefestoStateStore;
  LOutbox: IHefestoClientOutbox;
  LQueue: IHefestoQueueAdapter;
  LServer: IHefestoServer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LOutbox := THefestoStateStoreOutbox.New(LStore);
  LQueue := THefestoInMemoryQueueAdapter.New;

  LServer := THefestoServer.New
    .UseQueue(LQueue)
    .Concurrency(2)
    .StateStore(LStore)
    .Telemetry(THefestoConsoleTelemetry.New)
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
THefestoPeriodicJob.Register(
  'relay_outbox', '*/1 * * * *',  // a cada minuto
  'default', '{}', LScheduledStore
);
```

Ver [outbox.md](../04-features/outbox.md) para detalhes da interface.
