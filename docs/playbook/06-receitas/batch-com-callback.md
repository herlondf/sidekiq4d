# Receita: Batch com Callback

Processar múltiplos jobs como lote com notificação ao concluir.

```pascal
program BatchComCallback;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Server,
  Sidekiq4D.Handler,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Batch,
  Sidekiq4D.Telemetry.Console;

type
  TProcessItemHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: ISidekiqJobEnvelope);
  end;

function TProcessItemHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process_item';
end;

procedure TProcessItemHandler.Execute(const AJob: ISidekiqJobEnvelope);
begin
  Writeln('Processando item: ', AJob.Body);
  Sleep(100); // simula trabalho
end;

var
  LStore: ISidekiqStateStore;
  LBatchStore: ISidekiqBatchStore;
  LServer: ISidekiqServer;
  LBatch: ISidekiqBatch;
  I: Integer;
begin
  LStore := TSidekiqInMemoryStateStore.New;
  LBatchStore := TSidekiqStateStoreBatchStore.New(LStore);

  LServer := TSidekiqServer.New
    .UseQueue(TSidekiqInMemoryQueueAdapter.New)
    .Concurrency(4)
    .StateStore(LStore)
    .Telemetry(TSidekiqConsoleTelemetry.New)
    .RegisterHandler('process_item', TProcessItemHandler.Create)
    .Run;

  // Criar e commitar o batch
  LBatch := TSidekiqBatch.New(LBatchStore)
    .OnComplete(procedure
      begin
        Writeln('--- Batch concluído (sucesso ou falha) ---');
      end)
    .OnSuccess(procedure
      begin
        Writeln('--- Todos os itens processados com sucesso! ---');
      end);

  for I := 1 to 10 do
    LBatch.Add('process_item', Format('{"id": %d}', [I]));

  LBatch.Commit;
  Writeln('Batch de 10 items commitado.');

  Writeln('Aguardando conclusão... (Enter para parar)');
  ReadLn;
  LServer.Stop;
end.
```

**Batch com dados dinâmicos:**
```pascal
LBatch := TSidekiqBatch.New(LBatchStore)
  .OnSuccess(procedure begin NotificarSistema end);

for var Pedido in FPedidosPendentes do
  if Pedido.PrecisaProcessar then
    LBatch.Add('process_pedido', Pedido.ToJSON);

if LBatch.Count > 0 then
  LBatch.Commit
else
  Writeln('Nenhum pedido pendente.');
```

Ver [batch-jobs.md](../04-features/batch-jobs.md) para semântica detalhada dos callbacks.
