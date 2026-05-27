# Receita: Batch com Callback

Processar múltiplos jobs como lote com notificação ao concluir.

```pascal
program BatchComCallback;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Server,
  Hefesto.Handler,
  Hefesto.Queue.InMemory,
  Hefesto.Store.InMemory,
  Hefesto.Batch,
  Hefesto.Telemetry.Console;

type
  TProcessItemHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AAction: string): Boolean;
    procedure Execute(const AJob: IHefestoJobEnvelope);
  end;

function TProcessItemHandler.CanHandle(const AAction: string): Boolean;
begin
  Result := AAction = 'process_item';
end;

procedure TProcessItemHandler.Execute(const AJob: IHefestoJobEnvelope);
begin
  Writeln('Processando item: ', AJob.Body);
  Sleep(100); // simula trabalho
end;

var
  LStore: IHefestoStateStore;
  LBatchStore: IHefestoBatchStore;
  LServer: IHefestoServer;
  LBatch: IHefestoBatch;
  I: Integer;
begin
  LStore := THefestoInMemoryStateStore.New;
  LBatchStore := THefestoStateStoreBatchStore.New(LStore);

  LServer := THefestoServer.New
    .UseQueue(THefestoInMemoryQueueAdapter.New)
    .Concurrency(4)
    .StateStore(LStore)
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('process_item', TProcessItemHandler.Create)
    .Run;

  // Criar e commitar o batch
  LBatch := THefestoBatch.New(LBatchStore)
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
LBatch := THefestoBatch.New(LBatchStore)
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
