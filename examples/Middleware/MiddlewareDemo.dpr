program MiddlewareDemo;

{$APPTYPE CONSOLE}

// Demo: Middleware Pipeline (Client + Server)
//
// Mostra como adicionar middlewares que executam antes de publicar (client)
// e antes de processar (server) cada job. Util para logging, metricas,
// validacao, transformacao de payload, etc.

uses
  System.SysUtils,
  System.Classes,
  System.Diagnostics,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Middleware,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  // Middleware client: loga antes de publicar
  TLoggingClientMiddleware = class(TInterfacedObject, ISidekiqClientMiddleware)
    procedure Call(const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings; const ANext: TSidekiqNextProc);
  end;

  // Middleware server: mede tempo de execucao
  TTimingServerMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
  end;

  // Middleware server: valida payload
  TValidationServerMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
  end;

  TWorkerHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TLoggingClientMiddleware }

procedure TLoggingClientMiddleware.Call(const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings; const ANext: TSidekiqNextProc);
begin
  WriteLn(Format('  [client-mw] Publicando: queue=%s action=%s',
    [AQueueName, AAction]));
  ANext; // continua a cadeia
end;

{ TTimingServerMiddleware }

procedure TTimingServerMiddleware.Call(const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
var
  LWatch: TStopwatch;
begin
  LWatch := TStopwatch.StartNew;
  ANext; // executa o job
  LWatch.Stop;
  WriteLn(Format('  [server-mw] Job %s executou em %dms',
    [AJob.Action, LWatch.ElapsedMilliseconds]));
end;

{ TValidationServerMiddleware }

procedure TValidationServerMiddleware.Call(const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
begin
  if AJob.Body.IsEmpty then
  begin
    WriteLn(Format('  [validation-mw] Job %s rejeitado: body vazio', [AJob.Action]));
    Exit; // NAO chama ANext — job nao executa
  end;
  ANext;
end;

{ TWorkerHandler }

function TWorkerHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := True; // aceita qualquer action
end;

procedure TWorkerHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [handler] Processando: %s -> %s',
    [AContext.Job.Action, AContext.Job.Body]));
  Sleep(50);
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
begin
  try
    WriteLn('Sidekiq4D - Demo Middleware Pipeline');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    // Enfileira jobs diretamente (bypass client middleware para demonstracao)
    Queue.Enqueue('processar', '{"pedido":1}');
    Queue.Enqueue('processar', '{"pedido":2}');
    Queue.Enqueue('processar', ''); // body vazio — sera rejeitado

    WriteLn('--- Middlewares registrados ---');
    WriteLn('  Client: TLoggingClientMiddleware');
    WriteLn('  Server: TValidationServerMiddleware -> TTimingServerMiddleware');
    WriteLn('');

    WriteLn('--- Processando ---');
    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle

      // Client middleware (roda em Enqueue/EnqueueIn/EnqueueAt)
      .UseClientMiddleware(TLoggingClientMiddleware.Create)

      // Server middlewares (rodam em ordem antes do handler)
      .UseServerMiddleware(TValidationServerMiddleware.Create)
      .UseServerMiddleware(TTimingServerMiddleware.Create)

      .RegisterHandler('processar', TWorkerHandler.Create)
      .Run;

    WriteLn('');
    WriteLn('Demo concluido.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
