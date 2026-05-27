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
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Middleware,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  // Middleware client: loga antes de publicar
  TLoggingClientMiddleware = class(TInterfacedObject, IHefestoClientMiddleware)
    procedure Call(const AQueueName, AAction, ABody: string;
      const AAttributes: TStrings; const ANext: THefestoNextProc);
  end;

  // Middleware server: mede tempo de execucao
  TTimingServerMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
  end;

  // Middleware server: valida payload
  TValidationServerMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
  end;

  TWorkerHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

{ TLoggingClientMiddleware }

procedure TLoggingClientMiddleware.Call(const AQueueName, AAction, ABody: string;
  const AAttributes: TStrings; const ANext: THefestoNextProc);
begin
  WriteLn(Format('  [client-mw] Publicando: queue=%s action=%s',
    [AQueueName, AAction]));
  ANext; // continua a cadeia
end;

{ TTimingServerMiddleware }

procedure TTimingServerMiddleware.Call(const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
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

procedure TValidationServerMiddleware.Call(const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
begin
  if AJob.Body.IsEmpty then
  begin
    WriteLn(Format('  [validation-mw] Job %s rejeitado: body vazio', [AJob.Action]));
    Exit; // NAO chama ANext — job nao executa
  end;
  ANext;
end;

{ TWorkerHandler }

function TWorkerHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True; // aceita qualquer action
end;

procedure TWorkerHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn(Format('  [handler] Processando: %s -> %s',
    [AContext.Job.Action, AContext.Job.Body]));
  Sleep(50);
end;

var
  Queue: THefestoInMemoryQueueAdapter;
begin
  try
    WriteLn('Hefesto - Demo Middleware Pipeline');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;

    // Enfileira jobs diretamente (bypass client middleware para demonstracao)
    Queue.Enqueue('processar', '{"pedido":1}');
    Queue.Enqueue('processar', '{"pedido":2}');
    Queue.Enqueue('processar', ''); // body vazio — sera rejeitado

    WriteLn('--- Middlewares registrados ---');
    WriteLn('  Client: TLoggingClientMiddleware');
    WriteLn('  Server: TValidationServerMiddleware -> TTimingServerMiddleware');
    WriteLn('');

    WriteLn('--- Processando ---');
    THefestoServer.New
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
