unit Hefesto.Service.Main;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Winapi.Windows,
  Vcl.SvcMgr,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Store.InMemory,
  Hefesto.Locking,
  Hefesto.Idempotency,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  THefestoWorkerService = class(TService)
    procedure ServiceStart(Sender: TService; var Started: Boolean);
    procedure ServiceStop(Sender: TService; var Stopped: Boolean);
    procedure ServiceExecute(Sender: TService);
  private
    FServer: IHefestoServer;
    FQueue: THefestoInMemoryQueueAdapter;
    FWorkerThread: TThread;
  public
    function GetServiceController: TServiceController; override;
  end;

  // Exemplo: handler de processamento de fila
  TWorkerJobHandler = class(TInterfacedObject, IHefestoJobHandler)
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

var
  HefestoWorkerService: THefestoWorkerService;

implementation

{$R *.dfm}

{ TWorkerJobHandler }

function TWorkerJobHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True; // Aceita qualquer action
end;

procedure TWorkerJobHandler.Perform(const AContext: IHefestoJobContext);
begin
  // Aqui vai a logica real do seu worker.
  // Exemplos:
  //   - Emitir nota fiscal
  //   - Processar pagamento
  //   - Enviar email
  //   - Gerar relatorio
  //
  // O contexto contem:
  //   AContext.Job.Action  -> tipo do job
  //   AContext.Job.Body    -> payload JSON
  //   AContext.Job.Id      -> ID unico
end;

{ THefestoWorkerService }

procedure ServiceController(CtrlCode: DWord); stdcall;
begin
  HefestoWorkerService.Controller(CtrlCode);
end;

function THefestoWorkerService.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

procedure THefestoWorkerService.ServiceStart(Sender: TService; var Started: Boolean);
var
  LStateStore: IHefestoStateStore;
begin
  // Configurar o servidor Hefesto
  //
  // Em producao, substituir por:
  //   - THefestoSQSQueueAdapter para consumir filas AWS SQS
  //   - THefestoPostgresStateStore ou Redis4D para estado distribuido
  //   - Handlers reais do seu dominio

  LStateStore := THefestoInMemoryStateStore.New;
  FQueue := THefestoInMemoryQueueAdapter.New;

  FServer := THefestoServer.New
    .UseQueue(FQueue)

    // Configuracao de processamento
    .Concurrency(4)
    .BatchSize(10)
    .IdleDelayMs(1000)

    // Providers
    .StateStore(LStateStore)
    .LockProvider(THefestoInMemoryLockProvider.New(LStateStore))
    .Idempotency(THefestoStateStoreIdempotency.New(LStateStore))
    .RetryPolicy(THefestoSimpleRetryPolicy.New(5, 30))

    // Telemetry (em producao: StatsD, arquivo de log, etc)
    .Telemetry(THefestoConsoleTelemetry.New)

    // Handlers
    .RegisterHandler('default', TWorkerJobHandler.Create);

  Started := True;
end;

procedure THefestoWorkerService.ServiceExecute(Sender: TService);
begin
  // O ServiceExecute roda em uma thread dedicada pelo SCM.
  // Chamamos Server.Run que e blocking — ele faz polling ate ser parado.
  try
    FServer.Run;
  except
    on E: Exception do
    begin
      // Log do erro critico
      // Em producao: gravar em arquivo ou event log
    end;
  end;
end;

procedure THefestoWorkerService.ServiceStop(Sender: TService; var Stopped: Boolean);
begin
  // Shutdown gracioso: Server.Stop sinaliza para o Run parar
  // e aguarda os workers ativos finalizarem.
  if Assigned(FServer) then
    FServer.Stop;
  Stopped := True;
end;

end.
