unit Sidekiq4D.Service.Main;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  Winapi.Windows,
  Vcl.SvcMgr,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TSidekiqWorkerService = class(TService)
    procedure ServiceStart(Sender: TService; var Started: Boolean);
    procedure ServiceStop(Sender: TService; var Stopped: Boolean);
    procedure ServiceExecute(Sender: TService);
  private
    FServer: ISidekiqServer;
    FQueue: TSidekiqInMemoryQueueAdapter;
    FWorkerThread: TThread;
  public
    function GetServiceController: TServiceController; override;
  end;

  // Exemplo: handler de processamento de fila
  TWorkerJobHandler = class(TInterfacedObject, ISidekiqJobHandler)
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

var
  SidekiqWorkerService: TSidekiqWorkerService;

implementation

{$R *.dfm}

{ TWorkerJobHandler }

function TWorkerJobHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := True; // Aceita qualquer action
end;

procedure TWorkerJobHandler.Perform(const AContext: ISidekiqJobContext);
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

{ TSidekiqWorkerService }

procedure ServiceController(CtrlCode: DWord); stdcall;
begin
  SidekiqWorkerService.Controller(CtrlCode);
end;

function TSidekiqWorkerService.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

procedure TSidekiqWorkerService.ServiceStart(Sender: TService; var Started: Boolean);
var
  LStateStore: ISidekiqStateStore;
begin
  // Configurar o servidor Sidekiq4D
  //
  // Em producao, substituir por:
  //   - TSidekiqSQSQueueAdapter para consumir filas AWS SQS
  //   - TSidekiqPostgresStateStore ou Redis4D para estado distribuido
  //   - Handlers reais do seu dominio

  LStateStore := TSidekiqInMemoryStateStore.New;
  FQueue := TSidekiqInMemoryQueueAdapter.New;

  FServer := TSidekiqServer.New
    .UseQueue(FQueue)

    // Configuracao de processamento
    .Concurrency(4)
    .BatchSize(10)
    .IdleDelayMs(1000)

    // Providers
    .StateStore(LStateStore)
    .LockProvider(TSidekiqInMemoryLockProvider.New(LStateStore))
    .Idempotency(TSidekiqStateStoreIdempotency.New(LStateStore))
    .RetryPolicy(TSidekiqSimpleRetryPolicy.New(5, 30))

    // Telemetry (em producao: StatsD, arquivo de log, etc)
    .Telemetry(TSidekiqConsoleTelemetry.New)

    // Handlers
    .RegisterHandler('default', TWorkerJobHandler.Create);

  Started := True;
end;

procedure TSidekiqWorkerService.ServiceExecute(Sender: TService);
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

procedure TSidekiqWorkerService.ServiceStop(Sender: TService; var Stopped: Boolean);
begin
  // Shutdown gracioso: Server.Stop sinaliza para o Run parar
  // e aguarda os workers ativos finalizarem.
  if Assigned(FServer) then
    FServer.Stop;
  Stopped := True;
end;

end.
