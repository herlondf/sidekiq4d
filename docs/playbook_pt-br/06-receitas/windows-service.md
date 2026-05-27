# Receita: Windows Service

Executar o servidor Hefesto como Windows Service para inicialização automática e operação sem login de usuário.

## Estrutura do projeto

```
MyWorkerService/
├── MyWorkerService.dpr          programa principal
├── MyWorkerService.dproj
├── ServiceUnit.pas              lógica do service
└── WorkerSetup.pas              configuração do servidor
```

## ServiceUnit.pas

```pascal
unit ServiceUnit;

interface

uses
  System.SysUtils,
  Vcl.SvcMgr,
  Hefesto.Server,
  Hefesto.Queue.RedisStreams,
  Hefesto.Store.Redis4D,
  Hefesto.Retry,
  Hefesto.Telemetry.Console;

type
  TWorkerService = class(TService)
    procedure ServiceStart(Sender: TService; var Started: Boolean);
    procedure ServiceStop(Sender: TService);
    procedure ServiceExecute(Sender: TService);
  private
    FServer: IHefestoServer;
  public
    function GetServiceController: TServiceController; override;
  end;

implementation

uses
  WorkerSetup;

procedure TWorkerService.ServiceStart(Sender: TService; var Started: Boolean);
begin
  try
    FServer := TWorkerSetup.CreateServer;
    FServer.Run;
    Started := True;
    LogMessage('Hefesto Worker Service iniciado.');
  except on E: Exception do
    begin
      LogMessage('Erro ao iniciar: ' + E.Message, EVENTLOG_ERROR_TYPE);
      Started := False;
    end;
  end;
end;

procedure TWorkerService.ServiceStop(Sender: TService);
begin
  if Assigned(FServer) then
  begin
    FServer.Stop;
    FServer := nil;
    LogMessage('Hefesto Worker Service parado.');
  end;
end;

procedure TWorkerService.ServiceExecute(Sender: TService);
begin
  // Loop principal do service — mantém vivo enquanto não for parado
  while not Terminated do
  begin
    ServiceThread.ProcessRequests(True);
    Sleep(100);
  end;
end;

function TWorkerService.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

end.
```

## WorkerSetup.pas

```pascal
unit WorkerSetup;

interface

uses
  Hefesto.Server;

type
  TWorkerSetup = class
  public
    class function CreateServer: IHefestoServer;
  end;

implementation

uses
  System.SysUtils,
  Hefesto.Queue.RedisStreams,
  Hefesto.Store.Redis4D,
  Hefesto.Retry,
  Hefesto.Idempotency,
  Hefesto.Telemetry.Console,
  // seus handlers aqui
  MyHandlers;

const
  REDIS_URL = 'redis://localhost:6379';

class function TWorkerSetup.CreateServer: IHefestoServer;
var
  LStore: IHefestoStateStore;
begin
  LStore := THefestoRedis4DStateStore.New
    .ConnectionString(REDIS_URL);

  Result := THefestoServer.New
    .UseQueue(
      THefestoRedisStreamsAdapter.New
        .ConnectionString(REDIS_URL)
        .StreamName('sidekiq4d:jobs')
        .ConsumerGroup('workers')
        .ConsumerName('svc-' + GetEnvironmentVariable('COMPUTERNAME'))
    )
    .Concurrency(4)
    .StateStore(LStore)
    .Idempotency(THefestoStateStoreIdempotency.New(LStore))
    .RetryPolicy(THefestoExponentialRetryPolicy.New(5, 15, 3600))
    .Telemetry(THefestoConsoleTelemetry.New)
    .RegisterHandler('process_order', TProcessOrderHandler.Create)
    .RegisterHandler('send_email',    TSendEmailHandler.Create);
end;

end.
```

## MyWorkerService.dpr

```pascal
program MyWorkerService;

uses
  Vcl.SvcMgr,
  ServiceUnit in 'ServiceUnit.pas' {WorkerService: TService};

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;
  if not Application.DelayInitialize or Application.Installing then
    Application.Initialize;
  Application.CreateForm(TWorkerService, WorkerService);
  Application.Run;
end.
```

## Instalação e gerenciamento

```cmd
REM Instalar o service
MyWorkerService.exe /install

REM Iniciar
net start MyWorkerService
REM ou: sc start MyWorkerService

REM Parar
net stop MyWorkerService

REM Desinstalar
MyWorkerService.exe /uninstall
```

## Configuração no Services Manager

Após instalar:
1. Abrir `services.msc`
2. Localizar "MyWorkerService"
3. Configurar: Startup Type = Automatic, Recovery = Restart the service
4. Log On: conta com permissão de rede para acessar Redis

## Logging em produção

Substituir `THefestoConsoleTelemetry` por um provider que grava no Windows Event Log ou em arquivo:

```pascal
.Telemetry(
  THefestoCompositeTelemetry.New([
    THefestoOTLPTraceTelemetry.New('http://collector:4318', 'worker-svc'),
    THefestoMetricsTelemetry.New('statsd-host', 8125)
  ])
)
```
