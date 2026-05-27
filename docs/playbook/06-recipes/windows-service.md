# Recipe: Windows Service

Run the Hefesto server as a Windows Service for automatic startup and operation without user login.

## Project structure

```
MyWorkerService/
├── MyWorkerService.dpr          main program
├── MyWorkerService.dproj
├── ServiceUnit.pas              service logic
└── WorkerSetup.pas              server configuration
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
    LogMessage('Hefesto Worker Service started.');
  except on E: Exception do
    begin
      LogMessage('Error starting: ' + E.Message, EVENTLOG_ERROR_TYPE);
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
    LogMessage('Hefesto Worker Service stopped.');
  end;
end;

procedure TWorkerService.ServiceExecute(Sender: TService);
begin
  // Main service loop — keeps alive until stopped
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
  // your handlers here
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

## Installation and management

```cmd
REM Install the service
MyWorkerService.exe /install

REM Start
net start MyWorkerService
REM or: sc start MyWorkerService

REM Stop
net stop MyWorkerService

REM Uninstall
MyWorkerService.exe /uninstall
```

## Services Manager configuration

After installing:
1. Open `services.msc`
2. Locate "MyWorkerService"
3. Configure: Startup Type = Automatic, Recovery = Restart the service
4. Log On: account with network permission to access Redis

## Production logging

Replace `THefestoConsoleTelemetry` with a provider that writes to the Windows Event Log or a file:

```pascal
.Telemetry(
  THefestoCompositeTelemetry.New([
    THefestoOTLPTraceTelemetry.New('http://collector:4318', 'worker-svc'),
    THefestoMetricsTelemetry.New('statsd-host', 8125)
  ])
)
```
