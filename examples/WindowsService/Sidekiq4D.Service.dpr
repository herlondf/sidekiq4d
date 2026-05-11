program Sidekiq4DService;

// Demo: Sidekiq4D como Windows Service
//
// Template para deploy em producao como servico Windows.
// O servidor roda no ServiceExecute e para graciosamente no ServiceStop.
//
// Instalar: Sidekiq4D.Service.exe /install
// Remover:  Sidekiq4D.Service.exe /uninstall
// Rodar como console (debug): Sidekiq4D.Service.exe /console

uses
  System.SysUtils,
  Vcl.SvcMgr,
  Sidekiq4D.Service.Main in 'Sidekiq4D.Service.Main.pas' {SidekiqWorkerService: TService};

var
  Application: TServiceApplication;

begin
  if not Vcl.SvcMgr.Application.DelayInitialize or Vcl.SvcMgr.Application.Installing then
    Vcl.SvcMgr.Application.Initialize;
  Vcl.SvcMgr.Application.CreateForm(TSidekiqWorkerService, SidekiqWorkerService);
  Vcl.SvcMgr.Application.Run;
end.
