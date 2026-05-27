program HefestoService;

// Demo: Hefesto como Windows Service
//
// Template para deploy em producao como servico Windows.
// O servidor roda no ServiceExecute e para graciosamente no ServiceStop.
//
// Instalar: Hefesto.Service.exe /install
// Remover:  Hefesto.Service.exe /uninstall
// Rodar como console (debug): Hefesto.Service.exe /console

uses
  System.SysUtils,
  Vcl.SvcMgr,
  Hefesto.Service.Main in 'Hefesto.Service.Main.pas' {HefestoWorkerService: TService};

var
  Application: TServiceApplication;

begin
  if not Vcl.SvcMgr.Application.DelayInitialize or Vcl.SvcMgr.Application.Installing then
    Vcl.SvcMgr.Application.Initialize;
  Vcl.SvcMgr.Application.CreateForm(THefestoWorkerService, HefestoWorkerService);
  Vcl.SvcMgr.Application.Run;
end.
