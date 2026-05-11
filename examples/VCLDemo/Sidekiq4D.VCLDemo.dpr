program Sidekiq4DVCLDemo;

uses
  Vcl.Forms,
  Sidekiq4D.VCLDemo.Main in 'Sidekiq4D.VCLDemo.Main.pas' {FrmMain};


begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TFrmMain, FrmMain);
  Application.Run;
end.
