program HefestoVCLDemo;

uses
  Vcl.Forms,
  Hefesto.VCLDemo.Main in 'Hefesto.VCLDemo.Main.pas' {FrmMain};


begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TFrmMain, FrmMain);
  Application.Run;
end.
