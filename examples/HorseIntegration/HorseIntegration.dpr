program HorseIntegrationRunner;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  HorseIntegration.Main in 'HorseIntegration.Main.pas';

begin
  ReportMemoryLeaksOnShutdown := True;
  try
    StartServer(8080);
  except
    on E: Exception do
    begin
      Writeln(E.ClassName, ': ', E.Message);
      System.ExitCode := 1;
    end;
  end;
end.
