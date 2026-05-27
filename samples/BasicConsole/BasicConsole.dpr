program BasicConsole;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Hefesto.Job in '..\..\src\Hefesto.Job.pas',
  Hefesto.Context in '..\..\src\Hefesto.Context.pas',
  Hefesto.Handler in '..\..\src\Hefesto.Handler.pas',
  Hefesto.Options in '..\..\src\Hefesto.Options.pas',
  Hefesto.Queue.Interfaces in '..\..\src\Hefesto.Queue.Interfaces.pas',
  Hefesto.Queue.InMemory in '..\..\src\Hefesto.Queue.InMemory.pas',
  Hefesto.Dispatcher in '..\..\src\Hefesto.Dispatcher.pas',
  Hefesto.Retry in '..\..\src\Hefesto.Retry.pas',
  Hefesto.Telemetry in '..\..\src\Hefesto.Telemetry.pas',
  Hefesto.Server in '..\..\src\Hefesto.Server.pas';

type
  TEmailJobHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

{ TEmailJobHandler }

function TEmailJobHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'email';
end;

procedure TEmailJobHandler.Perform(const AContext: IHefestoJobContext);
begin
  Writeln('Handling email job: ' + AContext.Job.Body);
end;

var
  Queue: THefestoInMemoryQueueAdapter;
begin
  try
    Queue := THefestoInMemoryQueueAdapter.New;
    Queue.Enqueue('email', '{"to":"user@example.com"}');

    THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .Telemetry(THefestoConsoleTelemetry.New)
      .RegisterHandler('email', TEmailJobHandler.Create)
      .Run;
  except
    on E: Exception do
    begin
      Writeln(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
