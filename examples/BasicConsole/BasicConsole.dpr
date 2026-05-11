program BasicConsole;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Sidekiq4D.Job in '..\..\src\Sidekiq4D.Job.pas',
  Sidekiq4D.Context in '..\..\src\Sidekiq4D.Context.pas',
  Sidekiq4D.Handler in '..\..\src\Sidekiq4D.Handler.pas',
  Sidekiq4D.Options in '..\..\src\Sidekiq4D.Options.pas',
  Sidekiq4D.Queue.Interfaces in '..\..\src\Sidekiq4D.Queue.Interfaces.pas',
  Sidekiq4D.Queue.InMemory in '..\..\src\Sidekiq4D.Queue.InMemory.pas',
  Sidekiq4D.Dispatcher in '..\..\src\Sidekiq4D.Dispatcher.pas',
  Sidekiq4D.Retry in '..\..\src\Sidekiq4D.Retry.pas',
  Sidekiq4D.Telemetry in '..\..\src\Sidekiq4D.Telemetry.pas',
  Sidekiq4D.Server in '..\..\src\Sidekiq4D.Server.pas';

type
  TEmailJobHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TEmailJobHandler }

function TEmailJobHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'email';
end;

procedure TEmailJobHandler.Perform(const AContext: ISidekiqJobContext);
begin
  Writeln('Handling email job: ' + AContext.Job.Body);
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
begin
  try
    Queue := TSidekiqInMemoryQueueAdapter.New;
    Queue.Enqueue('email', '{"to":"user@example.com"}');

    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .Telemetry(TSidekiqConsoleTelemetry.New)
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
