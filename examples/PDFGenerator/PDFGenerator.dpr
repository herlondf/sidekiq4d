program PDFGenerator;

{$APPTYPE CONSOLE}

// Demo: Background PDF Generation with Batch Callback
//
// Demonstrates batch processing pattern:
// - 4 PDF jobs run in parallel (invoice, report, certificate, receipt)
// - OnComplete callback notifies user when all PDFs are ready
// - Shows how to coordinate parallel work with a final notification

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Batch,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TPDFGenerateHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TNotifyUserHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TPDFGenerateHandler }

function TPDFGenerateHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'generate_pdf';
end;

procedure TPDFGenerateHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [PDF] Generating: %s', [AContext.Job.Body]));
  Sleep(200); // Simulate PDF rendering
  WriteLn(Format('  [PDF] Done: %s', [AContext.Job.Body]));
end;

{ TNotifyUserHandler }

function TNotifyUserHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'notify_user';
end;

procedure TNotifyUserHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn('');
  WriteLn('  *** BATCH COMPLETE ***');
  WriteLn(Format('  [NOTIFY] %s', [AContext.Job.Body]));
  WriteLn('  *** All PDFs are ready for download ***');
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  Server: ISidekiqServer;
  Batch: ISidekiqBatch;
begin
  try
    WriteLn('===========================================');
    WriteLn(' Sidekiq4D Demo: PDF Generator with Batch');
    WriteLn('===========================================');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    Server := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .StateStore(TSidekiqInMemoryStateStore.New)
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('generate_pdf', TPDFGenerateHandler.Create)
      .RegisterHandler('notify_user', TNotifyUserHandler.Create);

    // Create a batch of PDF generation jobs
    WriteLn('--- Creating PDF batch ---');
    WriteLn('');

    Batch := Server.Batch('user-documents-export');

    Batch
      .Enqueue('default', 'generate_pdf', '{"type":"invoice","id":1001,"format":"A4"}')
      .Enqueue('default', 'generate_pdf', '{"type":"report","id":2050,"format":"Letter"}')
      .Enqueue('default', 'generate_pdf', '{"type":"certificate","id":3012,"format":"A4"}')
      .Enqueue('default', 'generate_pdf', '{"type":"receipt","id":4099,"format":"A5"}');

    // When all PDFs are generated, notify the user
    Batch.OnComplete('default', 'notify_user',
      '{"user_id":42,"message":"Your 4 documents are ready to download"}');

    WriteLn('  Batch created: 4 PDF jobs');
    WriteLn('  Callback: notify user on completion');
    WriteLn('');
    WriteLn('--- Processing ---');
    WriteLn('');

    Server.Run;

    WriteLn('');
    WriteLn('Demo complete. Batch pattern ensures notification');
    WriteLn('fires only after all PDFs finish generating.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
