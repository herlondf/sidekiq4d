program EmailSender;

{$APPTYPE CONSOLE}

// Demo: Async Email Sending with Retry and Rate Limiting
//
// Demonstrates how to send emails asynchronously with:
// - Rate limiting: max 10 emails per 60 seconds (token bucket)
// - Retry policy: 3 attempts with 5s delay on failure
// - Telemetry output showing processing flow

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.Store.InMemory,
  Hefesto.RateLimit,
  Hefesto.Dispatcher,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Server;

type
  TEmailSendHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

{ TEmailSendHandler }

function TEmailSendHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'send_email';
end;

procedure TEmailSendHandler.Perform(const AContext: IHefestoJobContext);
begin
  WriteLn('  [SMTP] Connecting to mail server...');
  Sleep(100); // Simulate SMTP connection + send
  WriteLn(Format('  [SMTP] Email sent successfully: %s', [AContext.Job.Body]));
end;

var
  Queue: THefestoInMemoryQueueAdapter;
  StateStore: IHefestoStateStore;
begin
  try
    WriteLn('===========================================');
    WriteLn(' Hefesto Demo: Async Email Sender');
    WriteLn('===========================================');
    WriteLn('');
    WriteLn('Configuration:');
    WriteLn('  - Rate Limit: 10 emails per 60 seconds');
    WriteLn('  - Retry: 3 attempts, 5s delay');
    WriteLn('');

    Queue := THefestoInMemoryQueueAdapter.New;
    StateStore := THefestoInMemoryStateStore.New;

    // Enqueue 5 emails to different recipients
    Queue.Enqueue('send_email', '{"to":"alice@company.com","subject":"Welcome aboard!"}');
    Queue.Enqueue('send_email', '{"to":"bob@partner.org","subject":"Invoice #1042"}');
    Queue.Enqueue('send_email', '{"to":"carol@client.io","subject":"Meeting reminder"}');
    Queue.Enqueue('send_email', '{"to":"dave@team.dev","subject":"Deploy notification"}');
    Queue.Enqueue('send_email', '{"to":"eve@support.com","subject":"Ticket resolved"}');

    WriteLn('--- Enqueued 5 emails ---');
    WriteLn('');
    WriteLn('--- Processing with rate limiting ---');
    WriteLn('');

    THefestoServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .StateStore(StateStore)
      .RateLimiter(THefestoTokenBucketRateLimiter.New(StateStore))
      .RetryPolicy(THefestoSimpleRetryPolicy.New(3, 5))
      .Telemetry(THefestoConsoleTelemetry.New)
      .RegisterHandler('send_email', TEmailSendHandler.Create)
      .Run;

    WriteLn('');
    WriteLn('All emails processed. Rate limiter ensured');
    WriteLn('delivery stays within 10 emails/minute threshold.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
