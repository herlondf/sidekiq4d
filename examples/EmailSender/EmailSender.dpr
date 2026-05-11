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
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler,
  Sidekiq4D.Options,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Queue.InMemory,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Store.InMemory,
  Sidekiq4D.RateLimit,
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

type
  TEmailSendHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TEmailSendHandler }

function TEmailSendHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'send_email';
end;

procedure TEmailSendHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn('  [SMTP] Connecting to mail server...');
  Sleep(100); // Simulate SMTP connection + send
  WriteLn(Format('  [SMTP] Email sent successfully: %s', [AContext.Job.Body]));
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  StateStore: ISidekiqStateStore;
begin
  try
    WriteLn('===========================================');
    WriteLn(' Sidekiq4D Demo: Async Email Sender');
    WriteLn('===========================================');
    WriteLn('');
    WriteLn('Configuration:');
    WriteLn('  - Rate Limit: 10 emails per 60 seconds');
    WriteLn('  - Retry: 3 attempts, 5s delay');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;
    StateStore := TSidekiqInMemoryStateStore.New;

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

    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .StateStore(StateStore)
      .RateLimiter(TSidekiqTokenBucketRateLimiter.New(StateStore))
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(3, 5))
      .Telemetry(TSidekiqConsoleTelemetry.New)
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
