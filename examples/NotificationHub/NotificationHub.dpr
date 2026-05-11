program NotificationHub;

{$APPTYPE CONSOLE}

// Demo: Multi-Channel Notification Routing
//
// Demonstrates fan-out/routing pattern:
// - A router handler reads the notification body and dispatches
//   to the appropriate channel handlers (push, email, sms)
// - Each channel handler processes independently
// - Concurrency limited to 2 per channel
// - Shows how to implement content-based routing with Sidekiq4D

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
  Sidekiq4D.Dispatcher,
  Sidekiq4D.Retry,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Server;

var
  GServer: ISidekiqServer;

type
  TNotificationRouterHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TPushNotificationHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TEmailNotificationHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  TSmsNotificationHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TNotificationRouterHandler }

function TNotificationRouterHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'route_notification';
end;

procedure TNotificationRouterHandler.Perform(const AContext: ISidekiqJobContext);
var
  LBody: string;
begin
  LBody := AContext.Job.Body;
  WriteLn(Format('  [ROUTER] Routing notification: %s', [LBody]));

  // Route based on user preferences in the body
  // In a real app, you would parse JSON; here we check for channel markers
  if Pos('"push":true', LBody) > 0 then
  begin
    WriteLn('  [ROUTER]   -> Dispatching to push channel');
    GServer.Enqueue('default', 'send_push', LBody);
  end;

  if Pos('"email":true', LBody) > 0 then
  begin
    WriteLn('  [ROUTER]   -> Dispatching to email channel');
    GServer.Enqueue('default', 'send_email_notif', LBody);
  end;

  if Pos('"sms":true', LBody) > 0 then
  begin
    WriteLn('  [ROUTER]   -> Dispatching to SMS channel');
    GServer.Enqueue('default', 'send_sms', LBody);
  end;
end;

{ TPushNotificationHandler }

function TPushNotificationHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'send_push';
end;

procedure TPushNotificationHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [PUSH] Sending push notification: %s', [AContext.Job.Body]));
  Sleep(50); // Simulate FCM/APNs call
  WriteLn('  [PUSH] Delivered to device.');
end;

{ TEmailNotificationHandler }

function TEmailNotificationHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'send_email_notif';
end;

procedure TEmailNotificationHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [EMAIL] Sending email notification: %s', [AContext.Job.Body]));
  Sleep(80); // Simulate SMTP send
  WriteLn('  [EMAIL] Delivered to inbox.');
end;

{ TSmsNotificationHandler }

function TSmsNotificationHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'send_sms';
end;

procedure TSmsNotificationHandler.Perform(const AContext: ISidekiqJobContext);
begin
  WriteLn(Format('  [SMS] Sending SMS: %s', [AContext.Job.Body]));
  Sleep(60); // Simulate SMS gateway call
  WriteLn('  [SMS] Delivered via gateway.');
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
begin
  try
    WriteLn('==============================================');
    WriteLn(' Sidekiq4D Demo: Multi-Channel Notification Hub');
    WriteLn('==============================================');
    WriteLn('');
    WriteLn('Pattern: Router dispatches to channel handlers');
    WriteLn('Channels: push, email, sms');
    WriteLn('Concurrency: 2 per channel action');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;

    // Enqueue notifications with different channel preferences
    Queue.Enqueue('route_notification',
      '{"user":"alice","msg":"Order shipped","push":true,"email":true,"sms":false}');
    Queue.Enqueue('route_notification',
      '{"user":"bob","msg":"Payment received","push":true,"email":false,"sms":true}');
    Queue.Enqueue('route_notification',
      '{"user":"carol","msg":"Account alert","push":true,"email":true,"sms":true}');
    Queue.Enqueue('route_notification',
      '{"user":"dave","msg":"Weekly digest","push":false,"email":true,"sms":false}');

    WriteLn('--- Enqueued 4 notifications to route ---');
    WriteLn('');
    WriteLn('--- Processing ---');
    WriteLn('');

    GServer := TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(100)
      .StopWhenIdle
      .Concurrency(6)
      .ActionConcurrency('send_push', 2)
      .ActionConcurrency('send_email_notif', 2)
      .ActionConcurrency('send_sms', 2)
      .StateStore(TSidekiqInMemoryStateStore.New)
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(2, 3))
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('route_notification', TNotificationRouterHandler.Create)
      .RegisterHandler('send_push', TPushNotificationHandler.Create)
      .RegisterHandler('send_email_notif', TEmailNotificationHandler.Create)
      .RegisterHandler('send_sms', TSmsNotificationHandler.Create);

    GServer.Run;

    WriteLn('');
    WriteLn('Demo complete. Router pattern enables flexible');
    WriteLn('multi-channel delivery based on user preferences.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
