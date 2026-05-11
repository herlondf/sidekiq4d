program WebhookDispatcher;

{$APPTYPE CONSOLE}

// Demo: Webhook Delivery with Retry
//
// Demonstrates resilient webhook delivery:
// - Simulates HTTP POST to external endpoints
// - Some endpoints randomly "fail" (simulating network issues)
// - Retry policy with 3 attempts and 2s delay
// - Shows how failed deliveries get retried automatically

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

type
  TWebhookDeliveryHandler = class(TInterfacedObject, ISidekiqJobHandler)
  private
    FCallCount: Integer;
  public
    constructor Create;
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

{ TWebhookDeliveryHandler }

constructor TWebhookDeliveryHandler.Create;
begin
  inherited Create;
  FCallCount := 0;
end;

function TWebhookDeliveryHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := AJob.Action = 'deliver_webhook';
end;

procedure TWebhookDeliveryHandler.Perform(const AContext: ISidekiqJobContext);
var
  LBody: string;
begin
  Inc(FCallCount);
  LBody := AContext.Job.Body;

  WriteLn(Format('  [HTTP] POST webhook: %s', [LBody]));
  Sleep(50); // Simulate network call

  // Simulate failure for endpoints containing "unstable" (first 2 attempts fail)
  if (Pos('unstable', LBody) > 0) and (AContext.Job.Attempts < 2) then
  begin
    WriteLn(Format('  [HTTP] FAILED - endpoint unreachable (attempt %d)',
      [AContext.Job.Attempts + 1]));
    raise Exception.Create('Connection timeout: endpoint unreachable');
  end;

  WriteLn('  [HTTP] 200 OK - webhook delivered successfully');
end;

var
  Queue: TSidekiqInMemoryQueueAdapter;
  StateStore: ISidekiqStateStore;
begin
  try
    WriteLn('==============================================');
    WriteLn(' Sidekiq4D Demo: Webhook Dispatcher');
    WriteLn('==============================================');
    WriteLn('');
    WriteLn('Configuration:');
    WriteLn('  - Retry: 3 attempts, 2s delay');
    WriteLn('  - Simulates unreachable endpoints');
    WriteLn('');

    Queue := TSidekiqInMemoryQueueAdapter.New;
    StateStore := TSidekiqInMemoryStateStore.New;

    // Enqueue 6 webhook deliveries - some to "unstable" endpoints
    Queue.Enqueue('deliver_webhook',
      '{"url":"https://api.partner.com/hooks/orders","event":"order.created","id":101}');
    Queue.Enqueue('deliver_webhook',
      '{"url":"https://unstable.service.io/callback","event":"payment.received","id":102}');
    Queue.Enqueue('deliver_webhook',
      '{"url":"https://api.analytics.com/ingest","event":"user.signup","id":103}');
    Queue.Enqueue('deliver_webhook',
      '{"url":"https://unstable.legacy.net/webhook","event":"invoice.paid","id":104}');
    Queue.Enqueue('deliver_webhook',
      '{"url":"https://api.crm.com/events","event":"lead.qualified","id":105}');
    Queue.Enqueue('deliver_webhook',
      '{"url":"https://api.notifications.io/push","event":"alert.triggered","id":106}');

    WriteLn('--- Enqueued 6 webhook deliveries ---');
    WriteLn('  (2 endpoints are unstable and will fail initially)');
    WriteLn('');
    WriteLn('--- Processing ---');
    WriteLn('');

    TSidekiqServer.New
      .UseQueue(Queue)
      .BatchSize(10)
      .IdleDelayMs(0)
      .StopWhenIdle
      .StateStore(StateStore)
      .RetryPolicy(TSidekiqSimpleRetryPolicy.New(3, 2))
      .Telemetry(TSidekiqConsoleTelemetry.New)
      .RegisterHandler('deliver_webhook', TWebhookDeliveryHandler.Create)
      .Run;

    WriteLn('');
    WriteLn('Demo complete. Unstable endpoints were retried');
    WriteLn('automatically until successful delivery.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
