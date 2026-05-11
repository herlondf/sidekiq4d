unit Sidekiq4D.Queue.Interfaces;

interface

uses
  System.Classes,
  System.Generics.Collections,
  Sidekiq4D.Job;

type
  TSidekiqFetchOptions = record
    BatchSize: Integer;
    WaitTimeSeconds: Integer;
    VisibilityTimeout: Integer;

    class function Default: TSidekiqFetchOptions; static;
  end;

  TSidekiqPublishRequest = record
    QueueName: string;
    Action: string;
    Body: string;
    DelaySeconds: Integer;
    Attributes: TArray<TPair<string, string>>;
  end;

  ISidekiqQueueAdapter = interface
    ['{CD1E2F6D-F56A-4B4C-BFAD-96DB0D0A7FB7}']
    function Name: string;
    function Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

  ISidekiqQueuePublisher = interface
    ['{FA4884D3-4B90-4D1A-BAB9-9F0A32F7A4F0}']
    procedure Publish(const ARequest: TSidekiqPublishRequest);
  end;

  // Optional batch-publish interface. Adapters that support it can publish
  // N jobs in a single network round-trip (e.g. Redis pipeline, SQS SendMessageBatch).
  // Server detects support via Supports() and prefers batch over individual Publish.
  ISidekiqQueueBatchPublisher = interface
    ['{B7C8D9E0-F1A2-4B3C-8D4E-5F6A7B8C9D0E}']
    procedure PublishBatch(const ARequests: TArray<TSidekiqPublishRequest>);
  end;

  // Pluggable job router. Application implements this interface and registers
  // it via ISidekiqServer.UseJobRouter. Called at publish time so routing
  // logic lives entirely outside Sidekiq4D core.
  ISidekiqJobRouter = interface
    ['{F2A3B4C5-D6E7-4F8A-9B0C-1D2E3F4A5B6C}']
    // Return the target queue name. Return empty string or AQueueName
    // unchanged to leave routing unmodified.
    function Route(
      const AQueueName: string;
      const AAction: string;
      const AAttributes: TArray<TPair<string, string>>): string;
  end;

  // Default no-op router — returns AQueueName unchanged. Used when no
  // custom router is registered; preserves existing behaviour.
  TSidekiqPassThroughRouter = class(TInterfacedObject, ISidekiqJobRouter)
  public
    function Route(
      const AQueueName: string;
      const AAction: string;
      const AAttributes: TArray<TPair<string, string>>): string;
  end;

  ISidekiqQueueLeaseManager = interface
    ['{C5C4A984-046D-48A3-8994-6B8B1B4D0B1D}']
    procedure RenewVisibility(
      const AJob: ISidekiqJobEnvelope;
      const AVisibilityTimeout: Integer);
  end;

  // Opcional: adapters que expõem profundidade da fila implementam esta
  // interface. O dashboard web usa Supports() para ler o valor; adapters
  // que não a implementam retornam -1 (desconhecido) no dashboard.
  ISidekiqQueueDepth = interface
    ['{B3C4D5E6-F7A8-4B9C-8D0E-1F2A3B4C5D6E}']
    function Depth: Integer;
  end;

function MakePublishRequest(
  const AQueueName, AAction, ABody: string;
  const ADelaySeconds: Integer;
  const AAttributes: TStrings): TSidekiqPublishRequest;

procedure ApplyPublishAttributesToStrings(
  const ARequest: TSidekiqPublishRequest;
  const ATarget: TStrings);

implementation

{ TSidekiqPassThroughRouter }

function TSidekiqPassThroughRouter.Route(
  const AQueueName: string;
  const AAction: string;
  const AAttributes: TArray<TPair<string, string>>): string;
begin
  Result := AQueueName;
end;

{ TSidekiqFetchOptions }

class function TSidekiqFetchOptions.Default: TSidekiqFetchOptions;
begin
  Result.BatchSize := 1;
  Result.WaitTimeSeconds := 0;
  Result.VisibilityTimeout := 0;
end;

procedure ApplyPublishAttributesToStrings(
  const ARequest: TSidekiqPublishRequest;
  const ATarget: TStrings);
var
  LIndex: Integer;
begin
  if not Assigned(ATarget) then
    Exit;

  for LIndex := 0 to Pred(Length(ARequest.Attributes)) do
    ATarget.Values[ARequest.Attributes[LIndex].Key] :=
      ARequest.Attributes[LIndex].Value;
end;

function MakePublishRequest(
  const AQueueName, AAction, ABody: string;
  const ADelaySeconds: Integer;
  const AAttributes: TStrings): TSidekiqPublishRequest;
var
  LIndex: Integer;
  LCount: Integer;
  LName: string;
begin
  Result.QueueName := AQueueName;
  Result.Action := AAction;
  Result.Body := ABody;
  Result.DelaySeconds := ADelaySeconds;
  LCount := 0;
  if Assigned(AAttributes) then
  begin
    SetLength(Result.Attributes, AAttributes.Count);
    for LIndex := 0 to Pred(AAttributes.Count) do
    begin
      LName := AAttributes.Names[LIndex];
      if LName = '' then
        Continue;
      Result.Attributes[LCount].Key := LName;
      Result.Attributes[LCount].Value := AAttributes.ValueFromIndex[LIndex];
      Inc(LCount);
    end;
    SetLength(Result.Attributes, LCount);
  end
  else
    SetLength(Result.Attributes, 0);
end;

end.
