unit Sidekiq4D.Queue.InMemory;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  System.SyncObjs,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces;

type
  TSidekiqInMemoryQueueAdapter = class(TInterfacedObject,
    ISidekiqQueueAdapter,
    ISidekiqQueuePublisher,
    ISidekiqQueueBatchPublisher,
    ISidekiqQueueDepth)
  private
    FName: string;
    FPending: TQueue<ISidekiqJobEnvelope>;
    FAcked: TList<ISidekiqJobEnvelope>;
    FNacked: TList<ISidekiqJobEnvelope>;
    FDeadLettered: TList<ISidekiqJobEnvelope>;
    FLock: TCriticalSection;
  public
    constructor Create(const AName: string = 'default');
    destructor Destroy; override;

    class function New(const AName: string = 'default'): TSidekiqInMemoryQueueAdapter;

    function Enqueue(
      const AAction,
      ABody: string;
      const AAttempts: Integer = 0): TSidekiqInMemoryQueueAdapter;
    function EnqueueWithAttributes(
      const AAction,
      ABody: string;
      const AAttributes: TStrings;
      const AAttempts: Integer = 0): TSidekiqInMemoryQueueAdapter;

    function Name: string;
    function Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure Publish(const ARequest: TSidekiqPublishRequest);
    procedure PublishBatch(const ARequests: TArray<TSidekiqPublishRequest>);

    function PendingCount: Integer;
    function AckedCount: Integer;
    function NackedCount: Integer;
    function DeadLetteredCount: Integer;

    { ISidekiqQueueDepth }
    function Depth: Integer;
  end;

implementation

{ TSidekiqInMemoryQueueAdapter }

procedure TSidekiqInMemoryQueueAdapter.Ack(const AJob: ISidekiqJobEnvelope);
begin
  FLock.Acquire;
  try
    FAcked.Add(AJob);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryQueueAdapter.AckedCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FAcked.Count;
  finally
    FLock.Release;
  end;
end;

constructor TSidekiqInMemoryQueueAdapter.Create(const AName: string);
begin
  inherited Create;
  FName := AName;
  FPending := TQueue<ISidekiqJobEnvelope>.Create;
  FAcked := TList<ISidekiqJobEnvelope>.Create;
  FNacked := TList<ISidekiqJobEnvelope>.Create;
  FDeadLettered := TList<ISidekiqJobEnvelope>.Create;
  FLock := TCriticalSection.Create;
end;

function TSidekiqInMemoryQueueAdapter.DeadLetteredCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FDeadLettered.Count;
  finally
    FLock.Release;
  end;
end;

destructor TSidekiqInMemoryQueueAdapter.Destroy;
begin
  FLock.Free;
  FDeadLettered.Free;
  FNacked.Free;
  FAcked.Free;
  FPending.Free;
  inherited;
end;

function TSidekiqInMemoryQueueAdapter.Enqueue(
  const AAction,
  ABody: string;
  const AAttempts: Integer): TSidekiqInMemoryQueueAdapter;
begin
  Result := EnqueueWithAttributes(AAction, ABody, nil, AAttempts);
end;

function TSidekiqInMemoryQueueAdapter.EnqueueWithAttributes(
  const AAction,
  ABody: string;
  const AAttributes: TStrings;
  const AAttempts: Integer): TSidekiqInMemoryQueueAdapter;
var
  LId: string;
  LEnvelope: TSidekiqJobEnvelope;
  LIndex: Integer;
begin
  Result := Self;
  LId := TGUID.NewGuid.ToString;
  LEnvelope := TSidekiqJobEnvelope.Create(
    LId,
    FName,
    LId,
    ABody,
    AAction,
    AAttempts);

  if Assigned(AAttributes) then
    for LIndex := 0 to Pred(AAttributes.Count) do
      if not AAttributes.Names[LIndex].IsEmpty then
        LEnvelope.AddAttribute(AAttributes.Names[LIndex], AAttributes.ValueFromIndex[LIndex]);

  FLock.Acquire;
  try
    FPending.Enqueue(LEnvelope);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryQueueAdapter.Fetch(
  const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
var
  LCount: Integer;
  LIndex: Integer;
begin
  FLock.Acquire;
  try
    LCount := AOptions.BatchSize;
    if LCount <= 0 then
      LCount := 1;

    if LCount > FPending.Count then
      LCount := FPending.Count;

    SetLength(Result, LCount);
    for LIndex := 0 to Pred(LCount) do
      Result[LIndex] := FPending.Dequeue;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqInMemoryQueueAdapter.MoveToDeadLetter(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
  FLock.Acquire;
  try
    FDeadLettered.Add(AJob);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryQueueAdapter.Name: string;
begin
  Result := FName;
end;

procedure TSidekiqInMemoryQueueAdapter.Nack(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
begin
  FLock.Acquire;
  try
    FNacked.Add(AJob);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryQueueAdapter.NackedCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FNacked.Count;
  finally
    FLock.Release;
  end;
end;

class function TSidekiqInMemoryQueueAdapter.New(
  const AName: string): TSidekiqInMemoryQueueAdapter;
begin
  Result := TSidekiqInMemoryQueueAdapter.Create(AName);
end;

function TSidekiqInMemoryQueueAdapter.PendingCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FPending.Count;
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryQueueAdapter.Depth: Integer;
begin
  Result := PendingCount;
end;

procedure TSidekiqInMemoryQueueAdapter.Publish(
  const ARequest: TSidekiqPublishRequest);
var
  LAttributes: TStringList;
begin
  LAttributes := TStringList.Create;
  try
    ApplyPublishAttributesToStrings(ARequest, LAttributes);
    EnqueueWithAttributes(ARequest.Action, ARequest.Body, LAttributes);
  finally
    LAttributes.Free;
  end;
end;

procedure TSidekiqInMemoryQueueAdapter.PublishBatch(
  const ARequests: TArray<TSidekiqPublishRequest>);
var
  LRequest: TSidekiqPublishRequest;
begin
  for LRequest in ARequests do
    Publish(LRequest);
end;

end.
