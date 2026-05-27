unit Hefesto.Queue.InMemory;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  System.SyncObjs,
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  THefestoInMemoryQueueAdapter = class(TInterfacedObject,
    IHefestoQueueAdapter,
    IHefestoQueuePublisher,
    IHefestoQueueBatchPublisher,
    IHefestoQueueDepth)
  private
    FName: string;
    FPending: TQueue<IHefestoJobEnvelope>;
    FAcked: TList<IHefestoJobEnvelope>;
    FNacked: TList<IHefestoJobEnvelope>;
    FDeadLettered: TList<IHefestoJobEnvelope>;
    FLock: TCriticalSection;
  public
    constructor Create(const AName: string = 'default');
    destructor Destroy; override;

    class function New(const AName: string = 'default'): THefestoInMemoryQueueAdapter;

    function Enqueue(
      const AAction,
      ABody: string;
      const AAttempts: Integer = 0): THefestoInMemoryQueueAdapter;
    function EnqueueWithAttributes(
      const AAction,
      ABody: string;
      const AAttributes: TStrings;
      const AAttempts: Integer = 0): THefestoInMemoryQueueAdapter;

    function Name: string;
    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);
    procedure Publish(const ARequest: THefestoPublishRequest);
    procedure PublishBatch(const ARequests: TArray<THefestoPublishRequest>);

    function PendingCount: Integer;
    function AckedCount: Integer;
    function NackedCount: Integer;
    function DeadLetteredCount: Integer;

    { IHefestoQueueDepth }
    function Depth: Integer;
  end;

implementation

{ THefestoInMemoryQueueAdapter }

procedure THefestoInMemoryQueueAdapter.Ack(const AJob: IHefestoJobEnvelope);
begin
  FLock.Acquire;
  try
    FAcked.Add(AJob);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryQueueAdapter.AckedCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FAcked.Count;
  finally
    FLock.Release;
  end;
end;

constructor THefestoInMemoryQueueAdapter.Create(const AName: string);
begin
  inherited Create;
  FName := AName;
  FPending := TQueue<IHefestoJobEnvelope>.Create;
  FAcked := TList<IHefestoJobEnvelope>.Create;
  FNacked := TList<IHefestoJobEnvelope>.Create;
  FDeadLettered := TList<IHefestoJobEnvelope>.Create;
  FLock := TCriticalSection.Create;
end;

function THefestoInMemoryQueueAdapter.DeadLetteredCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FDeadLettered.Count;
  finally
    FLock.Release;
  end;
end;

destructor THefestoInMemoryQueueAdapter.Destroy;
begin
  FLock.Free;
  FDeadLettered.Free;
  FNacked.Free;
  FAcked.Free;
  FPending.Free;
  inherited;
end;

function THefestoInMemoryQueueAdapter.Enqueue(
  const AAction,
  ABody: string;
  const AAttempts: Integer): THefestoInMemoryQueueAdapter;
begin
  Result := EnqueueWithAttributes(AAction, ABody, nil, AAttempts);
end;

function THefestoInMemoryQueueAdapter.EnqueueWithAttributes(
  const AAction,
  ABody: string;
  const AAttributes: TStrings;
  const AAttempts: Integer): THefestoInMemoryQueueAdapter;
var
  LId: string;
  LEnvelope: THefestoJobEnvelope;
  LIndex: Integer;
begin
  Result := Self;
  LId := TGUID.NewGuid.ToString;
  LEnvelope := THefestoJobEnvelope.Create(
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

function THefestoInMemoryQueueAdapter.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
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

procedure THefestoInMemoryQueueAdapter.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope;
  const AReason: string);
begin
  FLock.Acquire;
  try
    FDeadLettered.Add(AJob);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryQueueAdapter.Name: string;
begin
  Result := FName;
end;

procedure THefestoInMemoryQueueAdapter.Nack(
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer);
begin
  FLock.Acquire;
  try
    FNacked.Add(AJob);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryQueueAdapter.NackedCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FNacked.Count;
  finally
    FLock.Release;
  end;
end;

class function THefestoInMemoryQueueAdapter.New(
  const AName: string): THefestoInMemoryQueueAdapter;
begin
  Result := THefestoInMemoryQueueAdapter.Create(AName);
end;

function THefestoInMemoryQueueAdapter.PendingCount: Integer;
begin
  FLock.Acquire;
  try
    Result := FPending.Count;
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryQueueAdapter.Depth: Integer;
begin
  Result := PendingCount;
end;

procedure THefestoInMemoryQueueAdapter.Publish(
  const ARequest: THefestoPublishRequest);
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

procedure THefestoInMemoryQueueAdapter.PublishBatch(
  const ARequests: TArray<THefestoPublishRequest>);
var
  LRequest: THefestoPublishRequest;
begin
  for LRequest in ARequests do
    Publish(LRequest);
end;

end.
