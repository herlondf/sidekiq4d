unit Hefesto.ServerReliability;

interface

uses
  System.SysUtils,
  Hefesto.Job,
  Hefesto.Metadata,
  Hefesto.Queue.Interfaces,
  Hefesto.Store.Interfaces,
  Hefesto.Locking,
  Hefesto.Idempotency;

type
  IHefestoExecutionLease = interface
    ['{9149F7F4-B3BB-47C4-A2FC-3C6B11C90D79}']
  end;

  IHefestoServerReliability = interface
    ['{63195D9A-B43D-4288-A5CC-BF0D87382062}']
    function RecoverStaleExecutions: Integer;
    function BeginExecution(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer;
      const AIdempotencyKey: string;
      const ALockHandle: IHefestoLockHandle): IHefestoExecutionLease;
  end;

  THefestoServerReliability = class(TInterfacedObject, IHefestoServerReliability)
  private
    FStateStore: IHefestoStateStore;
    FLockProvider: IHefestoLockProvider;
    FIdempotency: IHefestoIdempotency;

    class function ActiveLeaseKey(const AExecutionId: string): string; static;
    class function RegistryLeaseKey(const AExecutionId: string): string; static;
    class function ResolveLeaseTtl(
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer): Integer; static;
    class function ResolveHeartbeatInterval(
      const AJob: IHefestoJobEnvelope;
      const ALeaseTtlSeconds: Integer): Integer; static;
  public
    constructor Create(
      const AStateStore: IHefestoStateStore;
      const ALockProvider: IHefestoLockProvider;
      const AIdempotency: IHefestoIdempotency);

    class function New(
      const AStateStore: IHefestoStateStore;
      const ALockProvider: IHefestoLockProvider;
      const AIdempotency: IHefestoIdempotency): IHefestoServerReliability;

    function RecoverStaleExecutions: Integer;
    function BeginExecution(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AVisibilityTimeout: Integer;
      const AIdempotencyKey: string;
      const ALockHandle: IHefestoLockHandle): IHefestoExecutionLease;
  end;

implementation

uses
  System.Classes,
  System.SyncObjs,
  System.JSON,
  System.Math;

type
  THefestoExecutionLeasePayload = record
    ExecutionId: string;
    JobId: string;
    QueueName: string;
    Action: string;
    IdempotencyKey: string;
    LockKey: string;
    LeaseTtlSeconds: Integer;
    HeartbeatIntervalSeconds: Integer;
    VisibilityTimeout: Integer;

    function ToJson: string;
    class function FromJson(const AValue: string): THefestoExecutionLeasePayload; static;
  end;

  THefestoExecutionLeaseHandle = class(TInterfacedObject, IHefestoExecutionLease)
  private
    FPayload: THefestoExecutionLeasePayload;
    FStateStore: IHefestoStateStore;
    FQueue: IHefestoQueueAdapter;
    FJob: IHefestoJobEnvelope;
    FIdempotency: IHefestoIdempotency;
    FLockHandle: IHefestoLockHandle;
    FStopEvent: TEvent;
    FHeartbeatThread: TThread;

    procedure StartHeartbeat;
    procedure TouchLease;
  public
    constructor Create(
      const APayload: THefestoExecutionLeasePayload;
      const AStateStore: IHefestoStateStore;
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AIdempotency: IHefestoIdempotency;
      const ALockHandle: IHefestoLockHandle);
    destructor Destroy; override;
  end;

{ THefestoExecutionLeasePayload }

class function THefestoExecutionLeasePayload.FromJson(
  const AValue: string): THefestoExecutionLeasePayload;
var
  LJsonValue: TJSONValue;
  LJson: TJSONObject;
begin
  FillChar(Result, SizeOf(Result), 0);
  LJsonValue := TJSONObject.ParseJSONValue(AValue);
  try
    if not (LJsonValue is TJSONObject) then
      Exit;

    LJson := TJSONObject(LJsonValue);
    Result.ExecutionId := LJson.GetValue<string>('execution_id', '');
    Result.JobId := LJson.GetValue<string>('job_id', '');
    Result.QueueName := LJson.GetValue<string>('queue_name', '');
    Result.Action := LJson.GetValue<string>('action', '');
    Result.IdempotencyKey := LJson.GetValue<string>('idempotency_key', '');
    Result.LockKey := LJson.GetValue<string>('lock_key', '');
    Result.LeaseTtlSeconds := LJson.GetValue<Integer>('lease_ttl_seconds', 0);
    Result.HeartbeatIntervalSeconds := LJson.GetValue<Integer>('heartbeat_interval_seconds', 0);
    Result.VisibilityTimeout := LJson.GetValue<Integer>('visibility_timeout', 0);
  finally
    LJsonValue.Free;
  end;
end;

function THefestoExecutionLeasePayload.ToJson: string;
var
  LJson: TJSONObject;
begin
  LJson := TJSONObject.Create;
  try
    LJson.AddPair('execution_id', ExecutionId);
    LJson.AddPair('job_id', JobId);
    LJson.AddPair('queue_name', QueueName);
    LJson.AddPair('action', Action);
    LJson.AddPair('idempotency_key', IdempotencyKey);
    LJson.AddPair('lock_key', LockKey);
    LJson.AddPair('lease_ttl_seconds', TJSONNumber.Create(LeaseTtlSeconds));
    LJson.AddPair('heartbeat_interval_seconds', TJSONNumber.Create(HeartbeatIntervalSeconds));
    LJson.AddPair('visibility_timeout', TJSONNumber.Create(VisibilityTimeout));
    Result := LJson.ToJSON;
  finally
    LJson.Free;
  end;
end;

{ THefestoExecutionLeaseHandle }

constructor THefestoExecutionLeaseHandle.Create(
  const APayload: THefestoExecutionLeasePayload;
  const AStateStore: IHefestoStateStore;
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AIdempotency: IHefestoIdempotency;
  const ALockHandle: IHefestoLockHandle);
begin
  inherited Create;
  FPayload := APayload;
  FStateStore := AStateStore;
  FQueue := AQueue;
  FJob := AJob;
  FIdempotency := AIdempotency;
  FLockHandle := ALockHandle;
  FStopEvent := TEvent.Create(nil, True, False, '');
  TouchLease;
  StartHeartbeat;
end;

destructor THefestoExecutionLeaseHandle.Destroy;
begin
  FStopEvent.SetEvent;
  if Assigned(FHeartbeatThread) then
  begin
    FHeartbeatThread.WaitFor;
    FHeartbeatThread.Free;
  end;
  FStopEvent.Free;

  if Assigned(FStateStore) then
  begin
    FStateStore.Delete(THefestoServerReliability.ActiveLeaseKey(FPayload.ExecutionId));
    FStateStore.Delete(THefestoServerReliability.RegistryLeaseKey(FPayload.ExecutionId));
  end;
  inherited;
end;

procedure THefestoExecutionLeaseHandle.StartHeartbeat;
begin
  if FPayload.HeartbeatIntervalSeconds <= 0 then
    Exit;

  FHeartbeatThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLeaseManager: IHefestoQueueLeaseManager;
      LRenewableLock: IHefestoRenewableLockHandle;
      LRenewableIdempotency: IHefestoRenewableIdempotency;
    begin
      while FStopEvent.WaitFor(FPayload.HeartbeatIntervalSeconds * 1000) = wrTimeout do
      begin
        if Supports(FQueue, IHefestoQueueLeaseManager, LLeaseManager)
          and (FPayload.VisibilityTimeout > 0) then
          LLeaseManager.RenewVisibility(FJob, FPayload.VisibilityTimeout);

        if Supports(FLockHandle, IHefestoRenewableLockHandle, LRenewableLock) then
          LRenewableLock.Renew(FPayload.LeaseTtlSeconds);

        if Supports(FIdempotency, IHefestoRenewableIdempotency, LRenewableIdempotency)
          and not FPayload.IdempotencyKey.IsEmpty then
          LRenewableIdempotency.Renew(
            FPayload.IdempotencyKey,
            FPayload.LeaseTtlSeconds);

        TouchLease;
      end;
    end);
  FHeartbeatThread.FreeOnTerminate := False;
  FHeartbeatThread.Start;
end;

procedure THefestoExecutionLeaseHandle.TouchLease;
const
  REGISTRY_TTL_SECONDS = 86400;
begin
  if not Assigned(FStateStore) then
    Exit;

  FStateStore.Put(
    THefestoServerReliability.ActiveLeaseKey(FPayload.ExecutionId),
    FPayload.ToJson,
    FPayload.LeaseTtlSeconds);
  FStateStore.Put(
    THefestoServerReliability.RegistryLeaseKey(FPayload.ExecutionId),
    FPayload.ToJson,
    Max(REGISTRY_TTL_SECONDS, FPayload.LeaseTtlSeconds * 4));
end;

{ THefestoServerReliability }

class function THefestoServerReliability.ActiveLeaseKey(
  const AExecutionId: string): string;
begin
  Result := 'server_reliability:active:' + AExecutionId;
end;

function THefestoServerReliability.BeginExecution(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AVisibilityTimeout: Integer;
  const AIdempotencyKey: string;
  const ALockHandle: IHefestoLockHandle): IHefestoExecutionLease;
var
  LPayload: THefestoExecutionLeasePayload;
begin
  if not Assigned(FStateStore) then
    Exit(nil);

  LPayload.ExecutionId := GuidToString(TGuid.NewGuid);
  LPayload.JobId := AJob.Id;
  LPayload.QueueName := AJob.QueueName;
  LPayload.Action := AJob.Action;
  LPayload.IdempotencyKey := AIdempotencyKey;
  if Assigned(ALockHandle) then
    LPayload.LockKey := ALockHandle.Key
  else
    LPayload.LockKey := '';
  LPayload.LeaseTtlSeconds := ResolveLeaseTtl(AJob, AVisibilityTimeout);
  LPayload.HeartbeatIntervalSeconds := ResolveHeartbeatInterval(
    AJob,
    LPayload.LeaseTtlSeconds);
  LPayload.VisibilityTimeout := AVisibilityTimeout;

  Result := THefestoExecutionLeaseHandle.Create(
    LPayload,
    FStateStore,
    AQueue,
    AJob,
    FIdempotency,
    ALockHandle);
end;

constructor THefestoServerReliability.Create(
  const AStateStore: IHefestoStateStore;
  const ALockProvider: IHefestoLockProvider;
  const AIdempotency: IHefestoIdempotency);
begin
  inherited Create;
  FStateStore := AStateStore;
  FLockProvider := ALockProvider;
  FIdempotency := AIdempotency;
end;

class function THefestoServerReliability.New(
  const AStateStore: IHefestoStateStore;
  const ALockProvider: IHefestoLockProvider;
  const AIdempotency: IHefestoIdempotency): IHefestoServerReliability;
begin
  Result := THefestoServerReliability.Create(
    AStateStore,
    ALockProvider,
    AIdempotency);
end;

function THefestoServerReliability.RecoverStaleExecutions: Integer;
var
  LRegistryKey: string;
  LExecutionId: string;
  LPayload: THefestoExecutionLeasePayload;
  LRecoverableLocks: IHefestoRecoverableLockProvider;
begin
  Result := 0;
  if not Assigned(FStateStore) then
    Exit;

  for LRegistryKey in FStateStore.ListKeys('server_reliability:registry:') do
  begin
    LExecutionId := Copy(
      LRegistryKey,
      Length('server_reliability:registry:') + 1,
      MaxInt);
    if FStateStore.Exists(ActiveLeaseKey(LExecutionId)) then
      Continue;

    LPayload := THefestoExecutionLeasePayload.FromJson(
      FStateStore.Get(LRegistryKey));
    if not LPayload.IdempotencyKey.IsEmpty and Assigned(FIdempotency) then
      FIdempotency.Clear(LPayload.IdempotencyKey);

    if not LPayload.LockKey.IsEmpty
      and Supports(FLockProvider, IHefestoRecoverableLockProvider, LRecoverableLocks) then
      LRecoverableLocks.ForceRelease(LPayload.LockKey);

    FStateStore.Delete(LRegistryKey);
    Inc(Result);
  end;
end;

class function THefestoServerReliability.RegistryLeaseKey(
  const AExecutionId: string): string;
begin
  Result := 'server_reliability:registry:' + AExecutionId;
end;

class function THefestoServerReliability.ResolveHeartbeatInterval(
  const AJob: IHefestoJobEnvelope;
  const ALeaseTtlSeconds: Integer): Integer;
begin
  Result := StrToIntDef(
    AJob.Attribute(THefestoJobAttribute.HeartbeatIntervalSeconds),
    0);
  if Result > 0 then
    Exit;

  if ALeaseTtlSeconds <= 10 then
    Result := 1
  else
    Result := Max(5, ALeaseTtlSeconds div 2);
end;

class function THefestoServerReliability.ResolveLeaseTtl(
  const AJob: IHefestoJobEnvelope;
  const AVisibilityTimeout: Integer): Integer;
begin
  Result := StrToIntDef(
    AJob.Attribute(THefestoJobAttribute.ServerLeaseTtlSeconds),
    0);
  if Result > 0 then
    Exit;

  if AVisibilityTimeout > 0 then
    Exit(AVisibilityTimeout);

  Result := 300;
end;

end.
