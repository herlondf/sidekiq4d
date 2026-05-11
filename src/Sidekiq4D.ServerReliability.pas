unit Sidekiq4D.ServerReliability;

interface

uses
  System.SysUtils,
  Sidekiq4D.Job,
  Sidekiq4D.Metadata,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.Locking,
  Sidekiq4D.Idempotency;

type
  ISidekiqExecutionLease = interface
    ['{9149F7F4-B3BB-47C4-A2FC-3C6B11C90D79}']
  end;

  ISidekiqServerReliability = interface
    ['{63195D9A-B43D-4288-A5CC-BF0D87382062}']
    function RecoverStaleExecutions: Integer;
    function BeginExecution(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AVisibilityTimeout: Integer;
      const AIdempotencyKey: string;
      const ALockHandle: ISidekiqLockHandle): ISidekiqExecutionLease;
  end;

  TSidekiqServerReliability = class(TInterfacedObject, ISidekiqServerReliability)
  private
    FStateStore: ISidekiqStateStore;
    FLockProvider: ISidekiqLockProvider;
    FIdempotency: ISidekiqIdempotency;

    class function ActiveLeaseKey(const AExecutionId: string): string; static;
    class function RegistryLeaseKey(const AExecutionId: string): string; static;
    class function ResolveLeaseTtl(
      const AJob: ISidekiqJobEnvelope;
      const AVisibilityTimeout: Integer): Integer; static;
    class function ResolveHeartbeatInterval(
      const AJob: ISidekiqJobEnvelope;
      const ALeaseTtlSeconds: Integer): Integer; static;
  public
    constructor Create(
      const AStateStore: ISidekiqStateStore;
      const ALockProvider: ISidekiqLockProvider;
      const AIdempotency: ISidekiqIdempotency);

    class function New(
      const AStateStore: ISidekiqStateStore;
      const ALockProvider: ISidekiqLockProvider;
      const AIdempotency: ISidekiqIdempotency): ISidekiqServerReliability;

    function RecoverStaleExecutions: Integer;
    function BeginExecution(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AVisibilityTimeout: Integer;
      const AIdempotencyKey: string;
      const ALockHandle: ISidekiqLockHandle): ISidekiqExecutionLease;
  end;

implementation

uses
  System.Classes,
  System.SyncObjs,
  System.JSON,
  System.Math;

type
  TSidekiqExecutionLeasePayload = record
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
    class function FromJson(const AValue: string): TSidekiqExecutionLeasePayload; static;
  end;

  TSidekiqExecutionLeaseHandle = class(TInterfacedObject, ISidekiqExecutionLease)
  private
    FPayload: TSidekiqExecutionLeasePayload;
    FStateStore: ISidekiqStateStore;
    FQueue: ISidekiqQueueAdapter;
    FJob: ISidekiqJobEnvelope;
    FIdempotency: ISidekiqIdempotency;
    FLockHandle: ISidekiqLockHandle;
    FStopEvent: TEvent;
    FHeartbeatThread: TThread;

    procedure StartHeartbeat;
    procedure TouchLease;
  public
    constructor Create(
      const APayload: TSidekiqExecutionLeasePayload;
      const AStateStore: ISidekiqStateStore;
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AIdempotency: ISidekiqIdempotency;
      const ALockHandle: ISidekiqLockHandle);
    destructor Destroy; override;
  end;

{ TSidekiqExecutionLeasePayload }

class function TSidekiqExecutionLeasePayload.FromJson(
  const AValue: string): TSidekiqExecutionLeasePayload;
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

function TSidekiqExecutionLeasePayload.ToJson: string;
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

{ TSidekiqExecutionLeaseHandle }

constructor TSidekiqExecutionLeaseHandle.Create(
  const APayload: TSidekiqExecutionLeasePayload;
  const AStateStore: ISidekiqStateStore;
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AIdempotency: ISidekiqIdempotency;
  const ALockHandle: ISidekiqLockHandle);
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

destructor TSidekiqExecutionLeaseHandle.Destroy;
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
    FStateStore.Delete(TSidekiqServerReliability.ActiveLeaseKey(FPayload.ExecutionId));
    FStateStore.Delete(TSidekiqServerReliability.RegistryLeaseKey(FPayload.ExecutionId));
  end;
  inherited;
end;

procedure TSidekiqExecutionLeaseHandle.StartHeartbeat;
begin
  if FPayload.HeartbeatIntervalSeconds <= 0 then
    Exit;

  FHeartbeatThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLeaseManager: ISidekiqQueueLeaseManager;
      LRenewableLock: ISidekiqRenewableLockHandle;
      LRenewableIdempotency: ISidekiqRenewableIdempotency;
    begin
      while FStopEvent.WaitFor(FPayload.HeartbeatIntervalSeconds * 1000) = wrTimeout do
      begin
        if Supports(FQueue, ISidekiqQueueLeaseManager, LLeaseManager)
          and (FPayload.VisibilityTimeout > 0) then
          LLeaseManager.RenewVisibility(FJob, FPayload.VisibilityTimeout);

        if Supports(FLockHandle, ISidekiqRenewableLockHandle, LRenewableLock) then
          LRenewableLock.Renew(FPayload.LeaseTtlSeconds);

        if Supports(FIdempotency, ISidekiqRenewableIdempotency, LRenewableIdempotency)
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

procedure TSidekiqExecutionLeaseHandle.TouchLease;
const
  REGISTRY_TTL_SECONDS = 86400;
begin
  if not Assigned(FStateStore) then
    Exit;

  FStateStore.Put(
    TSidekiqServerReliability.ActiveLeaseKey(FPayload.ExecutionId),
    FPayload.ToJson,
    FPayload.LeaseTtlSeconds);
  FStateStore.Put(
    TSidekiqServerReliability.RegistryLeaseKey(FPayload.ExecutionId),
    FPayload.ToJson,
    Max(REGISTRY_TTL_SECONDS, FPayload.LeaseTtlSeconds * 4));
end;

{ TSidekiqServerReliability }

class function TSidekiqServerReliability.ActiveLeaseKey(
  const AExecutionId: string): string;
begin
  Result := 'server_reliability:active:' + AExecutionId;
end;

function TSidekiqServerReliability.BeginExecution(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AVisibilityTimeout: Integer;
  const AIdempotencyKey: string;
  const ALockHandle: ISidekiqLockHandle): ISidekiqExecutionLease;
var
  LPayload: TSidekiqExecutionLeasePayload;
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

  Result := TSidekiqExecutionLeaseHandle.Create(
    LPayload,
    FStateStore,
    AQueue,
    AJob,
    FIdempotency,
    ALockHandle);
end;

constructor TSidekiqServerReliability.Create(
  const AStateStore: ISidekiqStateStore;
  const ALockProvider: ISidekiqLockProvider;
  const AIdempotency: ISidekiqIdempotency);
begin
  inherited Create;
  FStateStore := AStateStore;
  FLockProvider := ALockProvider;
  FIdempotency := AIdempotency;
end;

class function TSidekiqServerReliability.New(
  const AStateStore: ISidekiqStateStore;
  const ALockProvider: ISidekiqLockProvider;
  const AIdempotency: ISidekiqIdempotency): ISidekiqServerReliability;
begin
  Result := TSidekiqServerReliability.Create(
    AStateStore,
    ALockProvider,
    AIdempotency);
end;

function TSidekiqServerReliability.RecoverStaleExecutions: Integer;
var
  LRegistryKey: string;
  LExecutionId: string;
  LPayload: TSidekiqExecutionLeasePayload;
  LRecoverableLocks: ISidekiqRecoverableLockProvider;
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

    LPayload := TSidekiqExecutionLeasePayload.FromJson(
      FStateStore.Get(LRegistryKey));
    if not LPayload.IdempotencyKey.IsEmpty and Assigned(FIdempotency) then
      FIdempotency.Clear(LPayload.IdempotencyKey);

    if not LPayload.LockKey.IsEmpty
      and Supports(FLockProvider, ISidekiqRecoverableLockProvider, LRecoverableLocks) then
      LRecoverableLocks.ForceRelease(LPayload.LockKey);

    FStateStore.Delete(LRegistryKey);
    Inc(Result);
  end;
end;

class function TSidekiqServerReliability.RegistryLeaseKey(
  const AExecutionId: string): string;
begin
  Result := 'server_reliability:registry:' + AExecutionId;
end;

class function TSidekiqServerReliability.ResolveHeartbeatInterval(
  const AJob: ISidekiqJobEnvelope;
  const ALeaseTtlSeconds: Integer): Integer;
begin
  Result := StrToIntDef(
    AJob.Attribute(TSidekiqJobAttribute.HeartbeatIntervalSeconds),
    0);
  if Result > 0 then
    Exit;

  if ALeaseTtlSeconds <= 10 then
    Result := 1
  else
    Result := Max(5, ALeaseTtlSeconds div 2);
end;

class function TSidekiqServerReliability.ResolveLeaseTtl(
  const AJob: ISidekiqJobEnvelope;
  const AVisibilityTimeout: Integer): Integer;
begin
  Result := StrToIntDef(
    AJob.Attribute(TSidekiqJobAttribute.ServerLeaseTtlSeconds),
    0);
  if Result > 0 then
    Exit;

  if AVisibilityTimeout > 0 then
    Exit(AVisibilityTimeout);

  Result := 300;
end;

end.
