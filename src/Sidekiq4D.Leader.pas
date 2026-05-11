unit Sidekiq4D.Leader;

interface

uses
  System.SysUtils,
  System.DateUtils,
  System.SyncObjs,
  Sidekiq4D.Locking;

type
  ISidekiqLeaderElection = interface
    ['{7B96B878-DB80-48E7-B44F-F8BD87BC698D}']
    function EnsureLeadership: Boolean;
    function IsLeader: Boolean;
    function LeaderName: string;
    function OwnerId: string;
    procedure Release;
  end;

  TSidekiqLeaderElection = class(TInterfacedObject, ISidekiqLeaderElection)
  private
    FLockProvider: ISidekiqLockProvider;
    // Configuration fields: set once in constructor. Thread-safe after init.
    FLeaderName: string;
    FLeaseTtlSeconds: Integer;
    FOwnerId: string;
    FHandle: ISidekiqLockHandle;
    FLeaseExpiresAt: TDateTime;
    FLock: TCriticalSection;

    function HasValidLease: Boolean;
    function LeaderKey: string;
    procedure MarkLeaseWindow;
  public
    constructor Create(
      const ALockProvider: ISidekiqLockProvider;
      const ALeaderName: string;
      const ALeaseTtlSeconds: Integer);
    destructor Destroy; override;

    class function New(
      const ALockProvider: ISidekiqLockProvider;
      const ALeaderName: string = 'default';
      const ALeaseTtlSeconds: Integer = 30): ISidekiqLeaderElection;

    function EnsureLeadership: Boolean;
    function IsLeader: Boolean;
    function LeaderName: string;
    function OwnerId: string;
    procedure Release;
  end;

implementation

constructor TSidekiqLeaderElection.Create(
  const ALockProvider: ISidekiqLockProvider;
  const ALeaderName: string;
  const ALeaseTtlSeconds: Integer);
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FLockProvider := ALockProvider;
  FLeaderName := ALeaderName.Trim;
  if FLeaderName.IsEmpty then
    FLeaderName := 'default';
  if ALeaseTtlSeconds > 0 then
    FLeaseTtlSeconds := ALeaseTtlSeconds
  else
    FLeaseTtlSeconds := 30;
  FOwnerId := GuidToString(TGuid.NewGuid);
end;

destructor TSidekiqLeaderElection.Destroy;
begin
  FLock.Free;
  inherited;
end;

function TSidekiqLeaderElection.EnsureLeadership: Boolean;
var
  LRenewableHandle: ISidekiqRenewableLockHandle;
  LHandle: ISidekiqLockHandle;
begin
  FLock.Acquire;
  try
    if not Assigned(FLockProvider) then
      Exit(False);

    if Assigned(FHandle) and HasValidLease then
    begin
      if Supports(FHandle, ISidekiqRenewableLockHandle, LRenewableHandle) then
      begin
        LRenewableHandle.Renew(FLeaseTtlSeconds);
        MarkLeaseWindow;
      end;
      Exit(True);
    end;

    FHandle := nil;
    LHandle := FLockProvider.TryAcquire(LeaderKey, FLeaseTtlSeconds);
    if not Assigned(LHandle) then
      Exit(False);

    FHandle := LHandle;
    MarkLeaseWindow;
    Result := True;
  finally
    FLock.Release;
  end;
end;

function TSidekiqLeaderElection.HasValidLease: Boolean;
begin
  Result := Assigned(FHandle)
    and ((FLeaseExpiresAt = 0) or (Now < FLeaseExpiresAt));
end;

function TSidekiqLeaderElection.IsLeader: Boolean;
begin
  FLock.Acquire;
  try
    Result := HasValidLease;
  finally
    FLock.Release;
  end;
end;

function TSidekiqLeaderElection.LeaderKey: string;
begin
  Result := 'leader:' + FLeaderName;
end;

function TSidekiqLeaderElection.LeaderName: string;
begin
  Result := FLeaderName;
end;

procedure TSidekiqLeaderElection.MarkLeaseWindow;
begin
  FLeaseExpiresAt := IncSecond(Now, FLeaseTtlSeconds);
end;

class function TSidekiqLeaderElection.New(
  const ALockProvider: ISidekiqLockProvider;
  const ALeaderName: string;
  const ALeaseTtlSeconds: Integer): ISidekiqLeaderElection;
begin
  Result := TSidekiqLeaderElection.Create(
    ALockProvider,
    ALeaderName,
    ALeaseTtlSeconds);
end;

function TSidekiqLeaderElection.OwnerId: string;
begin
  Result := FOwnerId;
end;

procedure TSidekiqLeaderElection.Release;
begin
  FLock.Acquire;
  try
    if Assigned(FHandle) then
      FHandle.Release;
    FHandle := nil;
    FLeaseExpiresAt := 0;
  finally
    FLock.Release;
  end;
end;

end.
