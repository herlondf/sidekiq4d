unit Hefesto.Leader;

interface

uses
  System.SysUtils,
  System.DateUtils,
  System.SyncObjs,
  Hefesto.Locking;

type
  IHefestoLeaderElection = interface
    ['{7B96B878-DB80-48E7-B44F-F8BD87BC698D}']
    function EnsureLeadership: Boolean;
    function IsLeader: Boolean;
    function LeaderName: string;
    function OwnerId: string;
    procedure Release;
  end;

  THefestoLeaderElection = class(TInterfacedObject, IHefestoLeaderElection)
  private
    FLockProvider: IHefestoLockProvider;
    // Configuration fields: set once in constructor. Thread-safe after init.
    FLeaderName: string;
    FLeaseTtlSeconds: Integer;
    FOwnerId: string;
    FHandle: IHefestoLockHandle;
    FLeaseExpiresAt: TDateTime;
    FLock: TCriticalSection;

    function HasValidLease: Boolean;
    function LeaderKey: string;
    procedure MarkLeaseWindow;
  public
    constructor Create(
      const ALockProvider: IHefestoLockProvider;
      const ALeaderName: string;
      const ALeaseTtlSeconds: Integer);
    destructor Destroy; override;

    class function New(
      const ALockProvider: IHefestoLockProvider;
      const ALeaderName: string = 'default';
      const ALeaseTtlSeconds: Integer = 30): IHefestoLeaderElection;

    function EnsureLeadership: Boolean;
    function IsLeader: Boolean;
    function LeaderName: string;
    function OwnerId: string;
    procedure Release;
  end;

implementation

constructor THefestoLeaderElection.Create(
  const ALockProvider: IHefestoLockProvider;
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

destructor THefestoLeaderElection.Destroy;
begin
  FLock.Free;
  inherited;
end;

function THefestoLeaderElection.EnsureLeadership: Boolean;
var
  LRenewableHandle: IHefestoRenewableLockHandle;
  LHandle: IHefestoLockHandle;
begin
  FLock.Acquire;
  try
    if not Assigned(FLockProvider) then
      Exit(False);

    if Assigned(FHandle) and HasValidLease then
    begin
      if Supports(FHandle, IHefestoRenewableLockHandle, LRenewableHandle) then
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

function THefestoLeaderElection.HasValidLease: Boolean;
begin
  Result := Assigned(FHandle)
    and ((FLeaseExpiresAt = 0) or (Now < FLeaseExpiresAt));
end;

function THefestoLeaderElection.IsLeader: Boolean;
begin
  FLock.Acquire;
  try
    Result := HasValidLease;
  finally
    FLock.Release;
  end;
end;

function THefestoLeaderElection.LeaderKey: string;
begin
  Result := 'leader:' + FLeaderName;
end;

function THefestoLeaderElection.LeaderName: string;
begin
  Result := FLeaderName;
end;

procedure THefestoLeaderElection.MarkLeaseWindow;
begin
  FLeaseExpiresAt := IncSecond(Now, FLeaseTtlSeconds);
end;

class function THefestoLeaderElection.New(
  const ALockProvider: IHefestoLockProvider;
  const ALeaderName: string;
  const ALeaseTtlSeconds: Integer): IHefestoLeaderElection;
begin
  Result := THefestoLeaderElection.Create(
    ALockProvider,
    ALeaderName,
    ALeaseTtlSeconds);
end;

function THefestoLeaderElection.OwnerId: string;
begin
  Result := FOwnerId;
end;

procedure THefestoLeaderElection.Release;
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
