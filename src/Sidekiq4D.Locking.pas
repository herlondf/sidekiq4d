unit Sidekiq4D.Locking;

interface

uses
  System.SyncObjs,
  System.SysUtils,
  Sidekiq4D.Store.Interfaces;

type
  ISidekiqLockHandle = interface
    ['{F81597E5-C11C-49BF-83A4-6A41081A1C5D}']
    function Key: string;
    function Token: string;
    procedure Release;
  end;

  ISidekiqRenewableLockHandle = interface(ISidekiqLockHandle)
    ['{8AC64076-052B-4F2D-B52E-F0DFF0A8363F}']
    procedure Renew(const ATtlSeconds: Integer);
  end;

  ISidekiqLockProvider = interface
    ['{40DF5A58-C72D-4D9B-A4D4-053142A8A073}']
    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): ISidekiqLockHandle;
  end;

  ISidekiqRecoverableLockProvider = interface(ISidekiqLockProvider)
    ['{B5E09507-1B88-4198-8CC8-41A25D5C8F66}']
    procedure ForceRelease(const AKey: string);
  end;

  TSidekiqNoopLockHandle = class(TInterfacedObject, ISidekiqLockHandle)
  public
    function Key: string;
    function Token: string;
    procedure Release;
  end;

  TSidekiqNoopLockProvider = class(TInterfacedObject, ISidekiqRecoverableLockProvider)
  public
    class function New: ISidekiqLockProvider;
    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): ISidekiqLockHandle;
    procedure ForceRelease(const AKey: string);
  end;

  TSidekiqStateStoreLockHandle = class(
    TInterfacedObject,
    ISidekiqLockHandle,
    ISidekiqRenewableLockHandle)
  private
    FStateStore: ISidekiqStateStore;
    FKey: string;
    FToken: string;
    FReleased: Integer;
    function StateKey: string;
  public
    constructor Create(
      const AStateStore: ISidekiqStateStore;
      const AKey, AToken: string);

    function Key: string;
    function Token: string;
    procedure Release;
    procedure Renew(const ATtlSeconds: Integer);
  end;

  TSidekiqInMemoryLockProvider = class(
    TInterfacedObject,
    ISidekiqLockProvider,
    ISidekiqRecoverableLockProvider)
  private
    FStateStore: ISidekiqStateStore;
  public
    constructor Create(const AStateStore: ISidekiqStateStore);

    class function New(
      const AStateStore: ISidekiqStateStore): ISidekiqLockProvider;

    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): ISidekiqLockHandle;
    procedure ForceRelease(const AKey: string);
  end;

implementation

uses
  System.StrUtils;

{ TSidekiqNoopLockHandle }

function TSidekiqNoopLockHandle.Key: string;
begin
  Result := EmptyStr;
end;

procedure TSidekiqNoopLockHandle.Release;
begin
end;

function TSidekiqNoopLockHandle.Token: string;
begin
  Result := EmptyStr;
end;

{ TSidekiqNoopLockProvider }

class function TSidekiqNoopLockProvider.New: ISidekiqLockProvider;
begin
  Result := TSidekiqNoopLockProvider.Create as ISidekiqLockProvider;
end;

function TSidekiqNoopLockProvider.TryAcquire(
  const AKey: string;
  const ATtlSeconds: Integer): ISidekiqLockHandle;
begin
  Result := TSidekiqNoopLockHandle.Create;
end;

procedure TSidekiqNoopLockProvider.ForceRelease(const AKey: string);
begin
end;

{ TSidekiqStateStoreLockHandle }

constructor TSidekiqStateStoreLockHandle.Create(
  const AStateStore: ISidekiqStateStore;
  const AKey, AToken: string);
begin
  inherited Create;
  FStateStore := AStateStore;
  FKey := AKey;
  FToken := AToken;
end;

function TSidekiqStateStoreLockHandle.Key: string;
begin
  Result := FKey;
end;

procedure TSidekiqStateStoreLockHandle.Release;
var
  LCurrentToken: string;
begin
  if TInterlocked.CompareExchange(FReleased, 1, 0) <> 0 then
    Exit;
  if not Assigned(FStateStore) then
    Exit;
  LCurrentToken := FStateStore.Get(StateKey);
  if SameText(LCurrentToken, FToken) then
    FStateStore.Delete(StateKey);
end;

procedure TSidekiqStateStoreLockHandle.Renew(const ATtlSeconds: Integer);
var
  LCurrentToken: string;
begin
  if (TInterlocked.CompareExchange(FReleased, 0, 0) <> 0) or not Assigned(FStateStore)
    or (ATtlSeconds <= 0) then
    Exit;

  LCurrentToken := FStateStore.Get(StateKey);
  if SameText(LCurrentToken, FToken) then
    FStateStore.Put(StateKey, FToken, ATtlSeconds);
end;

function TSidekiqStateStoreLockHandle.StateKey: string;
begin
  Result := 'lock:' + FKey;
end;

function TSidekiqStateStoreLockHandle.Token: string;
begin
  Result := FToken;
end;

{ TSidekiqInMemoryLockProvider }

constructor TSidekiqInMemoryLockProvider.Create(
  const AStateStore: ISidekiqStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
end;

class function TSidekiqInMemoryLockProvider.New(
  const AStateStore: ISidekiqStateStore): ISidekiqLockProvider;
var
  LProvider: ISidekiqRecoverableLockProvider;
begin
  LProvider := TSidekiqInMemoryLockProvider.Create(AStateStore);
  Result := LProvider;
end;

function TSidekiqInMemoryLockProvider.TryAcquire(
  const AKey: string;
  const ATtlSeconds: Integer): ISidekiqLockHandle;
var
  LToken: string;
  LHandle: ISidekiqRenewableLockHandle;
begin
  Result := nil;
  if not Assigned(FStateStore) then
    Exit;

  LToken := GuidToString(TGuid.NewGuid);
  if FStateStore.TryPutIfAbsent('lock:' + AKey, LToken, ATtlSeconds) then
  begin
    LHandle := TSidekiqStateStoreLockHandle.Create(FStateStore, AKey, LToken);
    Result := LHandle;
  end;
end;

procedure TSidekiqInMemoryLockProvider.ForceRelease(const AKey: string);
begin
  if Assigned(FStateStore) and not AKey.Trim.IsEmpty then
    FStateStore.Delete('lock:' + AKey);
end;

end.
