unit Hefesto.Locking;

interface

uses
  System.SyncObjs,
  System.SysUtils,
  Hefesto.Store.Interfaces;

type
  IHefestoLockHandle = interface
    ['{F81597E5-C11C-49BF-83A4-6A41081A1C5D}']
    function Key: string;
    function Token: string;
    procedure Release;
  end;

  IHefestoRenewableLockHandle = interface(IHefestoLockHandle)
    ['{8AC64076-052B-4F2D-B52E-F0DFF0A8363F}']
    procedure Renew(const ATtlSeconds: Integer);
  end;

  IHefestoLockProvider = interface
    ['{40DF5A58-C72D-4D9B-A4D4-053142A8A073}']
    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): IHefestoLockHandle;
  end;

  IHefestoRecoverableLockProvider = interface(IHefestoLockProvider)
    ['{B5E09507-1B88-4198-8CC8-41A25D5C8F66}']
    procedure ForceRelease(const AKey: string);
  end;

  THefestoNoopLockHandle = class(TInterfacedObject, IHefestoLockHandle)
  public
    function Key: string;
    function Token: string;
    procedure Release;
  end;

  THefestoNoopLockProvider = class(TInterfacedObject, IHefestoRecoverableLockProvider)
  public
    class function New: IHefestoLockProvider;
    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): IHefestoLockHandle;
    procedure ForceRelease(const AKey: string);
  end;

  THefestoStateStoreLockHandle = class(
    TInterfacedObject,
    IHefestoLockHandle,
    IHefestoRenewableLockHandle)
  private
    FStateStore: IHefestoStateStore;
    FKey: string;
    FToken: string;
    FReleased: Integer;
    function StateKey: string;
  public
    constructor Create(
      const AStateStore: IHefestoStateStore;
      const AKey, AToken: string);

    function Key: string;
    function Token: string;
    procedure Release;
    procedure Renew(const ATtlSeconds: Integer);
  end;

  THefestoInMemoryLockProvider = class(
    TInterfacedObject,
    IHefestoLockProvider,
    IHefestoRecoverableLockProvider)
  private
    FStateStore: IHefestoStateStore;
  public
    constructor Create(const AStateStore: IHefestoStateStore);

    class function New(
      const AStateStore: IHefestoStateStore): IHefestoLockProvider;

    function TryAcquire(
      const AKey: string;
      const ATtlSeconds: Integer = 30): IHefestoLockHandle;
    procedure ForceRelease(const AKey: string);
  end;

implementation

uses
  System.StrUtils;

{ THefestoNoopLockHandle }

function THefestoNoopLockHandle.Key: string;
begin
  Result := EmptyStr;
end;

procedure THefestoNoopLockHandle.Release;
begin
end;

function THefestoNoopLockHandle.Token: string;
begin
  Result := EmptyStr;
end;

{ THefestoNoopLockProvider }

class function THefestoNoopLockProvider.New: IHefestoLockProvider;
begin
  Result := THefestoNoopLockProvider.Create as IHefestoLockProvider;
end;

function THefestoNoopLockProvider.TryAcquire(
  const AKey: string;
  const ATtlSeconds: Integer): IHefestoLockHandle;
begin
  Result := THefestoNoopLockHandle.Create;
end;

procedure THefestoNoopLockProvider.ForceRelease(const AKey: string);
begin
end;

{ THefestoStateStoreLockHandle }

constructor THefestoStateStoreLockHandle.Create(
  const AStateStore: IHefestoStateStore;
  const AKey, AToken: string);
begin
  inherited Create;
  FStateStore := AStateStore;
  FKey := AKey;
  FToken := AToken;
end;

function THefestoStateStoreLockHandle.Key: string;
begin
  Result := FKey;
end;

procedure THefestoStateStoreLockHandle.Release;
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

procedure THefestoStateStoreLockHandle.Renew(const ATtlSeconds: Integer);
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

function THefestoStateStoreLockHandle.StateKey: string;
begin
  Result := 'lock:' + FKey;
end;

function THefestoStateStoreLockHandle.Token: string;
begin
  Result := FToken;
end;

{ THefestoInMemoryLockProvider }

constructor THefestoInMemoryLockProvider.Create(
  const AStateStore: IHefestoStateStore);
begin
  inherited Create;
  FStateStore := AStateStore;
end;

class function THefestoInMemoryLockProvider.New(
  const AStateStore: IHefestoStateStore): IHefestoLockProvider;
var
  LProvider: IHefestoRecoverableLockProvider;
begin
  LProvider := THefestoInMemoryLockProvider.Create(AStateStore);
  Result := LProvider;
end;

function THefestoInMemoryLockProvider.TryAcquire(
  const AKey: string;
  const ATtlSeconds: Integer): IHefestoLockHandle;
var
  LToken: string;
  LHandle: IHefestoRenewableLockHandle;
begin
  Result := nil;
  if not Assigned(FStateStore) then
    Exit;

  LToken := GuidToString(TGuid.NewGuid);
  if FStateStore.TryPutIfAbsent('lock:' + AKey, LToken, ATtlSeconds) then
  begin
    LHandle := THefestoStateStoreLockHandle.Create(FStateStore, AKey, LToken);
    Result := LHandle;
  end;
end;

procedure THefestoInMemoryLockProvider.ForceRelease(const AKey: string);
begin
  if Assigned(FStateStore) and not AKey.Trim.IsEmpty then
    FStateStore.Delete('lock:' + AKey);
end;

end.
