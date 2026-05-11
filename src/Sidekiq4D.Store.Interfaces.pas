unit Sidekiq4D.Store.Interfaces;

interface

uses
  System.SysUtils;

type
  ESidekiqStateStore = class(Exception);
  ESidekiqStateStoreNotImplemented = class(ESidekiqStateStore);

  ISidekiqStateStore = interface
    ['{4ECFC636-7918-4C20-8F64-FB06D98A0D18}']
    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

end.
