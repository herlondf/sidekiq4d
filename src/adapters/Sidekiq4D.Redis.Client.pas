unit Sidekiq4D.Redis.Client;

// Minimal Redis client interface used by all Sidekiq4D Redis adapters.
// Zero third-party dependencies — implement with any Redis library.
// See Sidekiq4D.Redis4D.Client for the bundled Redis4D bridge.

interface

uses
  System.SysUtils,
  System.Classes;

type
  // Field in a Redis Stream message
  TSidekiqStreamField = record
    Name: string;
    Value: string;
  end;

  // Message returned by ISidekiqRedisStreams.ReadGroup
  TSidekiqStreamMessage = record
    ID: string;
    Fields: TArray<TSidekiqStreamField>;
    function FieldValue(const AName: string): string;
    function HasField(const AName: string): Boolean;
    function IndexOfField(const AName: string): Integer;
  end;

  // Minimal Redis Streams interface
  ISidekiqRedisStreams = interface
    ['{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}']
    procedure CreateGroup(
      const AStreamKey, AGroupName, AStartID: string);
    function ReadGroup(
      const AGroupName, AConsumerName, AStreamKey, AStart: string;
      ACount, AWaitMs: Integer): TArray<TSidekiqStreamMessage>;
    procedure Acknowledge(
      const AStreamKey, AGroupName: string;
      const AMessageIDs: TArray<string>);
    procedure Add(
      const AStreamKey, AMessageID: string;
      const AFields: TStrings);
  end;

  // Pipeline handle — queue multiple commands and fire in a single round-trip.
  // Usage:
  //   var P := Client.BeginPipeline;
  //   P.LPush('q:default', 'job1').LPush('q:default', 'job2');
  //   P.Execute; // one network round-trip
  ISidekiqRedisPipeline = interface
    ['{C3D7E1F0-A2B4-4C6D-8E9F-0A1B2C3D4E5F}']
    function LPush(const AKey, AValue: string): ISidekiqRedisPipeline;
    function ZAdd(const AKey: string; AScore: Double; const AMember: string): ISidekiqRedisPipeline;
    // Queue any raw Redis command (e.g. XADD, INCR, DEL …)
    function QueueCommand(const ACommand: TArray<string>): ISidekiqRedisPipeline;
    // Fire all queued commands in one round-trip. Safe to call with 0 commands (no-op).
    procedure Execute;
  end;

  // Minimal Redis client interface — all operations used by Sidekiq4D adapters.
  // Covers KV, sorted sets, lists, Lua scripting and Streams.
  ISidekiqRedisClient = interface
    ['{F0E1D2C3-B4A5-9687-7869-5A4B3C2D1E0F}']
    // Key-value
    function Get(const AKey: string): string;
    procedure SetEx(const AKey, AValue: string; ATtlSeconds: Integer);
    function SetNX(const AKey, AValue: string; ATtlSeconds: Integer): Boolean;
    procedure Del(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function Keys(const APattern: string): TArray<string>;
    // Sorted sets
    procedure ZAdd(const AKey: string; AScore: Double; const AMember: string);
    function ZCard(const AKey: string): Integer;
    // Lists
    procedure RPush(const AKey: string; const AValues: TArray<string>);
    function LRange(const AKey: string; AStart, AStop: Integer): TArray<string>;
    procedure LRem(const AKey: string; ACount: Integer; const AValue: string);
    function LLen(const AKey: string): Integer;
    // Pipeline — N commands in one round-trip
    function BeginPipeline: ISidekiqRedisPipeline;
    // Batch LLEN — N keys in one round-trip; returns 0 for missing keys
    function LLenBatch(const AKeys: TArray<string>): TArray<Integer>;
    // Lua scripting — returns array of strings (empty if result is not an array)
    function Eval(
      const AScript: string;
      const AKeys, AArgs: TArray<string>): TArray<string>;
    // Streams
    function Streams: ISidekiqRedisStreams;
  end;

implementation

{ TSidekiqStreamMessage }

function TSidekiqStreamMessage.FieldValue(const AName: string): string;
var
  LField: TSidekiqStreamField;
begin
  Result := '';
  for LField in Fields do
    if SameText(LField.Name, AName) then
      Exit(LField.Value);
end;

function TSidekiqStreamMessage.HasField(const AName: string): Boolean;
begin
  Result := IndexOfField(AName) >= 0;
end;

function TSidekiqStreamMessage.IndexOfField(const AName: string): Integer;
var
  LIndex: Integer;
begin
  for LIndex := 0 to Pred(Length(Fields)) do
    if SameText(Fields[LIndex].Name, AName) then
      Exit(LIndex);
  Result := -1;
end;

end.
