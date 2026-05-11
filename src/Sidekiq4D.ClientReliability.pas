unit Sidekiq4D.ClientReliability;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  Sidekiq4D.Queue.Interfaces;

type
  TSidekiqOutboxEntry = record
    EntryId: string;
    Request: TSidekiqPublishRequest;
  end;

  ISidekiqClientOutbox = interface
    ['{E7674122-CE64-4E0D-83E1-1BE7B4A72053}']
    procedure Save(const ARequest: TSidekiqPublishRequest);
    function Entries: TArray<TSidekiqOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

  TSidekiqInMemoryClientOutbox = class(TInterfacedObject, ISidekiqClientOutbox)
  private
    FEntries: TArray<TSidekiqOutboxEntry>;
    FLock: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: ISidekiqClientOutbox;

    procedure Save(const ARequest: TSidekiqPublishRequest);
    function Entries: TArray<TSidekiqOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

  TSidekiqFileClientOutbox = class(TInterfacedObject, ISidekiqClientOutbox)
  private
    FDirectory: string;

    class function DefaultDirectory: string; static;
    function FilePath(const AEntryId: string): string;
    class function LoadEntry(const AFileName: string): TSidekiqOutboxEntry; static;
    class procedure SaveEntry(
      const AFileName: string;
      const AEntry: TSidekiqOutboxEntry); static;
  public
    constructor Create(const ADirectory: string);

    class function New(const ADirectory: string): ISidekiqClientOutbox;
    class function NewDefault: ISidekiqClientOutbox;

    procedure Save(const ARequest: TSidekiqPublishRequest);
    function Entries: TArray<TSidekiqOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

implementation

uses
  System.JSON,
  System.IOUtils,
  System.Generics.Collections;

function PublishRequestFromJson(const AJson: TJSONObject): TSidekiqPublishRequest;
var
  LArray: TJSONArray;
  LItem: TJSONValue;
  LObject: TJSONObject;
  LCount: Integer;
begin
  Result.QueueName := AJson.GetValue<string>('queue_name');
  Result.Action := AJson.GetValue<string>('action');
  Result.Body := AJson.GetValue<string>('body');
  Result.DelaySeconds := AJson.GetValue<Integer>('delay_seconds');
  SetLength(Result.Attributes, 0);

  LArray := AJson.GetValue<TJSONArray>('attributes');
  if not Assigned(LArray) then
    Exit;

  SetLength(Result.Attributes, LArray.Count);
  LCount := 0;
  for LItem in LArray do
  begin
    if not (LItem is TJSONObject) then
      Continue;
    LObject := TJSONObject(LItem);
    Result.Attributes[LCount].Key := LObject.GetValue<string>('key');
    Result.Attributes[LCount].Value := LObject.GetValue<string>('value');
    Inc(LCount);
  end;
  SetLength(Result.Attributes, LCount);
end;

function PublishRequestToJson(const ARequest: TSidekiqPublishRequest): TJSONObject;
var
  LArray: TJSONArray;
  LIndex: Integer;
begin
  Result := TJSONObject.Create;
  Result.AddPair('queue_name', ARequest.QueueName);
  Result.AddPair('action', ARequest.Action);
  Result.AddPair('body', ARequest.Body);
  Result.AddPair('delay_seconds', TJSONNumber.Create(ARequest.DelaySeconds));
  LArray := TJSONArray.Create;
  for LIndex := 0 to Pred(Length(ARequest.Attributes)) do
    LArray.AddElement(
      TJSONObject.Create
        .AddPair('key', ARequest.Attributes[LIndex].Key)
        .AddPair('value', ARequest.Attributes[LIndex].Value));
  Result.AddPair('attributes', LArray);
end;

{ TSidekiqInMemoryClientOutbox }

constructor TSidekiqInMemoryClientOutbox.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor TSidekiqInMemoryClientOutbox.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure TSidekiqInMemoryClientOutbox.Clear;
begin
  FLock.Acquire;
  try
    SetLength(FEntries, 0);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryClientOutbox.Count: Integer;
begin
  FLock.Acquire;
  try
    Result := Length(FEntries);
  finally
    FLock.Release;
  end;
end;

function TSidekiqInMemoryClientOutbox.Entries: TArray<TSidekiqOutboxEntry>;
begin
  FLock.Acquire;
  try
    Result := Copy(FEntries);
  finally
    FLock.Release;
  end;
end;

class function TSidekiqInMemoryClientOutbox.New: ISidekiqClientOutbox;
begin
  Result := TSidekiqInMemoryClientOutbox.Create;
end;

procedure TSidekiqInMemoryClientOutbox.Remove(const AEntryId: string);
var
  LEntry: TSidekiqOutboxEntry;
  LFiltered: TArray<TSidekiqOutboxEntry>;
begin
  FLock.Acquire;
  try
    SetLength(LFiltered, 0);
    for LEntry in FEntries do
      if not SameText(LEntry.EntryId, AEntryId) then
      begin
        SetLength(LFiltered, Length(LFiltered) + 1);
        LFiltered[High(LFiltered)] := LEntry;
      end;
    FEntries := LFiltered;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqInMemoryClientOutbox.Save(const ARequest: TSidekiqPublishRequest);
var
  LLength: Integer;
begin
  FLock.Acquire;
  try
    LLength := Length(FEntries);
    SetLength(FEntries, LLength + 1);
    FEntries[LLength].EntryId := TGUID.NewGuid.ToString;
    FEntries[LLength].Request := ARequest;
  finally
    FLock.Release;
  end;
end;

{ TSidekiqFileClientOutbox }

procedure TSidekiqFileClientOutbox.Clear;
var
  LFileName: string;
begin
  if not TDirectory.Exists(FDirectory) then
    Exit;

  for LFileName in TDirectory.GetFiles(FDirectory, '*.json') do
    TFile.Delete(LFileName);
end;

function TSidekiqFileClientOutbox.Count: Integer;
begin
  if not TDirectory.Exists(FDirectory) then
    Exit(0);
  Result := Length(TDirectory.GetFiles(FDirectory, '*.json'));
end;

constructor TSidekiqFileClientOutbox.Create(const ADirectory: string);
begin
  inherited Create;
  FDirectory := ADirectory;
  TDirectory.CreateDirectory(FDirectory);
end;

class function TSidekiqFileClientOutbox.DefaultDirectory: string;
begin
  Result := TPath.Combine(TPath.GetTempPath, 'sidekiq4delphi-outbox');
end;

function TSidekiqFileClientOutbox.Entries: TArray<TSidekiqOutboxEntry>;
var
  LFiles: TArray<string>;
  LFileName: string;
begin
  SetLength(Result, 0);
  if not TDirectory.Exists(FDirectory) then
    Exit;

  LFiles := TDirectory.GetFiles(FDirectory, '*.json');
  TArray.Sort<string>(LFiles);
  for LFileName in LFiles do
  begin
    SetLength(Result, Length(Result) + 1);
    Result[High(Result)] := LoadEntry(LFileName);
  end;
end;

function TSidekiqFileClientOutbox.FilePath(const AEntryId: string): string;
begin
  Result := TPath.Combine(FDirectory, AEntryId + '.json');
end;

class function TSidekiqFileClientOutbox.LoadEntry(
  const AFileName: string): TSidekiqOutboxEntry;
var
  LJson: TJSONObject;
begin
  LJson := TJSONObject.ParseJSONValue(TFile.ReadAllText(AFileName)) as TJSONObject;
  try
    Result.EntryId := LJson.GetValue<string>('entry_id');
    Result.Request := PublishRequestFromJson(LJson.GetValue<TJSONObject>('request'));
  finally
    LJson.Free;
  end;
end;

class function TSidekiqFileClientOutbox.New(
  const ADirectory: string): ISidekiqClientOutbox;
begin
  Result := TSidekiqFileClientOutbox.Create(ADirectory);
end;

class function TSidekiqFileClientOutbox.NewDefault: ISidekiqClientOutbox;
begin
  Result := TSidekiqFileClientOutbox.Create(DefaultDirectory);
end;

procedure TSidekiqFileClientOutbox.Remove(const AEntryId: string);
var
  LFileName: string;
begin
  LFileName := FilePath(AEntryId);
  if TFile.Exists(LFileName) then
    TFile.Delete(LFileName);
end;

procedure TSidekiqFileClientOutbox.Save(const ARequest: TSidekiqPublishRequest);
var
  LEntry: TSidekiqOutboxEntry;
begin
  LEntry.EntryId := FormatDateTime('yyyymmddhhnnsszzz', Now) + '-' +
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', '');
  LEntry.Request := ARequest;
  SaveEntry(FilePath(LEntry.EntryId), LEntry);
end;

class procedure TSidekiqFileClientOutbox.SaveEntry(
  const AFileName: string;
  const AEntry: TSidekiqOutboxEntry);
var
  LJson: TJSONObject;
begin
  LJson := TJSONObject.Create;
  try
    LJson.AddPair('entry_id', AEntry.EntryId);
    LJson.AddPair('request', PublishRequestToJson(AEntry.Request));
    TFile.WriteAllText(AFileName, LJson.ToJSON);
  finally
    LJson.Free;
  end;
end;

end.
