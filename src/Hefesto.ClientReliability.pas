unit Hefesto.ClientReliability;

interface

uses
  System.SysUtils,
  System.SyncObjs,
  Hefesto.Queue.Interfaces;

type
  THefestoOutboxEntry = record
    EntryId: string;
    Request: THefestoPublishRequest;
  end;

  IHefestoClientOutbox = interface
    ['{E7674122-CE64-4E0D-83E1-1BE7B4A72053}']
    procedure Save(const ARequest: THefestoPublishRequest);
    function Entries: TArray<THefestoOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

  THefestoInMemoryClientOutbox = class(TInterfacedObject, IHefestoClientOutbox)
  private
    FEntries: TArray<THefestoOutboxEntry>;
    FLock: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: IHefestoClientOutbox;

    procedure Save(const ARequest: THefestoPublishRequest);
    function Entries: TArray<THefestoOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

  THefestoFileClientOutbox = class(TInterfacedObject, IHefestoClientOutbox)
  private
    FDirectory: string;

    class function DefaultDirectory: string; static;
    function FilePath(const AEntryId: string): string;
    class function LoadEntry(const AFileName: string): THefestoOutboxEntry; static;
    class procedure SaveEntry(
      const AFileName: string;
      const AEntry: THefestoOutboxEntry); static;
  public
    constructor Create(const ADirectory: string);

    class function New(const ADirectory: string): IHefestoClientOutbox;
    class function NewDefault: IHefestoClientOutbox;

    procedure Save(const ARequest: THefestoPublishRequest);
    function Entries: TArray<THefestoOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

implementation

uses
  System.JSON,
  System.IOUtils,
  System.Generics.Collections;

function PublishRequestFromJson(const AJson: TJSONObject): THefestoPublishRequest;
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

function PublishRequestToJson(const ARequest: THefestoPublishRequest): TJSONObject;
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

{ THefestoInMemoryClientOutbox }

constructor THefestoInMemoryClientOutbox.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor THefestoInMemoryClientOutbox.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure THefestoInMemoryClientOutbox.Clear;
begin
  FLock.Acquire;
  try
    SetLength(FEntries, 0);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryClientOutbox.Count: Integer;
begin
  FLock.Acquire;
  try
    Result := Length(FEntries);
  finally
    FLock.Release;
  end;
end;

function THefestoInMemoryClientOutbox.Entries: TArray<THefestoOutboxEntry>;
begin
  FLock.Acquire;
  try
    Result := Copy(FEntries);
  finally
    FLock.Release;
  end;
end;

class function THefestoInMemoryClientOutbox.New: IHefestoClientOutbox;
begin
  Result := THefestoInMemoryClientOutbox.Create;
end;

procedure THefestoInMemoryClientOutbox.Remove(const AEntryId: string);
var
  LEntry: THefestoOutboxEntry;
  LFiltered: TArray<THefestoOutboxEntry>;
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

procedure THefestoInMemoryClientOutbox.Save(const ARequest: THefestoPublishRequest);
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

{ THefestoFileClientOutbox }

procedure THefestoFileClientOutbox.Clear;
var
  LFileName: string;
begin
  if not TDirectory.Exists(FDirectory) then
    Exit;

  for LFileName in TDirectory.GetFiles(FDirectory, '*.json') do
    TFile.Delete(LFileName);
end;

function THefestoFileClientOutbox.Count: Integer;
begin
  if not TDirectory.Exists(FDirectory) then
    Exit(0);
  Result := Length(TDirectory.GetFiles(FDirectory, '*.json'));
end;

constructor THefestoFileClientOutbox.Create(const ADirectory: string);
begin
  inherited Create;
  FDirectory := ADirectory;
  TDirectory.CreateDirectory(FDirectory);
end;

class function THefestoFileClientOutbox.DefaultDirectory: string;
begin
  Result := TPath.Combine(TPath.GetTempPath, 'sidekiq4delphi-outbox');
end;

function THefestoFileClientOutbox.Entries: TArray<THefestoOutboxEntry>;
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

function THefestoFileClientOutbox.FilePath(const AEntryId: string): string;
begin
  Result := TPath.Combine(FDirectory, AEntryId + '.json');
end;

class function THefestoFileClientOutbox.LoadEntry(
  const AFileName: string): THefestoOutboxEntry;
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

class function THefestoFileClientOutbox.New(
  const ADirectory: string): IHefestoClientOutbox;
begin
  Result := THefestoFileClientOutbox.Create(ADirectory);
end;

class function THefestoFileClientOutbox.NewDefault: IHefestoClientOutbox;
begin
  Result := THefestoFileClientOutbox.Create(DefaultDirectory);
end;

procedure THefestoFileClientOutbox.Remove(const AEntryId: string);
var
  LFileName: string;
begin
  LFileName := FilePath(AEntryId);
  if TFile.Exists(LFileName) then
    TFile.Delete(LFileName);
end;

procedure THefestoFileClientOutbox.Save(const ARequest: THefestoPublishRequest);
var
  LEntry: THefestoOutboxEntry;
begin
  LEntry.EntryId := FormatDateTime('yyyymmddhhnnsszzz', Now) + '-' +
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', '');
  LEntry.Request := ARequest;
  SaveEntry(FilePath(LEntry.EntryId), LEntry);
end;

class procedure THefestoFileClientOutbox.SaveEntry(
  const AFileName: string;
  const AEntry: THefestoOutboxEntry);
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
