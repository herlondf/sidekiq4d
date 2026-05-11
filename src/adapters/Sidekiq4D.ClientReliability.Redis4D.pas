unit Sidekiq4D.ClientReliability.Redis4D;

interface

uses
  Sidekiq4D.ClientReliability,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Redis.Client;

type
  // Outbox distribuido via Redis list — suporta multi-processo e multi-servidor.
  //
  // Uso:
  //   LOutbox := TSidekiqRedis4DClientOutbox.New
  //     .ConnectionString('redis://127.0.0.1:6379/0');
  //
  //   LOutbox := TSidekiqRedis4DClientOutbox.New
  //     .RedisClient(LMyClient);
  TSidekiqRedis4DClientOutbox = class(TInterfacedObject, ISidekiqClientOutbox)
  private
    const RedisKey = 'sidekiq4d:outbox';
  private
    // Configuration field: set once before first use. Thread-safe after init.
    FClient: ISidekiqRedisClient;

    function GetClient: ISidekiqRedisClient;

    class function EntryToJson(const AEntry: TSidekiqOutboxEntry): string; static;
    class function EntryFromJson(const AJson: string): TSidekiqOutboxEntry; static;
  public
    constructor Create;

    class function New: TSidekiqRedis4DClientOutbox; static;

    function ConnectionString(
      const AValue: string): TSidekiqRedis4DClientOutbox;
    function RedisClient(
      const AClient: ISidekiqRedisClient): TSidekiqRedis4DClientOutbox;

    procedure Save(const ARequest: TSidekiqPublishRequest);
    function Entries: TArray<TSidekiqOutboxEntry>;
    procedure Remove(const AEntryId: string);
    function Count: Integer;
    procedure Clear;
  end;

implementation

uses
  System.SysUtils,
  System.JSON,
  Sidekiq4D.Redis4D.Client;

{ TSidekiqRedis4DClientOutbox }

constructor TSidekiqRedis4DClientOutbox.Create;
begin
  inherited Create;
end;

class function TSidekiqRedis4DClientOutbox.New: TSidekiqRedis4DClientOutbox;
begin
  Result := TSidekiqRedis4DClientOutbox.Create;
end;

function TSidekiqRedis4DClientOutbox.ConnectionString(
  const AValue: string): TSidekiqRedis4DClientOutbox;
begin
  Result := Self;
  FClient := TRedis4DClientBridge.NewFromConnectionString(AValue);
end;

function TSidekiqRedis4DClientOutbox.RedisClient(
  const AClient: ISidekiqRedisClient): TSidekiqRedis4DClientOutbox;
begin
  Result := Self;
  FClient := AClient;
end;

function TSidekiqRedis4DClientOutbox.GetClient: ISidekiqRedisClient;
begin
  if not Assigned(FClient) then
    raise EArgumentException.Create(
      'No Redis client configured. Call .ConnectionString(...) or .RedisClient(...).');
  Result := FClient;
end;

class function TSidekiqRedis4DClientOutbox.EntryToJson(
  const AEntry: TSidekiqOutboxEntry): string;
var
  LRoot: TJSONObject;
  LRequest: TJSONObject;
  LAttrs: TJSONArray;
  LIndex: Integer;
begin
  LRoot := TJSONObject.Create;
  try
    LRoot.AddPair('entry_id', AEntry.EntryId);

    LRequest := TJSONObject.Create;
    LRequest.AddPair('queue_name', AEntry.Request.QueueName);
    LRequest.AddPair('action', AEntry.Request.Action);
    LRequest.AddPair('body', AEntry.Request.Body);
    LRequest.AddPair('delay_seconds',
      TJSONNumber.Create(AEntry.Request.DelaySeconds));

    LAttrs := TJSONArray.Create;
    for LIndex := 0 to Pred(Length(AEntry.Request.Attributes)) do
      LAttrs.AddElement(
        TJSONObject.Create
          .AddPair('key', AEntry.Request.Attributes[LIndex].Key)
          .AddPair('value', AEntry.Request.Attributes[LIndex].Value));
    LRequest.AddPair('attributes', LAttrs);

    LRoot.AddPair('request', LRequest);
    Result := LRoot.ToJSON;
  finally
    LRoot.Free;
  end;
end;

class function TSidekiqRedis4DClientOutbox.EntryFromJson(
  const AJson: string): TSidekiqOutboxEntry;
var
  LRoot: TJSONObject;
  LRequest: TJSONObject;
  LAttrs: TJSONArray;
  LItem: TJSONValue;
  LAttr: TJSONObject;
  LCount: Integer;
begin
  LRoot := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  if not Assigned(LRoot) then
    raise EArgumentException.CreateFmt('JSON de entrada inválido: %s', [AJson]);
  try
    Result.EntryId := LRoot.GetValue<string>('entry_id');

    LRequest := LRoot.GetValue<TJSONObject>('request');
    Result.Request.QueueName    := LRequest.GetValue<string>('queue_name');
    Result.Request.Action       := LRequest.GetValue<string>('action');
    Result.Request.Body         := LRequest.GetValue<string>('body');
    Result.Request.DelaySeconds := LRequest.GetValue<Integer>('delay_seconds');

    SetLength(Result.Request.Attributes, 0);
    LAttrs := LRequest.GetValue<TJSONArray>('attributes');
    if Assigned(LAttrs) then
    begin
      SetLength(Result.Request.Attributes, LAttrs.Count);
      LCount := 0;
      for LItem in LAttrs do
      begin
        if not (LItem is TJSONObject) then
          Continue;
        LAttr := TJSONObject(LItem);
        Result.Request.Attributes[LCount].Key   := LAttr.GetValue<string>('key');
        Result.Request.Attributes[LCount].Value := LAttr.GetValue<string>('value');
        Inc(LCount);
      end;
      SetLength(Result.Request.Attributes, LCount);
    end;
  finally
    LRoot.Free;
  end;
end;

procedure TSidekiqRedis4DClientOutbox.Save(
  const ARequest: TSidekiqPublishRequest);
var
  LEntry: TSidekiqOutboxEntry;
begin
  LEntry.EntryId :=
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', '').ToLower;
  LEntry.Request := ARequest;
  GetClient.RPush(RedisKey, [EntryToJson(LEntry)]);
end;

function TSidekiqRedis4DClientOutbox.Entries: TArray<TSidekiqOutboxEntry>;
var
  LRaw: TArray<string>;
  LIndex: Integer;
begin
  LRaw := GetClient.LRange(RedisKey, 0, -1);
  SetLength(Result, Length(LRaw));
  for LIndex := 0 to Pred(Length(LRaw)) do
    Result[LIndex] := EntryFromJson(LRaw[LIndex]);
end;

procedure TSidekiqRedis4DClientOutbox.Remove(const AEntryId: string);
var
  LRaw: TArray<string>;
  LItem: string;
  LEntry: TSidekiqOutboxEntry;
begin
  LRaw := GetClient.LRange(RedisKey, 0, -1);
  for LItem in LRaw do
  begin
    try
      LEntry := EntryFromJson(LItem);
    except
      Continue;
    end;
    if SameText(LEntry.EntryId, AEntryId) then
    begin
      GetClient.LRem(RedisKey, 1, LItem);
      Exit;
    end;
  end;
end;

function TSidekiqRedis4DClientOutbox.Count: Integer;
begin
  Result := GetClient.LLen(RedisKey);
end;

procedure TSidekiqRedis4DClientOutbox.Clear;
begin
  GetClient.Del(RedisKey);
end;

end.
