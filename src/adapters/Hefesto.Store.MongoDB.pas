unit Hefesto.Store.MongoDB;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.Net.HttpClient,
  System.Net.URLClient,
  System.NetEncoding,
  System.DateUtils,
  System.Generics.Collections,
  Hefesto.Store.Interfaces;

type
  // State store usando MongoDB Atlas Data API (HTTP/JSON puro).
  // Nao requer driver nativo — usa REST API do Atlas ou mongod com HTTP interface.
  //
  // Uso com Atlas Data API:
  //   THefestoMongoDBStateStore.New
  //     .BaseUrl('https://data.mongodb-api.com/app/{appId}/endpoint/data/v1')
  //     .ApiKey('your-atlas-api-key')
  //     .Database('sidekiq4d')
  //     .Collection('state');
  //
  // Uso com mongod HTTP interface (localhost):
  //   THefestoMongoDBStateStore.New
  //     .BaseUrl('http://localhost:27017')
  //     .Database('sidekiq4d')
  //     .Collection('state');
  THefestoMongoDBStateStore = class(TInterfacedObject, IHefestoStateStore)
  private
    // Configuration fields: set once before first use. Thread-safe after init.
    FBaseUrl: string;
    FApiKey: string;
    FDatabase: string;
    FCollection: string;
    FDataSource: string;

    function BuildActionUrl(const AAction: string): string;
    function DoRequest(const AAction: string; const ABody: TJSONObject): TJSONObject;
    function NowUtc: TDateTime;
  public
    constructor Create;

    class function New: THefestoMongoDBStateStore;

    // Configuracao fluente
    function BaseUrl(const AValue: string): THefestoMongoDBStateStore;
    function ApiKey(const AValue: string): THefestoMongoDBStateStore;
    function Database(const AValue: string): THefestoMongoDBStateStore;
    function Collection(const AValue: string): THefestoMongoDBStateStore;
    function DataSource(const AValue: string): THefestoMongoDBStateStore;

    // IHefestoStateStore
    function Get(const AKey: string): string;
    procedure Put(const AKey, AValue: string; const ATtlSeconds: Integer = 0);
    procedure Delete(const AKey: string);
    function Exists(const AKey: string): Boolean;
    function ListKeys(const APrefix: string = ''): TArray<string>;
    function TryPutIfAbsent(
      const AKey, AValue: string; const ATtlSeconds: Integer = 0): Boolean;
  end;

implementation

{ THefestoMongoDBStateStore }

constructor THefestoMongoDBStateStore.Create;
begin
  inherited Create;
  FBaseUrl := 'http://localhost:27017';
  FDatabase := 'sidekiq4d';
  FCollection := 'state';
  FDataSource := 'Cluster0';
end;

class function THefestoMongoDBStateStore.New: THefestoMongoDBStateStore;
begin
  Result := THefestoMongoDBStateStore.Create;
end;

function THefestoMongoDBStateStore.BaseUrl(const AValue: string): THefestoMongoDBStateStore;
begin
  Result := Self;
  FBaseUrl := AValue.TrimRight(['/']);
end;

function THefestoMongoDBStateStore.ApiKey(const AValue: string): THefestoMongoDBStateStore;
begin
  Result := Self;
  FApiKey := AValue;
end;

function THefestoMongoDBStateStore.Database(const AValue: string): THefestoMongoDBStateStore;
begin
  Result := Self;
  FDatabase := AValue;
end;

function THefestoMongoDBStateStore.Collection(const AValue: string): THefestoMongoDBStateStore;
begin
  Result := Self;
  FCollection := AValue;
end;

function THefestoMongoDBStateStore.DataSource(const AValue: string): THefestoMongoDBStateStore;
begin
  Result := Self;
  FDataSource := AValue;
end;

function THefestoMongoDBStateStore.NowUtc: TDateTime;
begin
  Result := TTimeZone.Local.ToUniversalTime(Now);
end;

function THefestoMongoDBStateStore.BuildActionUrl(const AAction: string): string;
begin
  Result := FBaseUrl + '/action/' + AAction;
end;

function THefestoMongoDBStateStore.DoRequest(
  const AAction: string; const ABody: TJSONObject): TJSONObject;
var
  LHttp: THTTPClient;
  LContent: TStringStream;
  LResponse: IHTTPResponse;
  LRequestBody: TJSONObject;
begin
  LRequestBody := TJSONObject.Create;
  try
    LRequestBody.AddPair('dataSource', FDataSource);
    LRequestBody.AddPair('database', FDatabase);
    LRequestBody.AddPair('collection', FCollection);
    // Merge body fields
    if Assigned(ABody) then
    begin
      var LPair: TJSONPair;
      for LPair in ABody do
        LRequestBody.AddPair(TJSONPair(LPair.Clone));
    end;

    LHttp := THTTPClient.Create;
    try
      LHttp.ContentType := 'application/json';
      if not FApiKey.IsEmpty then
        LHttp.CustomHeaders['api-key'] := FApiKey;

      LContent := TStringStream.Create(LRequestBody.ToJSON, TEncoding.UTF8);
      try
        LResponse := LHttp.Post(BuildActionUrl(AAction), LContent);
        if (LResponse.StatusCode >= 200) and (LResponse.StatusCode < 300) then
          Result := TJSONObject.ParseJSONValue(
            LResponse.ContentAsString(TEncoding.UTF8)) as TJSONObject
        else
          Result := nil;
      finally
        LContent.Free;
      end;
    finally
      LHttp.Free;
    end;
  finally
    LRequestBody.Free;
  end;
end;

function THefestoMongoDBStateStore.Get(const AKey: string): string;
var
  LFilter, LBody: TJSONObject;
  LResponse: TJSONObject;
  LDoc: TJSONObject;
begin
  Result := '';
  LBody := TJSONObject.Create;
  LFilter := TJSONObject.Create;
  try
    LFilter.AddPair('_id', AKey);
    LBody.AddPair('filter', LFilter);

    LResponse := DoRequest('findOne', LBody);
    if Assigned(LResponse) then
    try
      LDoc := LResponse.GetValue<TJSONObject>('document', nil);
      if Assigned(LDoc) then
      begin
        // Check expiration
        var LExpiresAt := LDoc.GetValue<string>('expires_at', '');
        if (not LExpiresAt.IsEmpty) and (ISO8601ToDate(LExpiresAt) <= NowUtc) then
        begin
          Delete(AKey);
          Exit('');
        end;
        Result := LDoc.GetValue<string>('value', '');
      end;
    finally
      LResponse.Free;
    end;
  finally
    LBody.Free;
  end;
end;

procedure THefestoMongoDBStateStore.Put(
  const AKey, AValue: string; const ATtlSeconds: Integer);
var
  LFilter, LUpdate, LSet, LBody: TJSONObject;
begin
  LBody := TJSONObject.Create;
  LFilter := TJSONObject.Create;
  LUpdate := TJSONObject.Create;
  LSet := TJSONObject.Create;
  try
    LFilter.AddPair('_id', AKey);
    LSet.AddPair('value', AValue);
    if ATtlSeconds > 0 then
      LSet.AddPair('expires_at', DateToISO8601(IncSecond(NowUtc, ATtlSeconds)))
    else
      LSet.AddPair('expires_at', TJSONNull.Create);
    LUpdate.AddPair('$set', LSet);
    LBody.AddPair('filter', LFilter);
    LBody.AddPair('update', LUpdate);
    LBody.AddPair('upsert', TJSONBool.Create(True));

    var LResp := DoRequest('updateOne', LBody);
    if Assigned(LResp) then
      LResp.Free;
  finally
    LBody.Free;
  end;
end;

procedure THefestoMongoDBStateStore.Delete(const AKey: string);
var
  LFilter, LBody: TJSONObject;
begin
  LBody := TJSONObject.Create;
  LFilter := TJSONObject.Create;
  try
    LFilter.AddPair('_id', AKey);
    LBody.AddPair('filter', LFilter);
    var LResp := DoRequest('deleteOne', LBody);
    if Assigned(LResp) then
      LResp.Free;
  finally
    LBody.Free;
  end;
end;

function THefestoMongoDBStateStore.Exists(const AKey: string): Boolean;
begin
  Result := not Get(AKey).IsEmpty;
end;

function THefestoMongoDBStateStore.ListKeys(const APrefix: string): TArray<string>;
var
  LFilter, LBody, LProjection: TJSONObject;
  LResponse: TJSONObject;
  LDocs: TJSONArray;
  LIndex: Integer;
begin
  SetLength(Result, 0);
  LBody := TJSONObject.Create;
  LFilter := TJSONObject.Create;
  LProjection := TJSONObject.Create;
  try
    if not APrefix.IsEmpty then
      LFilter.AddPair('_id', TJSONObject.Create.AddPair('$regex', '^' + APrefix));
    LProjection.AddPair('_id', TJSONNumber.Create(1));
    LBody.AddPair('filter', LFilter);
    LBody.AddPair('projection', LProjection);

    LResponse := DoRequest('find', LBody);
    if Assigned(LResponse) then
    try
      LDocs := LResponse.GetValue<TJSONArray>('documents', nil);
      if Assigned(LDocs) then
      begin
        SetLength(Result, LDocs.Count);
        for LIndex := 0 to Pred(LDocs.Count) do
          Result[LIndex] := (LDocs.Items[LIndex] as TJSONObject).GetValue<string>('_id', '');
      end;
    finally
      LResponse.Free;
    end;
  finally
    LBody.Free;
  end;
end;

function THefestoMongoDBStateStore.TryPutIfAbsent(
  const AKey, AValue: string; const ATtlSeconds: Integer): Boolean;
var
  LFilter, LUpdate, LSetOnInsert, LBody: TJSONObject;
  LResponse: TJSONObject;
begin
  // Atomic check-and-insert via updateOne with $setOnInsert + upsert:true.
  // If the document already exists, $setOnInsert is a no-op and upsertedId is absent.
  // If the document is new, it gets inserted and upsertedId is set — Result := True.
  LBody := TJSONObject.Create;
  LFilter := TJSONObject.Create;
  LUpdate := TJSONObject.Create;
  LSetOnInsert := TJSONObject.Create;
  try
    LFilter.AddPair('_id', AKey);
    LSetOnInsert.AddPair('value', AValue);
    if ATtlSeconds > 0 then
      LSetOnInsert.AddPair('expires_at', DateToISO8601(IncSecond(NowUtc, ATtlSeconds)))
    else
      LSetOnInsert.AddPair('expires_at', TJSONNull.Create);
    LUpdate.AddPair('$setOnInsert', LSetOnInsert);
    LBody.AddPair('filter', LFilter);
    LBody.AddPair('update', LUpdate);
    LBody.AddPair('upsert', TJSONBool.Create(True));

    LResponse := DoRequest('updateOne', LBody);
    if Assigned(LResponse) then
    try
      // upsertedId is present when a new document was inserted; absent when matched existing
      Result := not LResponse.GetValue<TJSONValue>('upsertedId', nil).Null;
    finally
      LResponse.Free;
    end
    else
      Result := False;
  finally
    LBody.Free;
  end;
end;

end.
