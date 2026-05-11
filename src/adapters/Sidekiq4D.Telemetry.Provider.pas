unit Sidekiq4D.Telemetry.Provider;

interface

uses
  System.SysUtils,
  System.JSON,
  System.Classes,
  System.Net.HttpClient,
  Sidekiq4D.Job,
  Sidekiq4D.Context,
  Sidekiq4D.Handler;

type
  // Base class para handlers que entregam eventos a providers de telemetria.
  // Substitui ITelemetryProvider do AgenteTelemetria.
  //
  // Cada provider concreto herda e implementa BuildPayload + SubmitUrl.
  TSidekiqTelemetryProviderHandler = class abstract(TInterfacedObject, ISidekiqJobHandler)
  protected
    function ProviderName: string; virtual; abstract;
    function SubmitUrl: string; virtual; abstract;
    function BuildPayload(const AEventJson: string): string; virtual; abstract;
    function ContentType: string; virtual;
    function AuthorizationHeader: string; virtual;
  public
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

  // Provider: Elasticsearch (Bulk API)
  //
  //   Server.RegisterHandler('elasticsearch',
  //     TSidekiqElasticsearchHandler.New('http://localhost:9200', 'telemetry-events'));
  TSidekiqElasticsearchHandler = class(TSidekiqTelemetryProviderHandler)
  private
    FBaseUrl: string;
    FIndexName: string;
  protected
    function ProviderName: string; override;
    function SubmitUrl: string; override;
    function BuildPayload(const AEventJson: string): string; override;
  public
    constructor Create(const ABaseUrl, AIndexName: string);
    class function New(const ABaseUrl: string;
      const AIndexName: string = 'telemetry-events'): ISidekiqJobHandler;
  end;

  // Provider: Datadog (Logs API)
  //
  //   Server.RegisterHandler('datadog',
  //     TSidekiqDatadogHandler.New('https://http-intake.logs.datadoghq.com', 'DD_API_KEY'));
  TSidekiqDatadogHandler = class(TSidekiqTelemetryProviderHandler)
  private
    FBaseUrl: string;
    FApiKey: string;
  protected
    function ProviderName: string; override;
    function SubmitUrl: string; override;
    function BuildPayload(const AEventJson: string): string; override;
    function AuthorizationHeader: string; override;
  public
    constructor Create(const ABaseUrl, AApiKey: string);
    class function New(const ABaseUrl, AApiKey: string): ISidekiqJobHandler;
  end;

  // Provider: OTLP/HTTP (OpenTelemetry Logs)
  //
  //   Server.RegisterHandler('otlp',
  //     TSidekiqOTLPHandler.New('http://localhost:4318'));
  TSidekiqOTLPHandler = class(TSidekiqTelemetryProviderHandler)
  private
    FBaseUrl: string;
  protected
    function ProviderName: string; override;
    function SubmitUrl: string; override;
    function BuildPayload(const AEventJson: string): string; override;
    function ContentType: string; override;
  public
    constructor Create(const ABaseUrl: string);
    class function New(const ABaseUrl: string): ISidekiqJobHandler;
  end;

implementation

{ TSidekiqTelemetryProviderHandler }

function TSidekiqTelemetryProviderHandler.CanHandle(
  const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, ProviderName);
end;

function TSidekiqTelemetryProviderHandler.ContentType: string;
begin
  Result := 'application/json';
end;

function TSidekiqTelemetryProviderHandler.AuthorizationHeader: string;
begin
  Result := '';
end;

procedure TSidekiqTelemetryProviderHandler.Perform(
  const AContext: ISidekiqJobContext);
var
  LHttp: THTTPClient;
  LPayload: TStringStream;
  LResponse: IHTTPResponse;
  LAuth: string;
begin
  LHttp := THTTPClient.Create;
  try
    LHttp.ContentType := ContentType;
    LAuth := AuthorizationHeader;
    if not LAuth.IsEmpty then
      LHttp.CustomHeaders['Authorization'] := LAuth;

    LPayload := TStringStream.Create(BuildPayload(AContext.Job.Body), TEncoding.UTF8);
    try
      LResponse := LHttp.Post(SubmitUrl, LPayload);

      if (LResponse.StatusCode < 200) or (LResponse.StatusCode >= 300) then
        raise Exception.CreateFmt(
          '%s submit failed: HTTP %d - %s',
          [ProviderName, LResponse.StatusCode, LResponse.ContentAsString(TEncoding.UTF8)]);
    finally
      LPayload.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

{ TSidekiqElasticsearchHandler }

constructor TSidekiqElasticsearchHandler.Create(const ABaseUrl, AIndexName: string);
begin
  inherited Create;
  FBaseUrl := ABaseUrl.TrimRight(['/']);
  FIndexName := AIndexName;
end;

class function TSidekiqElasticsearchHandler.New(
  const ABaseUrl, AIndexName: string): ISidekiqJobHandler;
begin
  Result := TSidekiqElasticsearchHandler.Create(ABaseUrl, AIndexName);
end;

function TSidekiqElasticsearchHandler.ProviderName: string;
begin
  Result := 'elasticsearch';
end;

function TSidekiqElasticsearchHandler.SubmitUrl: string;
begin
  Result := Format('%s/%s/_doc', [FBaseUrl, FIndexName]);
end;

function TSidekiqElasticsearchHandler.BuildPayload(const AEventJson: string): string;
begin
  // Elasticsearch aceita o documento JSON diretamente
  Result := AEventJson;
end;

{ TSidekiqDatadogHandler }

constructor TSidekiqDatadogHandler.Create(const ABaseUrl, AApiKey: string);
begin
  inherited Create;
  FBaseUrl := ABaseUrl.TrimRight(['/']);
  FApiKey := AApiKey;
end;

class function TSidekiqDatadogHandler.New(
  const ABaseUrl, AApiKey: string): ISidekiqJobHandler;
begin
  Result := TSidekiqDatadogHandler.Create(ABaseUrl, AApiKey);
end;

function TSidekiqDatadogHandler.ProviderName: string;
begin
  Result := 'datadog';
end;

function TSidekiqDatadogHandler.SubmitUrl: string;
begin
  Result := FBaseUrl + '/api/v2/logs';
end;

function TSidekiqDatadogHandler.AuthorizationHeader: string;
begin
  Result := 'DD-API-KEY ' + FApiKey;
end;

function TSidekiqDatadogHandler.BuildPayload(const AEventJson: string): string;
var
  LEvent, LDDLog: TJSONObject;
begin
  // Transforma evento canonico em formato Datadog Logs
  LEvent := TJSONObject.ParseJSONValue(AEventJson) as TJSONObject;
  if not Assigned(LEvent) then
    Exit(AEventJson);
  try
    LDDLog := TJSONObject.Create;
    try
      LDDLog.AddPair('ddsource', 'sidekiq4d');
      LDDLog.AddPair('service', LEvent.GetValue<string>('service_name', 'unknown'));
      LDDLog.AddPair('hostname', LEvent.GetValue<string>('instance_name', ''));
      LDDLog.AddPair('message', LEvent.GetValue<string>('message_text', ''));
      LDDLog.AddPair('status', LEvent.GetValue<string>('severity', 'info'));
      Result := '[' + LDDLog.ToJSON + ']';
    finally
      LDDLog.Free;
    end;
  finally
    LEvent.Free;
  end;
end;

{ TSidekiqOTLPHandler }

constructor TSidekiqOTLPHandler.Create(const ABaseUrl: string);
begin
  inherited Create;
  FBaseUrl := ABaseUrl.TrimRight(['/']);
end;

class function TSidekiqOTLPHandler.New(const ABaseUrl: string): ISidekiqJobHandler;
begin
  Result := TSidekiqOTLPHandler.Create(ABaseUrl);
end;

function TSidekiqOTLPHandler.ProviderName: string;
begin
  Result := 'otlp';
end;

function TSidekiqOTLPHandler.SubmitUrl: string;
begin
  Result := FBaseUrl + '/v1/logs';
end;

function TSidekiqOTLPHandler.ContentType: string;
begin
  Result := 'application/json';
end;

function TSidekiqOTLPHandler.BuildPayload(const AEventJson: string): string;
var
  LEvent: TJSONObject;
  LBody, LResource, LScope, LLog, LLogRecord: TJSONObject;
  LResourceLogs, LScopeLogs, LLogRecords: TJSONArray;
begin
  // Transforma evento canonico em OTLP LogRecord
  LEvent := TJSONObject.ParseJSONValue(AEventJson) as TJSONObject;
  if not Assigned(LEvent) then
    Exit(AEventJson);
  try
    LLogRecord := TJSONObject.Create;
    LLogRecord.AddPair('timeUnixNano', '0');
    LLogRecord.AddPair('severityText', LEvent.GetValue<string>('severity', 'INFO'));
    LLogRecord.AddPair('body', TJSONObject.Create.AddPair('stringValue',
      LEvent.GetValue<string>('message_text', '')));

    LLogRecords := TJSONArray.Create;
    LLogRecords.AddElement(LLogRecord);

    LScope := TJSONObject.Create;
    LScopeLogs := TJSONArray.Create;
    LScopeLogs.AddElement(TJSONObject.Create
      .AddPair('scope', LScope)
      .AddPair('logRecords', LLogRecords));

    LResource := TJSONObject.Create;
    LResourceLogs := TJSONArray.Create;
    LResourceLogs.AddElement(TJSONObject.Create
      .AddPair('resource', LResource)
      .AddPair('scopeLogs', LScopeLogs));

    LBody := TJSONObject.Create;
    LBody.AddPair('resourceLogs', LResourceLogs);

    Result := LBody.ToJSON;
    LBody.Free;
  finally
    LEvent.Free;
  end;
end;

end.
