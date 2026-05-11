unit Sidekiq4D.Telemetry.OTLP;

{
  OpenTelemetry Trace Exporter para Sidekiq4D.

  Implementa ISidekiqTelemetry e exporta spans OTLP/HTTP JSON para qualquer
  coletor compativel (Jaeger v2+, Grafana Tempo, Honeycomb, OTel Collector).

  Uso:
    Server.UseTelemetry(
      TSidekiqOTLPTraceTelemetry.New(
        'http://localhost:4318',  // OTLP HTTP endpoint
        'meu-servico',           // service.name
        'instancia-01'           // service.instance.id (opcional)
      )
    );

  Ou combinado com o console via TSidekiqCompositeTelemetry:
    Server.UseTelemetry(
      TSidekiqCompositeTelemetry.New([
        TSidekiqConsoleTelemetry.New,
        TSidekiqOTLPTraceTelemetry.New('http://localhost:4318', 'meu-servico')
      ])
    );

  Cada job gera um span com:
    - traceId/spanId aleatorios (128/64 bits, GUID-based)
    - job.id, job.queue, job.action, job.attempts como atributos
    - status OK (sucesso) ou ERROR (falha com error.message)
    - kind = SPAN_KIND_CONSUMER (2)

  Erros de exportacao sao ignorados silenciosamente para nao afetar o
  processamento dos jobs.
}

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.DateUtils,
  System.Diagnostics,
  System.Net.HttpClient,
  System.Net.URLClient,
  System.JSON,
  System.Generics.Collections,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Job;

type
  TSidekiqOTLPTraceTelemetry = class(TInterfacedObject, ISidekiqTelemetry)
  private type
    TSpanContext = record
      TraceId   : string;
      SpanId    : string;
      StartNano : Int64;
      Action    : string;
      QueueName : string;
      Attempts  : Integer;
    end;
  private
    FEndpoint     : string;
    FServiceName  : string;
    FInstanceId   : string;
    FPendingSpans : TDictionary<string, TSpanContext>;
    FLock         : TCriticalSection;

    class function GenerateTraceId: string; static;
    class function GenerateSpanId: string; static;
    class function NowUnixNano: Int64; static;

    procedure ExportSpan(
      const AJobId    : string;
      const ACtx      : TSpanContext;
      const AEndNano  : Int64;
      const ASuccess  : Boolean;
      const AErrorMsg : string = '');

    procedure CloseSpan(
      const AJob    : ISidekiqJobEnvelope;
      const ASuccess: Boolean;
      const AErrorMsg: string = '');
  public
    constructor Create(
      const AEndpoint    : string;
      const AServiceName : string;
      const AInstanceId  : string);
    destructor Destroy; override;

    class function New(
      const AEndpoint    : string;
      const AServiceName : string;
      const AInstanceId  : string = ''): ISidekiqTelemetry;

    { ISidekiqTelemetry }
    procedure ServerStarted;
    procedure ServerStopped;
    procedure PollingStarted;
    procedure PollingStopped;
    procedure Idle(const ADelayMs: Integer);
    procedure FetchFinished(const AQueueName: string; const ACount: Integer);
    procedure JobStarted(const AJob: ISidekiqJobEnvelope);
    procedure JobSucceeded(const AJob: ISidekiqJobEnvelope; const ADurationMs: Int64);
    procedure JobFailed(const AJob: ISidekiqJobEnvelope; const AError: Exception; const ADurationMs: Int64);
    procedure JobAcked(const AJob: ISidekiqJobEnvelope);
    procedure JobRetried(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string);
    procedure JobDeadLettered(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

implementation

{ TSidekiqOTLPTraceTelemetry — private helpers }

class function TSidekiqOTLPTraceTelemetry.GenerateTraceId: string;
var
  LG1, LG2: TGuid;
begin
  CreateGUID(LG1);
  CreateGUID(LG2);
  // Dois GUIDs sem hifens = 32 hex chars = 128 bits (OTLP traceId)
  Result :=
    (LG1.ToString + LG2.ToString)
    .Replace('{', '').Replace('}', '').Replace('-', '').ToLower;
  SetLength(Result, 32);
end;

class function TSidekiqOTLPTraceTelemetry.GenerateSpanId: string;
var
  LG: TGuid;
begin
  CreateGUID(LG);
  // Metade de um GUID sem hifens = 16 hex chars = 64 bits (OTLP spanId)
  Result := LG.ToString
    .Replace('{', '').Replace('}', '').Replace('-', '').ToLower;
  SetLength(Result, 16);
end;

class function TSidekiqOTLPTraceTelemetry.NowUnixNano: Int64;
var
  LSeconds: Int64;
  LFrac   : Int64;
begin
  // Segundos UTC desde epoch (resolução 1s)
  LSeconds := DateTimeToUnix(Now, False);
  // Sub-segundo via TStopwatch (frequência do hardware, ~100ns no Windows)
  LFrac := (TStopwatch.GetTimeStamp mod TStopwatch.Frequency)
           * Int64(1000000000) div TStopwatch.Frequency;
  Result := LSeconds * Int64(1000000000) + LFrac;
end;

{ TSidekiqOTLPTraceTelemetry }

constructor TSidekiqOTLPTraceTelemetry.Create(
  const AEndpoint, AServiceName, AInstanceId: string);
begin
  inherited Create;
  FEndpoint     := AEndpoint.TrimRight(['/']);
  FServiceName  := AServiceName;
  FInstanceId   := AInstanceId;
  FPendingSpans := TDictionary<string, TSpanContext>.Create;
  FLock         := TCriticalSection.Create;
end;

destructor TSidekiqOTLPTraceTelemetry.Destroy;
begin
  FLock.Free;
  FPendingSpans.Free;
  inherited;
end;

class function TSidekiqOTLPTraceTelemetry.New(
  const AEndpoint, AServiceName, AInstanceId: string): ISidekiqTelemetry;
begin
  Result := TSidekiqOTLPTraceTelemetry.Create(AEndpoint, AServiceName, AInstanceId);
end;

procedure TSidekiqOTLPTraceTelemetry.ExportSpan(
  const AJobId   : string;
  const ACtx     : TSpanContext;
  const AEndNano : Int64;
  const ASuccess : Boolean;
  const AErrorMsg: string);

  function StrAttr(const AKey, AVal: string): TJSONObject;
  begin
    Result := TJSONObject.Create;
    Result.AddPair('key', AKey);
    Result.AddPair('value', TJSONObject.Create.AddPair('stringValue', AVal));
  end;

  function IntAttr(const AKey: string; const AVal: Integer): TJSONObject;
  begin
    Result := TJSONObject.Create;
    Result.AddPair('key', AKey);
    Result.AddPair('value', TJSONObject.Create
      .AddPair('intValue', TJSONNumber.Create(AVal)));
  end;

var
  LAttrs, LResAttrs, LSpans, LScopeSpans, LResourceSpans: TJSONArray;
  LSpan, LStatus, LScope, LResource, LBody: TJSONObject;
  LHttp: THTTPClient;
  LPayload: TStringStream;
begin
  LAttrs := TJSONArray.Create;
  LAttrs.AddElement(StrAttr('job.id',     AJobId));
  LAttrs.AddElement(StrAttr('job.queue',  ACtx.QueueName));
  LAttrs.AddElement(StrAttr('job.action', ACtx.Action));
  LAttrs.AddElement(IntAttr('job.attempts', ACtx.Attempts));
  if (not ASuccess) and (not AErrorMsg.IsEmpty) then
    LAttrs.AddElement(StrAttr('error.message', AErrorMsg));

  LStatus := TJSONObject.Create;
  if ASuccess then
    LStatus.AddPair('code', TJSONNumber.Create(1))  // STATUS_CODE_OK
  else
  begin
    LStatus.AddPair('code', TJSONNumber.Create(2)); // STATUS_CODE_ERROR
    if not AErrorMsg.IsEmpty then
      LStatus.AddPair('message', AErrorMsg);
  end;

  LSpan := TJSONObject.Create;
  LSpan.AddPair('traceId',           ACtx.TraceId);
  LSpan.AddPair('spanId',            ACtx.SpanId);
  LSpan.AddPair('name',              'job:' + ACtx.Action);
  LSpan.AddPair('kind',              TJSONNumber.Create(2)); // SPAN_KIND_CONSUMER
  LSpan.AddPair('startTimeUnixNano', ACtx.StartNano.ToString);
  LSpan.AddPair('endTimeUnixNano',   AEndNano.ToString);
  LSpan.AddPair('attributes',        LAttrs);
  LSpan.AddPair('status',            LStatus);

  LSpans := TJSONArray.Create;
  LSpans.AddElement(LSpan);

  LResAttrs := TJSONArray.Create;
  LResAttrs.AddElement(StrAttr('service.name', FServiceName));
  if not FInstanceId.IsEmpty then
    LResAttrs.AddElement(StrAttr('service.instance.id', FInstanceId));

  LResource := TJSONObject.Create;
  LResource.AddPair('attributes', LResAttrs);

  LScope := TJSONObject.Create;
  LScope.AddPair('name',    'sidekiq4d');
  LScope.AddPair('version', '1.0');

  LScopeSpans := TJSONArray.Create;
  LScopeSpans.AddElement(
    TJSONObject.Create
      .AddPair('scope', LScope)
      .AddPair('spans', LSpans));

  LResourceSpans := TJSONArray.Create;
  LResourceSpans.AddElement(
    TJSONObject.Create
      .AddPair('resource',   LResource)
      .AddPair('scopeSpans', LScopeSpans));

  LBody := TJSONObject.Create;
  LBody.AddPair('resourceSpans', LResourceSpans);
  try
    LHttp := THTTPClient.Create;
    try
      LPayload := TStringStream.Create(LBody.ToJSON, TEncoding.UTF8);
      try
        try
          LHttp.Post(FEndpoint + '/v1/traces', LPayload, nil,
            [TNameValuePair.Create('Content-Type', 'application/json')]);
        except
          on E: Exception do
            WriteLn('[OTLP-DEBUG] EXCEPTION: ' + E.ClassName + ': ' + E.Message);
        end;
      finally
        LPayload.Free;
      end;
    finally
      LHttp.Free;
    end;
  finally
    LBody.Free;
  end;
end;

procedure TSidekiqOTLPTraceTelemetry.CloseSpan(
  const AJob    : ISidekiqJobEnvelope;
  const ASuccess: Boolean;
  const AErrorMsg: string);
var
  LCtx  : TSpanContext;
  LFound: Boolean;
begin
  FLock.Acquire;
  try
    LFound := FPendingSpans.TryGetValue(AJob.Id, LCtx);
    if LFound then
      FPendingSpans.Remove(AJob.Id);
  finally
    FLock.Release;
  end;
  if LFound then
    ExportSpan(AJob.Id, LCtx, NowUnixNano, ASuccess, AErrorMsg);
end;

{ ISidekiqTelemetry — stubs para eventos sem span }

procedure TSidekiqOTLPTraceTelemetry.ServerStarted;   begin end;
procedure TSidekiqOTLPTraceTelemetry.ServerStopped;   begin end;
procedure TSidekiqOTLPTraceTelemetry.PollingStarted;  begin end;
procedure TSidekiqOTLPTraceTelemetry.PollingStopped;  begin end;
procedure TSidekiqOTLPTraceTelemetry.Idle(const ADelayMs: Integer); begin end;
procedure TSidekiqOTLPTraceTelemetry.FetchFinished(const AQueueName: string; const ACount: Integer); begin end;
procedure TSidekiqOTLPTraceTelemetry.JobAcked(const AJob: ISidekiqJobEnvelope); begin end;
procedure TSidekiqOTLPTraceTelemetry.JobDiscarded(const AJob: ISidekiqJobEnvelope; const AReason: string); begin end;

{ ISidekiqTelemetry — ciclo de vida do span }

procedure TSidekiqOTLPTraceTelemetry.JobStarted(const AJob: ISidekiqJobEnvelope);
var
  LCtx: TSpanContext;
begin
  LCtx.TraceId   := GenerateTraceId;
  LCtx.SpanId    := GenerateSpanId;
  LCtx.StartNano := NowUnixNano;
  LCtx.Action    := AJob.Action;
  LCtx.QueueName := AJob.QueueName;
  LCtx.Attempts  := AJob.Attempts;

  FLock.Acquire;
  try
    FPendingSpans.AddOrSetValue(AJob.Id, LCtx);
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqOTLPTraceTelemetry.JobSucceeded(
  const AJob: ISidekiqJobEnvelope;
  const ADurationMs: Int64);
begin
  CloseSpan(AJob, True);
end;

procedure TSidekiqOTLPTraceTelemetry.JobFailed(
  const AJob: ISidekiqJobEnvelope;
  const AError: Exception;
  const ADurationMs: Int64);
var
  LMsg: string;
begin
  LMsg := '';
  if Assigned(AError) then
    LMsg := AError.Message;
  CloseSpan(AJob, False, LMsg);
end;

procedure TSidekiqOTLPTraceTelemetry.JobRetried(
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer);
begin
  CloseSpan(AJob, False,
    Format('retried (delay=%ds)', [ADelaySeconds]));
end;

procedure TSidekiqOTLPTraceTelemetry.JobDeadLettered(
  const AJob: ISidekiqJobEnvelope;
  const AReason: string);
begin
  CloseSpan(AJob, False, 'dead-lettered: ' + AReason);
end;

end.
