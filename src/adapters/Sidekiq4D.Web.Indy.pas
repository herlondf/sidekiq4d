unit Sidekiq4D.Web.Indy;

{
  SidekiqWeb4D — Dashboard HTTP embutido (v2), implementado com Indy TIdHTTPServer.

  SPA com Bootstrap 5 + Chart.js, 9 seções, auto-refresh a cada 5 s.

  Endpoints REST:
    GET    /                          — HTML (SPA)
    GET    /api/overview              — workers, filas, DLQ, agendados, métricas 1h
    GET    /api/queues                — profundidade e status de cada fila
    GET    /api/workers               — workers ativos via server_reliability:active:*
    GET    /api/scheduled             — jobs no ISidekiqScheduledStore
    GET    /api/periodic              — definições de jobs periódicos
    GET    /api/batches               — batches via batch:*:state no StateStore
    GET    /api/metrics               — snapshots históricos para Chart.js
    GET    /api/locks                 — chaves de lock ativas
    GET    /health                    — health check (status + uptime em segundos)
    GET    /metrics                   — métricas em formato Prometheus text
    GET    /api/dlq                   — entradas da Dead Letter Queue
    POST   /api/queues/:name/pause    — pausa fila
    POST   /api/queues/:name/resume   — retoma fila
    POST   /api/dlq/retry-all         — re-enfileira todos os jobs da DLQ
    DELETE /api/dlq                   — apaga toda a DLQ
    POST   /api/dlq/:id/retry         — re-enfileira job específico
    DELETE /api/dlq/:id               — remove job específico
    DELETE /api/scheduled             — cancela job agendado (queue+action+due_at params)

  Thread safety:
    Cada request Indy roda em thread separado. Todos os objetos de config
    (StateStore, DeadLetterQueue, DashboardServer, Metrics, ScheduledStore)
    são thread-safe por contrato de interface. FRunning é protegido por FLock.
    Nunca segure FLock enquanto chama métodos desses objetos para evitar
    inversão de prioridade.
}

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.DateUtils,
  System.JSON,
  System.Hash,
  System.NetEncoding,
  IdHTTPServer,
  IdContext,
  IdCustomHTTPServer,
  IdIOHandler,
  Sidekiq4D.Web,
  Sidekiq4D.Web.WebSocket,
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.DeadLetter,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Periodic;

type
  { Thread que faz broadcast de overview pelo WebSocket a cada 1 segundo. }
  TSidekiqWsBroadcastThread = class(TThread)
  private
    FHub    : ISidekiqWebSocketHub;
    FGetJson: TFunc<string>;
  protected
    procedure Execute; override;
  public
    constructor Create(
      const AHub    : ISidekiqWebSocketHub;
      const AGetJson: TFunc<string>);
  end;

  TSidekiqIndyWebDashboard = class(TInterfacedObject, ISidekiqWebDashboard)
  private
    FServer          : TIdHTTPServer;
    FConfig          : TSidekiqWebDashboardConfig;
    FLock            : TCriticalSection;
    FRunning         : Boolean;
    FStartTime       : TDateTime;
    FWsHub            : ISidekiqWebSocketHub;
    FWsBroadcastThread: TSidekiqWsBroadcastThread;

    procedure HandleCommand(
      AContext     : TIdContext;
      ARequestInfo : TIdHTTPRequestInfo;
      AResponseInfo: TIdHTTPResponseInfo);

    // ── API handlers ──────────────────────────────────────────────────────
    function HandleApiHealth: string;
    function HandleApiPrometheusMetrics: string;
    function HandleApiOverview: string;
    function HandleApiQueues: string;
    function HandleApiWorkers: string;
    function HandleApiScheduled: string;
    function HandleApiPeriodic: string;
    function HandleApiBatches: string;
    function HandleApiMetrics: string;
    function HandleApiLocks: string;
    function HandleApiDlq(const AAction, AQueue, AFrom, ATo: string): string;
    function HandleApiQueuePause(const AName: string): Integer;
    function HandleApiQueueResume(const AName: string): Integer;
    function HandleApiDlqRetryAll: Integer;
    function HandleApiDlqRetryFiltered(const AAction, AQueue, AFrom, ATo: string): Integer;
    function HandleApiDlqClear: Integer;
    function HandleApiDlqRetry(const AJobId: string): Integer;
    function HandleApiDlqDelete(const AJobId: string): Integer;
    function HandleApiScheduledDelete(
      const AQueue, AAction, ADueAt: string): Integer;

    // ── WebSocket ─────────────────────────────────────────────────────────
    procedure HandleWebSocketUpgrade(AContext: TIdContext;
      ARequestInfo: TIdHTTPRequestInfo);
    class function ComputeWsAccept(const AKey: string): string; static;

    // ── HTML SPA ─────────────────────────────────────────────────────────
    class function BuildHtml: string; static;

    // ── Helpers ───────────────────────────────────────────────────────────
    class function JsonEscape(const AValue: string): string; static;
    class function DateToIso(const AValue: TDateTime): string; static;
    class function ExtractSegment(
      const APath : string;
      const AIndex: Integer): string; static;
    class function FailedAtToStr(const AValue: TDateTime): string; static;

  public
    constructor Create(const AConfig: TSidekiqWebDashboardConfig);
    destructor Destroy; override;

    class function New(
      const AConfig: TSidekiqWebDashboardConfig): ISidekiqWebDashboard;

    { ISidekiqWebDashboard }
    procedure Start(const APort: Word);
    procedure Stop;
    function IsRunning: Boolean;
  end;

implementation

{ TSidekiqIndyWebDashboard }

{ TSidekiqWsBroadcastThread }

constructor TSidekiqWsBroadcastThread.Create(
  const AHub    : ISidekiqWebSocketHub;
  const AGetJson: TFunc<string>);
begin
  inherited Create(True);
  FHub     := AHub;
  FGetJson := AGetJson;
  FreeOnTerminate := False;
end;

procedure TSidekiqWsBroadcastThread.Execute;
begin
  while not Terminated do
  begin
    try
      if FHub.ConnectedClients > 0 then
        FHub.BroadcastMetrics(FGetJson());
    except
      { ignora erros de broadcast — clientes mortos são limpos internamente }
    end;
    Sleep(1000);
  end;
end;

{ TSidekiqIndyWebDashboard }

constructor TSidekiqIndyWebDashboard.Create(
  const AConfig: TSidekiqWebDashboardConfig);
begin
  inherited Create;
  FConfig             := AConfig;
  FLock               := TCriticalSection.Create;
  FRunning            := False;
  FStartTime          := Now;
  FWsHub              := TSidekiqInMemoryWebSocketHub.New;
  FServer             := TIdHTTPServer.Create(nil);
  FServer.OnCommandGet   := HandleCommand;
  FServer.OnCommandOther := HandleCommand;
  FWsBroadcastThread  := TSidekiqWsBroadcastThread.Create(
    FWsHub,
    function: string
    begin
      Result := HandleApiOverview;
    end);
  FWsBroadcastThread.Start;
end;

destructor TSidekiqIndyWebDashboard.Destroy;
begin
  Stop;
  FWsBroadcastThread.Terminate;
  FWsBroadcastThread.WaitFor;
  FWsBroadcastThread.Free;
  FServer.Free;
  FWsHub := nil;
  FLock.Free;
  inherited;
end;

class function TSidekiqIndyWebDashboard.New(
  const AConfig: TSidekiqWebDashboardConfig): ISidekiqWebDashboard;
begin
  Result := TSidekiqIndyWebDashboard.Create(AConfig);
end;

procedure TSidekiqIndyWebDashboard.Start(const APort: Word);
begin
  FLock.Acquire;
  try
    if FRunning then
      Exit;
    FServer.DefaultPort := APort;
    FServer.Active      := True;
    FRunning            := True;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqIndyWebDashboard.Stop;
begin
  FLock.Acquire;
  try
    if not FRunning then
      Exit;
    FServer.Active := False;
    FRunning       := False;
  finally
    FLock.Release;
  end;
end;

function TSidekiqIndyWebDashboard.IsRunning: Boolean;
begin
  FLock.Acquire;
  try
    Result := FRunning;
  finally
    FLock.Release;
  end;
end;

// ── Route dispatch ────────────────────────────────────────────────────────────

procedure TSidekiqIndyWebDashboard.HandleCommand(
  AContext     : TIdContext;
  ARequestInfo : TIdHTTPRequestInfo;
  AResponseInfo: TIdHTTPResponseInfo);
var
  LPath  : string;
  LMethod: string;
  LName  : string;
  LJobId : string;
  LStatus: Integer;
begin
  LPath   := ARequestInfo.Document;
  LMethod := ARequestInfo.Command.ToUpper;

  AResponseInfo.ContentEncoding := 'UTF-8';

  // GET /
  if (LMethod = 'GET') and (LPath = '/') then
  begin
    AResponseInfo.ContentType := 'text/html; charset=utf-8';
    AResponseInfo.ContentText := BuildHtml;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/overview
  if (LMethod = 'GET') and (LPath = '/api/overview') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiOverview;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/queues
  if (LMethod = 'GET') and (LPath = '/api/queues') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiQueues;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/workers
  if (LMethod = 'GET') and (LPath = '/api/workers') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiWorkers;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/scheduled
  if (LMethod = 'GET') and (LPath = '/api/scheduled') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiScheduled;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/periodic
  if (LMethod = 'GET') and (LPath = '/api/periodic') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiPeriodic;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/batches
  if (LMethod = 'GET') and (LPath = '/api/batches') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiBatches;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/metrics
  if (LMethod = 'GET') and (LPath = '/api/metrics') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiMetrics;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/locks
  if (LMethod = 'GET') and (LPath = '/api/locks') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiLocks;
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // GET /api/dlq
  if (LMethod = 'GET') and (LPath = '/api/dlq') then
  begin
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiDlq(
      ARequestInfo.Params.Values['action'],
      ARequestInfo.Params.Values['queue'],
      ARequestInfo.Params.Values['from'],
      ARequestInfo.Params.Values['to']);
    AResponseInfo.ResponseNo  := 200;
    Exit;
  end;

  // POST /api/queues/:name/pause
  if (LMethod = 'POST') and LPath.StartsWith('/api/queues/') and
    LPath.EndsWith('/pause') then
  begin
    LName   := ExtractSegment(LPath, 3);
    LStatus := HandleApiQueuePause(LName);
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // POST /api/queues/:name/resume
  if (LMethod = 'POST') and LPath.StartsWith('/api/queues/') and
    LPath.EndsWith('/resume') then
  begin
    LName   := ExtractSegment(LPath, 3);
    LStatus := HandleApiQueueResume(LName);
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // POST /api/dlq/retry-filtered
  if (LMethod = 'POST') and (LPath = '/api/dlq/retry-filtered') then
  begin
    LStatus := HandleApiDlqRetryFiltered(
      ARequestInfo.Params.Values['action'],
      ARequestInfo.Params.Values['queue'],
      ARequestInfo.Params.Values['from'],
      ARequestInfo.Params.Values['to']);
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // POST /api/dlq/retry-all  (must come before POST /api/dlq/:id/retry)
  if (LMethod = 'POST') and (LPath = '/api/dlq/retry-all') then
  begin
    LStatus := HandleApiDlqRetryAll;
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // DELETE /api/dlq  (exact — clear all)
  if (LMethod = 'DELETE') and (LPath = '/api/dlq') then
  begin
    LStatus := HandleApiDlqClear;
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // POST /api/dlq/:id/retry
  if (LMethod = 'POST') and LPath.StartsWith('/api/dlq/') and
    LPath.EndsWith('/retry') then
  begin
    LJobId  := ExtractSegment(LPath, 3);
    LStatus := HandleApiDlqRetry(LJobId);
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // DELETE /api/dlq/:id
  if (LMethod = 'DELETE') and LPath.StartsWith('/api/dlq/') then
  begin
    LJobId  := ExtractSegment(LPath, 3);
    LStatus := HandleApiDlqDelete(LJobId);
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := '{"ok":' + BoolToStr(LStatus = 200, True).ToLower + '}';
    AResponseInfo.ResponseNo  := LStatus;
    Exit;
  end;

  // GET /api/events — Server-Sent Events: push de overview a cada 1s.
  // O browser usa EventSource; cai automaticamente para polling de 5s se SSE falhar.
  if (LMethod = 'GET') and (LPath = '/api/events') then
  begin
    try
      AContext.Connection.IOHandler.WriteLn('HTTP/1.1 200 OK');
      AContext.Connection.IOHandler.WriteLn('Content-Type: text/event-stream');
      AContext.Connection.IOHandler.WriteLn('Cache-Control: no-cache');
      AContext.Connection.IOHandler.WriteLn('Connection: keep-alive');
      AContext.Connection.IOHandler.WriteLn('X-Accel-Buffering: no');
      AContext.Connection.IOHandler.WriteLn('Access-Control-Allow-Origin: *');
      AContext.Connection.IOHandler.WriteLn('');
      while AContext.Connection.Connected do
      begin
        try
          AContext.Connection.IOHandler.WriteLn('data: ' + HandleApiOverview);
          AContext.Connection.IOHandler.WriteLn('');
        except
          Break;
        end;
        Sleep(1000);
      end;
    except
      // Desconexão do cliente é evento normal em SSE — não propaga
    end;
    AContext.Connection.Disconnect;
    Exit;
  end;

  // GET /health
  if (LMethod = 'GET') and (LPath = '/health') then
  begin
    AResponseInfo.ResponseNo  := 200;
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := HandleApiHealth;
    Exit;
  end;

  // GET /metrics  — Prometheus text format
  if (LMethod = 'GET') and (LPath = '/metrics') then
  begin
    AResponseInfo.ResponseNo  := 200;
    AResponseInfo.ContentType := 'text/plain; version=0.0.4; charset=utf-8';
    AResponseInfo.ContentText := HandleApiPrometheusMetrics;
    Exit;
  end;

  // DELETE /api/scheduled  — cancela job agendado (query: queue, action, due_at)
  if (LMethod = 'DELETE') and (LPath = '/api/scheduled') then
  begin
    LStatus := HandleApiScheduledDelete(
      ARequestInfo.Params.Values['queue'],
      ARequestInfo.Params.Values['action'],
      ARequestInfo.Params.Values['due_at']);
    AResponseInfo.ResponseNo  := 200;
    AResponseInfo.ContentType := 'application/json';
    AResponseInfo.ContentText := Format('{"cancelled":%d}', [LStatus]);
    Exit;
  end;

  // GET /ws — WebSocket upgrade (RFC 6455)
  if (LMethod = 'GET') and (LPath = '/ws') then
  begin
    HandleWebSocketUpgrade(AContext, ARequestInfo);
    Exit;
  end;

  AResponseInfo.ResponseNo  := 404;
  AResponseInfo.ContentType := 'application/json';
  AResponseInfo.ContentText := '{"error":"not found"}';
end;

// ── API handlers ──────────────────────────────────────────────────────────────

function TSidekiqIndyWebDashboard.HandleApiOverview: string;
var
  LObj       : TJSONObject;
  LWorkers   : Integer;
  LMaxConc   : Integer;
  LEnqueued  : Integer;
  LDlqCount  : Integer;
  LSchedCount: Integer;
  LProcessed : Int64;
  LFailed    : Int64;
  LIsLeader  : Boolean;
  LIsDraining: Boolean;
  LQueue     : ISidekiqQueueAdapter;
  LDepth     : ISidekiqQueueDepth;
  LSnaps     : TArray<TSidekiqHistoricalMetricSnapshot>;
  LSnap      : TSidekiqHistoricalMetricSnapshot;
begin
  LWorkers    := 0;
  LMaxConc    := 0;
  LIsLeader   := False;
  LIsDraining := False;
  if Assigned(FConfig.DashboardServer) then
  begin
    LWorkers    := FConfig.DashboardServer.ActiveWorkerCount;
    LMaxConc    := FConfig.DashboardServer.MaxConcurrencyCount;
    LIsLeader   := FConfig.DashboardServer.IsServerLeader;
    LIsDraining := FConfig.DashboardServer.IsServerDraining;
  end;

  LEnqueued := 0;
  for LQueue in FConfig.Queues do
    if Supports(LQueue, ISidekiqQueueDepth, LDepth) then
      Inc(LEnqueued, LDepth.Depth);

  LDlqCount := 0;
  if Assigned(FConfig.DeadLetterQueue) then
    LDlqCount := FConfig.DeadLetterQueue.Count;

  LSchedCount := 0;
  if Assigned(FConfig.ScheduledStore) then
    LSchedCount := FConfig.ScheduledStore.Count;

  LProcessed := 0;
  LFailed    := 0;
  if Assigned(FConfig.Metrics) then
  begin
    LSnaps := FConfig.Metrics.Snapshot;
    for LSnap in LSnaps do
    begin
      Inc(LProcessed, LSnap.SucceededJobs);
      Inc(LFailed,    LSnap.FailedJobs);
    end;
  end;

  LObj := TJSONObject.Create;
  try
    LObj.AddPair('workers',        TJSONNumber.Create(LWorkers));
    LObj.AddPair('max_concurrency',TJSONNumber.Create(LMaxConc));
    LObj.AddPair('enqueued',       TJSONNumber.Create(LEnqueued));
    LObj.AddPair('dlq_count',      TJSONNumber.Create(LDlqCount));
    LObj.AddPair('scheduled_count',TJSONNumber.Create(LSchedCount));
    LObj.AddPair('is_leader',      TJSONBool.Create(LIsLeader));
    LObj.AddPair('is_draining',    TJSONBool.Create(LIsDraining));
    LObj.AddPair('processed_1h',   TJSONNumber.Create(LProcessed));
    LObj.AddPair('failed_1h',      TJSONNumber.Create(LFailed));
    Result := LObj.ToJSON;
  finally
    LObj.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiQueues: string;
var
  LArr     : TJSONArray;
  LObj     : TJSONObject;
  LQueue   : ISidekiqQueueAdapter;
  LDepth   : ISidekiqQueueDepth;
  LDepthVal: Integer;
  LPaused  : Boolean;
begin
  LArr := TJSONArray.Create;
  try
    for LQueue in FConfig.Queues do
    begin
      if Supports(LQueue, ISidekiqQueueDepth, LDepth) then
        LDepthVal := LDepth.Depth
      else
        LDepthVal := -1;

      LPaused := Assigned(FConfig.StateStore) and
        FConfig.StateStore.Exists('queue:paused:' + LQueue.Name.Trim.ToLower);

      LObj := TJSONObject.Create;
      LObj.AddPair('name',   LQueue.Name);
      LObj.AddPair('depth',  TJSONNumber.Create(LDepthVal));
      LObj.AddPair('paused', TJSONBool.Create(LPaused));
      LArr.AddElement(LObj);
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiWorkers: string;
var
  LArr   : TJSONArray;
  LObj   : TJSONObject;
  LKeys  : TArray<string>;
  LKey   : string;
  LJson  : string;
  LParsed: TJSONValue;
  LWorker: TJSONObject;
begin
  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.StateStore) then
    begin
      LKeys := FConfig.StateStore.ListKeys('server_reliability:active:');
      for LKey in LKeys do
      begin
        LJson := FConfig.StateStore.Get(LKey);
        if LJson.Trim.IsEmpty then
          Continue;
        LParsed := TJSONObject.ParseJSONValue(LJson);
        if not Assigned(LParsed) then
          Continue;
        try
          if not (LParsed is TJSONObject) then
            Continue;
          LWorker := TJSONObject(LParsed);
          LObj := TJSONObject.Create;
          LObj.AddPair('execution_id', LWorker.GetValue<string>('execution_id', ''));
          LObj.AddPair('job_id',       LWorker.GetValue<string>('job_id', ''));
          LObj.AddPair('action',       LWorker.GetValue<string>('action', ''));
          LObj.AddPair('queue_name',   LWorker.GetValue<string>('queue_name', ''));
          LObj.AddPair('lease_ttl',    TJSONNumber.Create(
            LWorker.GetValue<Integer>('lease_ttl_seconds', 0)));
          LArr.AddElement(LObj);
        finally
          LParsed.Free;
        end;
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiScheduled: string;
var
  LArr    : TJSONArray;
  LObj    : TJSONObject;
  LEntries: TArray<TSidekiqScheduledEntry>;
  LEntry  : TSidekiqScheduledEntry;
begin
  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.ScheduledStore) then
    begin
      LEntries := FConfig.ScheduledStore.List;
      for LEntry in LEntries do
      begin
        LObj := TJSONObject.Create;
        LObj.AddPair('queue_name', LEntry.QueueName);
        LObj.AddPair('action',     LEntry.Action);
        LObj.AddPair('body',       LEntry.Body);
        LObj.AddPair('due_at',     DateToIso(LEntry.DueAt));
        LArr.AddElement(LObj);
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiPeriodic: string;
var
  LArr : TJSONArray;
  LObj : TJSONObject;
  LJobs: TArray<TSidekiqPeriodicDefinition>;
  LJob : TSidekiqPeriodicDefinition;
begin
  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.DashboardServer) then
    begin
      LJobs := FConfig.DashboardServer.ListPeriodicJobs;
      for LJob in LJobs do
      begin
        LObj := TJSONObject.Create;
        LObj.AddPair('name',            LJob.Name);
        LObj.AddPair('cron_expression', LJob.CronExpression);
        LObj.AddPair('queue_name',      LJob.QueueName);
        LObj.AddPair('action',          LJob.Action);
        LObj.AddPair('body',            LJob.Body);
        LArr.AddElement(LObj);
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiBatches: string;
var
  LArr    : TJSONArray;
  LObj    : TJSONObject;
  LKeys   : TArray<string>;
  LKey    : string;
  LId     : string;
  LJson   : string;
  LParsed : TJSONValue;
  LBatch  : TJSONObject;
  LTotal  : Integer;
  LPending: Integer;
  LFail   : Integer;
begin
  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.StateStore) then
    begin
      LKeys := FConfig.StateStore.ListKeys('batch:');
      for LKey in LKeys do
      begin
        if not LKey.EndsWith(':state') then
          Continue;
        // Key format: batch:<id>:state  (prefix 6, suffix 6)
        if LKey.Length <= 12 then
          Continue;
        LId := LKey.Substring(6, LKey.Length - 12);
        LJson := FConfig.StateStore.Get(LKey);
        if LJson.Trim.IsEmpty then
          Continue;
        LParsed := TJSONObject.ParseJSONValue(LJson);
        if not Assigned(LParsed) then
          Continue;
        try
          if not (LParsed is TJSONObject) then
            Continue;
          LBatch   := TJSONObject(LParsed);
          LTotal   := LBatch.GetValue<Integer>('total', 0);
          LPending := LBatch.GetValue<Integer>('pending', 0);
          LFail    := LBatch.GetValue<Integer>('failures', 0);
          LObj := TJSONObject.Create;
          LObj.AddPair('id',          LId);
          LObj.AddPair('description', LBatch.GetValue<string>('description', ''));
          LObj.AddPair('total',       TJSONNumber.Create(LTotal));
          LObj.AddPair('pending',     TJSONNumber.Create(LPending));
          LObj.AddPair('failures',    TJSONNumber.Create(LFail));
          LArr.AddElement(LObj);
        finally
          LParsed.Free;
        end;
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiMetrics: string;
var
  LArr  : TJSONArray;
  LObj  : TJSONObject;
  LSnaps: TArray<TSidekiqHistoricalMetricSnapshot>;
  LSnap : TSidekiqHistoricalMetricSnapshot;
begin
  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.Metrics) then
    begin
      LSnaps := FConfig.Metrics.Snapshot;
      for LSnap in LSnaps do
      begin
        LObj := TJSONObject.Create;
        LObj.AddPair('bucket_started_at', DateToIso(LSnap.BucketStartedAt));
        LObj.AddPair('succeeded',   TJSONNumber.Create(LSnap.SucceededJobs));
        LObj.AddPair('failed',      TJSONNumber.Create(LSnap.FailedJobs));
        LObj.AddPair('retried',     TJSONNumber.Create(LSnap.RetriedJobs));
        LObj.AddPair('discarded',   TJSONNumber.Create(LSnap.DiscardedJobs));
        LObj.AddPair('dead_lettered', TJSONNumber.Create(LSnap.DeadLetteredJobs));
        LObj.AddPair('avg_duration_ms',
          TJSONNumber.Create(LSnap.AverageDurationMs));
        LArr.AddElement(LObj);
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiLocks: string;
var
  LArr : TJSONArray;
  LObj : TJSONObject;
  LKeys: TArray<string>;
  LKey : string;
begin
  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.StateStore) then
    begin
      LKeys := FConfig.StateStore.ListKeys('lock:');
      for LKey in LKeys do
      begin
        LObj := TJSONObject.Create;
        LObj.AddPair('key', LKey);
        LArr.AddElement(LObj);
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TryParseDateFilter(const AStr: string; out ADate: TDateTime): Boolean;
var
  LParts: TArray<string>;
  Y, M, D: Integer;
begin
  Result := False;
  if AStr.IsEmpty then Exit;
  LParts := AStr.Split(['-']);
  if Length(LParts) <> 3 then Exit;
  if not TryStrToInt(LParts[0], Y) then Exit;
  if not TryStrToInt(LParts[1], M) then Exit;
  if not TryStrToInt(LParts[2], D) then Exit;
  try
    ADate  := EncodeDate(Y, M, D);
    Result := True;
  except
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiDlq(
  const AAction, AQueue, AFrom, ATo: string): string;
var
  LArr       : TJSONArray;
  LObj       : TJSONObject;
  LEntries   : TArray<TSidekiqDeadLetterEntry>;
  LEntry     : TSidekiqDeadLetterEntry;
  LFromDt    : TDateTime;
  LToDt      : TDateTime;
  LHasFrom   : Boolean;
  LHasTo     : Boolean;
  LEntryDate : TDateTime;
begin
  LHasFrom := TryParseDateFilter(AFrom, LFromDt);
  LHasTo   := TryParseDateFilter(ATo,   LToDt);

  LArr := TJSONArray.Create;
  try
    if Assigned(FConfig.DeadLetterQueue) then
    begin
      LEntries := FConfig.DeadLetterQueue.List;
      for LEntry in LEntries do
      begin
        if (not AAction.IsEmpty) and
           not LEntry.Action.ToLower.Contains(AAction.ToLower) then
          Continue;
        if (not AQueue.IsEmpty) and
           not LEntry.OriginalQueue.ToLower.Contains(AQueue.ToLower) then
          Continue;
        LEntryDate := Trunc(LEntry.FailedAt);
        if LHasFrom and (LEntryDate < LFromDt) then Continue;
        if LHasTo   and (LEntryDate > LToDt)   then Continue;

        LObj := TJSONObject.Create;
        LObj.AddPair('job_id',         LEntry.JobId);
        LObj.AddPair('action',         LEntry.Action);
        LObj.AddPair('original_queue', LEntry.OriginalQueue);
        LObj.AddPair('error_message',  LEntry.ErrorMessage);
        LObj.AddPair('retry_count',    TJSONNumber.Create(LEntry.RetryCount));
        LObj.AddPair('failed_at',      FailedAtToStr(LEntry.FailedAt));
        LArr.AddElement(LObj);
      end;
    end;
    Result := LArr.ToJSON;
  finally
    LArr.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiQueuePause(
  const AName: string): Integer;
begin
  if AName.IsEmpty or not Assigned(FConfig.DashboardServer) then
    Exit(400);
  try
    FConfig.DashboardServer.PauseQueueByName(AName);
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiQueueResume(
  const AName: string): Integer;
begin
  if AName.IsEmpty or not Assigned(FConfig.DashboardServer) then
    Exit(400);
  try
    FConfig.DashboardServer.ResumeQueueByName(AName);
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiDlqRetryAll: Integer;
var
  LEntries: TArray<TSidekiqDeadLetterEntry>;
  LEntry  : TSidekiqDeadLetterEntry;
begin
  if not Assigned(FConfig.DeadLetterQueue) then
    Exit(400);
  try
    LEntries := FConfig.DeadLetterQueue.List;
    for LEntry in LEntries do
      FConfig.DeadLetterQueue.Retry(LEntry.JobId);
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiDlqRetryFiltered(
  const AAction, AQueue, AFrom, ATo: string): Integer;
var
  LEntries   : TArray<TSidekiqDeadLetterEntry>;
  LEntry     : TSidekiqDeadLetterEntry;
  LFromDt    : TDateTime;
  LToDt      : TDateTime;
  LHasFrom   : Boolean;
  LHasTo     : Boolean;
  LEntryDate : TDateTime;
begin
  if not Assigned(FConfig.DeadLetterQueue) then
    Exit(400);

  LHasFrom := TryParseDateFilter(AFrom, LFromDt);
  LHasTo   := TryParseDateFilter(ATo,   LToDt);
  try
    LEntries := FConfig.DeadLetterQueue.List;
    for LEntry in LEntries do
    begin
      if (not AAction.IsEmpty) and
         not LEntry.Action.ToLower.Contains(AAction.ToLower) then
        Continue;
      if (not AQueue.IsEmpty) and
         not LEntry.OriginalQueue.ToLower.Contains(AQueue.ToLower) then
        Continue;
      LEntryDate := Trunc(LEntry.FailedAt);
      if LHasFrom and (LEntryDate < LFromDt) then Continue;
      if LHasTo   and (LEntryDate > LToDt)   then Continue;
      try
        FConfig.DeadLetterQueue.Retry(LEntry.JobId);
      except
      end;
    end;
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiDlqClear: Integer;
var
  LEntries: TArray<TSidekiqDeadLetterEntry>;
  LEntry  : TSidekiqDeadLetterEntry;
begin
  if not Assigned(FConfig.DeadLetterQueue) then
    Exit(400);
  try
    LEntries := FConfig.DeadLetterQueue.List;
    for LEntry in LEntries do
      FConfig.DeadLetterQueue.Delete(LEntry.JobId);
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiDlqRetry(
  const AJobId: string): Integer;
begin
  if AJobId.IsEmpty or not Assigned(FConfig.DeadLetterQueue) then
    Exit(400);
  try
    FConfig.DeadLetterQueue.Retry(AJobId);
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiDlqDelete(
  const AJobId: string): Integer;
begin
  if AJobId.IsEmpty or not Assigned(FConfig.DeadLetterQueue) then
    Exit(400);
  try
    FConfig.DeadLetterQueue.Delete(AJobId);
    Result := 200;
  except
    Result := 500;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiHealth: string;
begin
  Result := Format('{"status":"ok","uptime":%d}',
    [SecondsBetween(Now, FStartTime)]);
end;

function TSidekiqIndyWebDashboard.HandleApiPrometheusMetrics: string;
var
  LWorkers, LMaxConc, LEnqueued, LDlqCount, LSchedCount: Integer;
  LProcessed, LFailed: Int64;
  LQueue: ISidekiqQueueAdapter;
  LDepth: ISidekiqQueueDepth;
  LSnaps: TArray<TSidekiqHistoricalMetricSnapshot>;
  LSnap : TSidekiqHistoricalMetricSnapshot;
  LSb   : TStringBuilder;
begin
  LWorkers  := 0;
  LMaxConc  := 0;
  if Assigned(FConfig.DashboardServer) then
  begin
    LWorkers := FConfig.DashboardServer.ActiveWorkerCount;
    LMaxConc := FConfig.DashboardServer.MaxConcurrencyCount;
  end;
  LEnqueued := 0;
  for LQueue in FConfig.Queues do
    if Supports(LQueue, ISidekiqQueueDepth, LDepth) then
      Inc(LEnqueued, LDepth.Depth);
  LDlqCount := 0;
  if Assigned(FConfig.DeadLetterQueue) then
    LDlqCount := FConfig.DeadLetterQueue.Count;
  LSchedCount := 0;
  if Assigned(FConfig.ScheduledStore) then
    LSchedCount := FConfig.ScheduledStore.Count;
  LProcessed := 0;
  LFailed    := 0;
  if Assigned(FConfig.Metrics) then
  begin
    LSnaps := FConfig.Metrics.Snapshot;
    for LSnap in LSnaps do
    begin
      Inc(LProcessed, LSnap.SucceededJobs);
      Inc(LFailed,    LSnap.FailedJobs);
    end;
  end;

  LSb := TStringBuilder.Create;
  try
    LSb.AppendLine('# HELP sidekiq4d_workers_active Active worker threads');
    LSb.AppendLine('# TYPE sidekiq4d_workers_active gauge');
    LSb.AppendFormat('sidekiq4d_workers_active %d', [LWorkers]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_max_concurrency Configured max concurrency');
    LSb.AppendLine('# TYPE sidekiq4d_max_concurrency gauge');
    LSb.AppendFormat('sidekiq4d_max_concurrency %d', [LMaxConc]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_jobs_enqueued Jobs currently in queues');
    LSb.AppendLine('# TYPE sidekiq4d_jobs_enqueued gauge');
    LSb.AppendFormat('sidekiq4d_jobs_enqueued %d', [LEnqueued]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_dlq_size Dead letter queue depth');
    LSb.AppendLine('# TYPE sidekiq4d_dlq_size gauge');
    LSb.AppendFormat('sidekiq4d_dlq_size %d', [LDlqCount]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_scheduled_jobs Scheduled jobs pending');
    LSb.AppendLine('# TYPE sidekiq4d_scheduled_jobs gauge');
    LSb.AppendFormat('sidekiq4d_scheduled_jobs %d', [LSchedCount]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_processed_total Jobs succeeded in last hour');
    LSb.AppendLine('# TYPE sidekiq4d_processed_total counter');
    LSb.AppendFormat('sidekiq4d_processed_total %d', [LProcessed]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_failed_total Jobs failed in last hour');
    LSb.AppendLine('# TYPE sidekiq4d_failed_total counter');
    LSb.AppendFormat('sidekiq4d_failed_total %d', [LFailed]); LSb.AppendLine;
    LSb.AppendLine('# HELP sidekiq4d_uptime_seconds Server uptime in seconds');
    LSb.AppendLine('# TYPE sidekiq4d_uptime_seconds counter');
    LSb.AppendFormat('sidekiq4d_uptime_seconds %d', [SecondsBetween(Now, FStartTime)]);
    LSb.AppendLine;
    Result := LSb.ToString;
  finally
    LSb.Free;
  end;
end;

function TSidekiqIndyWebDashboard.HandleApiScheduledDelete(
  const AQueue, AAction, ADueAt: string): Integer;
var
  LParts: TArray<string>;
  LDate : TArray<string>;
  LTime : TArray<string>;
  Y, Mo, D, H, Mi, S: Integer;
  LDueAt: TDateTime;
begin
  Result := 0;
  if not Assigned(FConfig.ScheduledStore) then Exit;
  if AQueue.IsEmpty or AAction.IsEmpty or ADueAt.IsEmpty then Exit;
  LParts := ADueAt.Split(['T']);
  if Length(LParts) <> 2 then Exit;
  LDate := LParts[0].Split(['-']);
  LTime := LParts[1].Split([':']);
  if (Length(LDate) <> 3) or (Length(LTime) <> 3) then Exit;
  if not (TryStrToInt(LDate[0], Y) and TryStrToInt(LDate[1], Mo) and
          TryStrToInt(LDate[2], D) and TryStrToInt(LTime[0], H)  and
          TryStrToInt(LTime[1], Mi) and TryStrToInt(LTime[2], S)) then Exit;
  try
    LDueAt := EncodeDateTime(Y, Mo, D, H, Mi, S, 0);
  except
    Exit;
  end;
  FConfig.ScheduledStore.Delete(AQueue, AAction, LDueAt);
  Result := 1;
end;

// ── WebSocket ─────────────────────────────────────────────────────────────────

{
  Computa o valor de Sec-WebSocket-Accept conforme RFC 6455 §4.2.2:
    accept = Base64( SHA1( key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" ) )
}
class function TSidekiqIndyWebDashboard.ComputeWsAccept(
  const AKey: string): string;
var
  LMagic: string;
  LHash : TBytes;
begin
  LMagic := AKey + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
  LHash  := THashSHA1.GetHashBytes(LMagic);
  Result := TNetEncoding.Base64.EncodeBytesToString(LHash);
end;

{
  Realiza o handshake WebSocket RFC 6455 diretamente no IOHandler Indy.
  Após o handshake, a conexão é mantida aberta; o cliente é registrado no hub
  e a thread mantém a conexão viva enquanto o cliente estiver conectado.
  Dados enviados pelo cliente são lidos e descartados (dashboard é read-only).
}
procedure TSidekiqIndyWebDashboard.HandleWebSocketUpgrade(
  AContext    : TIdContext;
  ARequestInfo: TIdHTTPRequestInfo);
var
  LKey   : string;
  LAccept: string;
  LIO    : TIdIOHandler;
begin
  LKey := ARequestInfo.RawHeaders.Values['Sec-WebSocket-Key'];
  if LKey.IsEmpty then
    Exit;

  LAccept := ComputeWsAccept(Trim(LKey));
  LIO     := AContext.Connection.IOHandler;

  { Envia resposta 101 Switching Protocols diretamente no socket. }
  LIO.WriteLn('HTTP/1.1 101 Switching Protocols');
  LIO.WriteLn('Upgrade: websocket');
  LIO.WriteLn('Connection: Upgrade');
  LIO.WriteLn('Sec-WebSocket-Accept: ' + LAccept);
  LIO.WriteLn('');

  { Registra no hub para receber broadcasts (passa TObject, implementação faz cast). }
  FWsHub.RegisterClient(LIO);

  { Lê e descarta frames do cliente enquanto conectado (ping/pong/close). }
  try
    while AContext.Connection.Connected do
    begin
      try
        LIO.ReadByte;  { retorna Byte — descartado; lança exceção ao desconectar }
      except
        Break;
      end;
      Sleep(100);
    end;
  except
    { Desconexão normal — não propaga. }
  end;
end;

// ── Helpers ───────────────────────────────────────────────────────────────────

class function TSidekiqIndyWebDashboard.JsonEscape(
  const AValue: string): string;
var
  LChar: Char;
  LBuilder: TStringBuilder;
begin
  LBuilder := TStringBuilder.Create;
  try
    for LChar in AValue do
    begin
      case LChar of
        '"':  LBuilder.Append('\"');
        '\':  LBuilder.Append('\\');
        '/':  LBuilder.Append('\/');
        #8:   LBuilder.Append('\b');
        #9:   LBuilder.Append('\t');
        #10:  LBuilder.Append('\n');
        #12:  LBuilder.Append('\f');
        #13:  LBuilder.Append('\r');
      else
        LBuilder.Append(LChar);
      end;
    end;
    Result := LBuilder.ToString;
  finally
    LBuilder.Free;
  end;
end;

class function TSidekiqIndyWebDashboard.DateToIso(
  const AValue: TDateTime): string;
begin
  if AValue = 0 then
    Result := ''
  else
    Result := FormatDateTime('yyyy-mm-dd"T"hh:nn:ss', AValue);
end;

class function TSidekiqIndyWebDashboard.FailedAtToStr(
  const AValue: TDateTime): string;
begin
  if AValue = 0 then
    Result := ''
  else
    Result := FormatDateTime('yyyy-mm-dd hh:nn:ss', AValue);
end;

class function TSidekiqIndyWebDashboard.ExtractSegment(
  const APath : string;
  const AIndex: Integer): string;
var
  LParts: TArray<string>;
begin
  LParts := APath.Split(['/']);
  if AIndex < Length(LParts) then
    Result := LParts[AIndex]
  else
    Result := '';
end;

// ── HTML SPA ──────────────────────────────────────────────────────────────────

class function TSidekiqIndyWebDashboard.BuildHtml: string;
var
  LB: TStringBuilder;
begin
  LB := TStringBuilder.Create;
  try
    LB.Append('<!DOCTYPE html><html lang="pt-BR"><head>');
    LB.Append('<meta charset="UTF-8">');
    LB.Append('<meta name="viewport" content="width=device-width,initial-scale=1">');
    LB.Append('<title>Sidekiq4D Dashboard</title>');
    LB.Append('<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">');
    LB.Append('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js">');
    LB.Append('<\/script>');
    LB.Append('<style>');
    LB.Append('body{margin:0;height:100vh;overflow:hidden}');
    LB.Append('.sidebar{width:220px;min-height:100vh;background:#1a1a2e;flex-shrink:0;overflow-y:auto}');
    LB.Append('.sidebar a{display:block;color:#9ca3af;padding:10px 20px;text-decoration:none;border-left:3px solid transparent;font-size:0.92rem}');
    LB.Append('.sidebar a:hover,.sidebar a.active{color:#fff;background:#16213e;border-left-color:#3b82f6}');
    LB.Append('.main-content{flex:1;padding:24px;overflow-y:auto;height:100vh}');
    LB.Append('.section{display:none}.section.active{display:block}');
    LB.Append('.card{border-radius:8px}');
    LB.Append('</style>');
    LB.Append('</head>');
    LB.Append('<body class="d-flex">');

    // Sidebar
    LB.Append('<nav class="sidebar py-3">');
    LB.Append('<div class="px-4 py-2 text-white fw-bold fs-6 mb-2" style="letter-spacing:1px">&#9889; SIDEKIQ4D</div>');
    LB.Append('<hr style="border-color:#374151;margin:8px 16px">');
    LB.Append('<a href="#overview" data-section="overview">&#128202; Vis&atilde;o Geral</a>');
    LB.Append('<a href="#queues" data-section="queues">&#128203; Filas</a>');
    LB.Append('<a href="#workers" data-section="workers">&#9881;&#65039; Workers</a>');
    LB.Append('<a href="#scheduled" data-section="scheduled">&#128336; Agendados</a>');
    LB.Append('<a href="#periodic" data-section="periodic">&#128260; Peri&oacute;dicos</a>');
    LB.Append('<a href="#batches" data-section="batches">&#128230; Batches</a>');
    LB.Append('<a href="#dlq" data-section="dlq">&#128128; Dead Letter</a>');
    LB.Append('<a href="#locks" data-section="locks">&#128274; Locks</a>');
    LB.Append('<a href="#metrics" data-section="metrics">&#128200; M&eacute;tricas</a>');
    LB.Append('<hr style="border-color:#374151;margin:8px 16px">');
    LB.Append('<div class="px-4 py-1 text-muted" style="font-size:0.75rem" id="conn-status">Conectando...</div>');
    LB.Append('</nav>');

    // Main content area
    LB.Append('<div class="main-content">');

    // Section: Overview
    LB.Append('<div id="section-overview" class="section">');
    LB.Append('<h5 class="mb-4">&#128202; Vis&atilde;o Geral</h5>');
    LB.Append('<div class="row g-3" id="overview-cards"></div>');
    LB.Append('</div>');

    // Section: Queues
    LB.Append('<div id="section-queues" class="section">');
    LB.Append('<h5 class="mb-3">&#128203; Filas</h5>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>Nome</th><th>Profundidade</th><th>Status</th><th>A&ccedil;&otilde;es</th></tr></thead>');
    LB.Append('<tbody id="body-queues"></tbody></table></div>');
    LB.Append('</div>');

    // Section: Workers
    LB.Append('<div id="section-workers" class="section">');
    LB.Append('<h5 class="mb-3">&#9881;&#65039; Workers Ativos</h5>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>Execution ID</th><th>Action</th><th>Fila</th><th>Lease TTL</th></tr></thead>');
    LB.Append('<tbody id="body-workers"></tbody></table></div>');
    LB.Append('</div>');

    // Section: Scheduled
    LB.Append('<div id="section-scheduled" class="section">');
    LB.Append('<h5 class="mb-3">&#128336; Jobs Agendados</h5>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>Fila</th><th>Action</th><th>Executa em</th><th>Body</th><th></th></tr></thead>');
    LB.Append('<tbody id="body-scheduled"></tbody></table></div>');
    LB.Append('</div>');

    // Section: Periodic
    LB.Append('<div id="section-periodic" class="section">');
    LB.Append('<h5 class="mb-3">&#128260; Jobs Peri&oacute;dicos</h5>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>Nome</th><th>Cron</th><th>Fila</th><th>Action</th></tr></thead>');
    LB.Append('<tbody id="body-periodic"></tbody></table></div>');
    LB.Append('</div>');

    // Section: Batches
    LB.Append('<div id="section-batches" class="section">');
    LB.Append('<h5 class="mb-3">&#128230; Batches</h5>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>ID</th><th>Descri&ccedil;&atilde;o</th><th>Total</th><th>Pendente</th><th>Falhas</th><th>Status</th></tr></thead>');
    LB.Append('<tbody id="body-batches"></tbody></table></div>');
    LB.Append('</div>');

    // Section: DLQ
    LB.Append('<div id="section-dlq" class="section">');
    LB.Append('<div class="d-flex align-items-center mb-3">');
    LB.Append('<h5 class="mb-0 me-2">&#128128; Dead Letter Queue</h5>');
    LB.Append('<span class="badge bg-danger me-auto" id="dlq-count">0</span>');
    LB.Append('<button class="btn btn-sm btn-warning me-2" id="btn-retry-all">Retry All</button>');
    LB.Append('<button class="btn btn-sm btn-danger" id="btn-clear-all">Clear All</button>');
    LB.Append('</div>');
    LB.Append('<div class="row g-2 mb-3 align-items-end">');
    LB.Append('<div class="col-auto"><input class="form-control form-control-sm" id="dlq-f-action" placeholder="Action (handler)"></div>');
    LB.Append('<div class="col-auto"><input class="form-control form-control-sm" id="dlq-f-queue" placeholder="Fila"></div>');
    LB.Append('<div class="col-auto"><label class="form-label mb-0 small">De</label><input class="form-control form-control-sm" type="date" id="dlq-f-from"></div>');
    LB.Append('<div class="col-auto"><label class="form-label mb-0 small">At&eacute;</label><input class="form-control form-control-sm" type="date" id="dlq-f-to"></div>');
    LB.Append('<div class="col-auto d-flex gap-1 mt-3">');
    LB.Append('<button class="btn btn-sm btn-primary" onclick="loadDlq()">Filtrar</button>');
    LB.Append('<button class="btn btn-sm btn-secondary" onclick="clearDlqFilters()">Limpar</button>');
    LB.Append('<button class="btn btn-sm btn-outline-warning" id="btn-retry-filtered">Retry Filtrado</button>');
    LB.Append('</div></div>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>Job ID</th><th>Action</th><th>Fila</th><th>Erro</th><th>Tentativas</th><th>Falhou em</th><th>A&ccedil;&otilde;es</th></tr></thead>');
    LB.Append('<tbody id="body-dlq"></tbody></table></div>');
    LB.Append('</div>');

    // Section: Locks
    LB.Append('<div id="section-locks" class="section">');
    LB.Append('<h5 class="mb-3">&#128274; Locks Ativos</h5>');
    LB.Append('<div class="table-responsive"><table class="table table-sm table-bordered table-hover">');
    LB.Append('<thead class="table-dark"><tr><th>Chave</th></tr></thead>');
    LB.Append('<tbody id="body-locks"></tbody></table></div>');
    LB.Append('</div>');

    // Section: Metrics
    LB.Append('<div id="section-metrics" class="section">');
    LB.Append('<h5 class="mb-3">&#128200; M&eacute;tricas Hist&oacute;ricas</h5>');
    LB.Append('<div class="row">');
    LB.Append('<div class="col-12 mb-4"><canvas id="chart-line" style="max-height:280px"></canvas></div>');
    LB.Append('<div class="col-12"><canvas id="chart-bar" style="max-height:280px"></canvas></div>');
    LB.Append('</div></div>');

    LB.Append('</div>'); // .main-content

    // JavaScript
    LB.Append('<script>');

    // Constants & state
    LB.Append('var SECS=["overview","queues","workers","scheduled","periodic","batches","dlq","locks","metrics"];');
    LB.Append('var cur="overview";');
    LB.Append('var lineChart=null,barChart=null;');

    // showSection
    LB.Append('function showSection(n){');
    LB.Append('cur=n;');
    LB.Append('SECS.forEach(function(s){');
    LB.Append('var el=document.getElementById("section-"+s);if(el)el.classList.remove("active");');
    LB.Append('var lk=document.querySelector("[data-section=\""+s+"\"]");if(lk)lk.classList.remove("active");');
    LB.Append('});');
    LB.Append('var ae=document.getElementById("section-"+n);if(ae)ae.classList.add("active");');
    LB.Append('var al=document.querySelector("[data-section=\""+n+"\"]");if(al)al.classList.add("active");');
    LB.Append('loadSection(n);');
    LB.Append('}');

    // loadSection
    LB.Append('function loadSection(n){');
    LB.Append('({overview:loadOverview,queues:loadQueues,workers:loadWorkers,');
    LB.Append('scheduled:loadScheduled,periodic:loadPeriodic,batches:loadBatches,');
    LB.Append('dlq:loadDlq,locks:loadLocks,metrics:loadMetrics}[n]||function(){})();');
    LB.Append('}');

    // Card helper
    LB.Append('function mkCard(title,val,color){');
    LB.Append('return "<div class=\"col-sm-6 col-md-4 col-lg-3\"><div class=\"card h-100 border-"+color+"\">"');
    LB.Append('+"<div class=\"card-body\"><p class=\"card-title text-muted small mb-1\">"+title+"</p>"');
    LB.Append('+"<h3 class=\"mb-0 text-"+color+"\">"+val+"</h3>"');
    LB.Append('+"</div></div></div>";}');

    // Empty row helper
    LB.Append('function emptyRow(cols,msg){');
    LB.Append('return "<tr><td colspan=\""+cols+"\" class=\"text-center text-muted py-3\">"+msg+"</td></tr>";}');

    // loadOverview
    LB.Append('function loadOverview(){');
    LB.Append('fetch("/api/overview").then(function(r){return r.json();}).then(function(d){');
    LB.Append('var h="";');
    LB.Append('h+=mkCard("Workers Ativos",d.workers+" / "+d.max_concurrency,"primary");');
    LB.Append('h+=mkCard("Enfileirados",d.enqueued,"info");');
    LB.Append('h+=mkCard("Dead Letter",d.dlq_count,"danger");');
    LB.Append('h+=mkCard("Agendados",d.scheduled_count,"warning");');
    LB.Append('h+=mkCard("Processados",d.processed_1h,"success");');
    LB.Append('h+=mkCard("Falhos",d.failed_1h,"danger");');
    LB.Append('h+=mkCard("Leader",d.is_leader?"Sim":"N&atilde;o",d.is_leader?"success":"secondary");');
    LB.Append('h+=mkCard("Draining",d.is_draining?"Sim":"N&atilde;o",d.is_draining?"warning":"secondary");');
    LB.Append('document.getElementById("overview-cards").innerHTML=h;');
    LB.Append('}).catch(function(){document.getElementById("overview-cards").innerHTML="<p class=\"text-danger\">Erro ao carregar</p>";})}');

    // loadQueues
    LB.Append('function loadQueues(){');
    LB.Append('fetch("/api/queues").then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-queues");b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(4,"Nenhuma fila configurada");return;}');
    LB.Append('data.forEach(function(q){');
    LB.Append('var d=q.depth<0?"?":q.depth;');
    LB.Append('var s=q.paused');
    LB.Append('?"<span class=\"badge bg-danger\">pausada</span>"');
    LB.Append(':"<span class=\"badge bg-success\">ativa</span>";');
    LB.Append('var btn=q.paused');
    LB.Append('?"<button class=\"btn btn-sm btn-success\" data-qn=\""+q.name+"\" onclick=\"doResumeQ(this.dataset.qn)\">Resume</button>"');
    LB.Append(':"<button class=\"btn btn-sm btn-warning\" data-qn=\""+q.name+"\" onclick=\"doPauseQ(this.dataset.qn)\">Pause</button>";');
    LB.Append('b.innerHTML+="<tr><td>"+q.name+"</td><td>"+d+"</td><td>"+s+"</td><td>"+btn+"</td></tr>";');
    LB.Append('});});}');
    LB.Append('function doPauseQ(n){fetch("/api/queues/"+encodeURIComponent(n)+"/pause",{method:"POST"}).then(function(){loadQueues();});}');
    LB.Append('function doResumeQ(n){fetch("/api/queues/"+encodeURIComponent(n)+"/resume",{method:"POST"}).then(function(){loadQueues();});}');

    // loadWorkers
    LB.Append('function loadWorkers(){');
    LB.Append('fetch("/api/workers").then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-workers");b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(4,"Nenhum worker ativo");return;}');
    LB.Append('data.forEach(function(w){');
    LB.Append('b.innerHTML+="<tr><td><code>"+w.execution_id+"</code></td><td>"+w.action+"</td><td>"+w.queue_name+"</td><td>"+w.lease_ttl+"s</td></tr>";');
    LB.Append('});});}');

    // loadScheduled
    LB.Append('function loadScheduled(){');
    LB.Append('fetch("/api/scheduled").then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-scheduled");b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(5,"Nenhum job agendado");return;}');
    LB.Append('data.forEach(function(e){');
    LB.Append('var btn="<button class=''btn btn-sm btn-danger'' onclick=''cancelScheduled(\\\""+e.queue_name+"\\\",\\\""+e.action+"\\\",\\\""+e.due_at+"\\\")'' >Cancelar</button>";');
    LB.Append('b.innerHTML+="<tr><td>"+e.queue_name+"</td><td>"+e.action+"</td><td>"+e.due_at+"</td><td><small>"+e.body+"</small></td><td>"+btn+"</td></tr>";');
    LB.Append('});});}');
    LB.Append('function cancelScheduled(q,a,d){');
    LB.Append('if(!confirm("Cancelar job "+a+" ("+d+") ?"))return;');
    LB.Append('fetch("/api/scheduled?queue="+encodeURIComponent(q)+"&action="+encodeURIComponent(a)+"&due_at="+encodeURIComponent(d),{method:"DELETE"})');
    LB.Append('.then(function(){loadScheduled();})');
    LB.Append('.catch(function(err){alert("Erro: "+err);});}');

    // loadPeriodic
    LB.Append('function loadPeriodic(){');
    LB.Append('fetch("/api/periodic").then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-periodic");b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(4,"Nenhum job peri&oacute;dico registrado");return;}');
    LB.Append('data.forEach(function(p){');
    LB.Append('b.innerHTML+="<tr><td>"+p.name+"</td><td><code>"+p.cron_expression+"</code></td><td>"+p.queue_name+"</td><td>"+p.action+"</td></tr>";');
    LB.Append('});});}');

    // loadBatches
    LB.Append('function loadBatches(){');
    LB.Append('fetch("/api/batches").then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-batches");b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(6,"Nenhum batch");return;}');
    LB.Append('data.forEach(function(bt){');
    LB.Append('var st=bt.failures>0?"<span class=\"badge bg-danger\">falho</span>"');
    LB.Append(':bt.pending===0?"<span class=\"badge bg-success\">completo</span>"');
    LB.Append(':"<span class=\"badge bg-primary\">em andamento</span>";');
    LB.Append('b.innerHTML+="<tr><td><small>"+bt.id+"</small></td><td>"+bt.description+"</td>"');
    LB.Append('+"<td>"+bt.total+"</td><td>"+bt.pending+"</td><td>"+bt.failures+"</td><td>"+st+"</td></tr>";');
    LB.Append('});});}');

    // loadDlq
    LB.Append('function dlqFilterQs(){');
    LB.Append('var a=document.getElementById("dlq-f-action").value;');
    LB.Append('var q=document.getElementById("dlq-f-queue").value;');
    LB.Append('var f=document.getElementById("dlq-f-from").value;');
    LB.Append('var t=document.getElementById("dlq-f-to").value;');
    LB.Append('var ps=[];');
    LB.Append('if(a)ps.push("action="+encodeURIComponent(a));');
    LB.Append('if(q)ps.push("queue="+encodeURIComponent(q));');
    LB.Append('if(f)ps.push("from="+encodeURIComponent(f));');
    LB.Append('if(t)ps.push("to="+encodeURIComponent(t));');
    LB.Append('return ps.length?"?"+ps.join("&"):"";}');
    LB.Append('function loadDlq(){');
    LB.Append('fetch("/api/dlq"+dlqFilterQs()).then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-dlq");');
    LB.Append('var cnt=document.getElementById("dlq-count");if(cnt)cnt.textContent=data.length;');
    LB.Append('b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(7,"DLQ vazia");return;}');
    LB.Append('data.forEach(function(e){');
    LB.Append('b.innerHTML+="<tr><td><small>"+e.job_id+"</small></td><td>"+e.action+"</td><td>"+e.original_queue+"</td>"');
    LB.Append('+"<td><small>"+e.error_message+"</small></td><td>"+e.retry_count+"</td><td>"+e.failed_at+"</td>"');
    LB.Append('+"<td>"');
    LB.Append('+"<button class=\"btn btn-sm btn-warning me-1\" data-jid=\""+e.job_id+"\" onclick=\"doRetryJob(this.dataset.jid)\">Retry</button>"');
    LB.Append('+"<button class=\"btn btn-sm btn-danger\" data-jid=\""+e.job_id+"\" onclick=\"doDeleteJob(this.dataset.jid)\">Del</button>"');
    LB.Append('+"</td></tr>";');
    LB.Append('});});}');
    LB.Append('function clearDlqFilters(){');
    LB.Append('["dlq-f-action","dlq-f-queue","dlq-f-from","dlq-f-to"].forEach(function(id){');
    LB.Append('document.getElementById(id).value="";});loadDlq();}');
    LB.Append('function doRetryJob(id){fetch("/api/dlq/"+encodeURIComponent(id)+"/retry",{method:"POST"}).then(function(){loadDlq();});}');
    LB.Append('function doDeleteJob(id){');
    LB.Append('if(!confirm("Confirmar exclus\u00e3o?"))return;');
    LB.Append('fetch("/api/dlq/"+encodeURIComponent(id),{method:"DELETE"}).then(function(){loadDlq();});}');

    // loadLocks
    LB.Append('function loadLocks(){');
    LB.Append('fetch("/api/locks").then(function(r){return r.json();}).then(function(data){');
    LB.Append('var b=document.getElementById("body-locks");b.innerHTML="";');
    LB.Append('if(!data.length){b.innerHTML=emptyRow(1,"Nenhum lock ativo");return;}');
    LB.Append('data.forEach(function(l){b.innerHTML+="<tr><td><code>"+l.key+"</code></td></tr>";});');
    LB.Append('});}');

    // loadMetrics
    LB.Append('function loadMetrics(){');
    LB.Append('fetch("/api/metrics").then(function(r){return r.json();}).then(function(data){');
    LB.Append('if(!data.length)return;');
    LB.Append('var lbl=data.map(function(d){return d.bucket_started_at;});');
    LB.Append('var ok=data.map(function(d){return d.succeeded;});');
    LB.Append('var err=data.map(function(d){return d.failed;});');
    LB.Append('var dur=data.map(function(d){return d.avg_duration_ms;});');
    LB.Append('if(lineChart)lineChart.destroy();');
    LB.Append('if(barChart)barChart.destroy();');
    LB.Append('var c1=document.getElementById("chart-line").getContext("2d");');
    LB.Append('lineChart=new Chart(c1,{type:"line",data:{labels:lbl,datasets:[');
    LB.Append('{label:"Processados",data:ok,borderColor:"#198754",backgroundColor:"rgba(25,135,84,0.08)",tension:0.2,fill:true},');
    LB.Append('{label:"Falhos",data:err,borderColor:"#dc3545",backgroundColor:"rgba(220,53,69,0.08)",tension:0.2,fill:true}');
    LB.Append(']},options:{responsive:true,plugins:{title:{display:true,text:"Processados vs Falhos (por bucket)"}}}});');
    LB.Append('var c2=document.getElementById("chart-bar").getContext("2d");');
    LB.Append('barChart=new Chart(c2,{type:"bar",data:{labels:lbl,datasets:[');
    LB.Append('{label:"Dura\u00e7\u00e3o M\u00e9dia (ms)",data:dur,backgroundColor:"rgba(13,110,253,0.6)"}');
    LB.Append(']},options:{responsive:true,plugins:{title:{display:true,text:"Dura\u00e7\u00e3o M\u00e9dia por Bucket (ms)"}}}});');
    LB.Append('});}');

    // DLQ bulk actions wired after DOM ready
    LB.Append('document.addEventListener("DOMContentLoaded",function(){');
    LB.Append('var ra=document.getElementById("btn-retry-all");');
    LB.Append('if(ra)ra.addEventListener("click",function(){');
    LB.Append('if(!confirm("Re-enfileirar todos os jobs da DLQ?"))return;');
    LB.Append('fetch("/api/dlq/retry-all",{method:"POST"}).then(function(){loadDlq();loadQueues();});});');
    LB.Append('var ca=document.getElementById("btn-clear-all");');
    LB.Append('if(ca)ca.addEventListener("click",function(){');
    LB.Append('if(!confirm("Apagar TODA a DLQ? Esta a\u00e7\u00e3o n\u00e3o pode ser desfeita."))return;');
    LB.Append('fetch("/api/dlq",{method:"DELETE"}).then(function(){loadDlq();});});');
    LB.Append('var rf=document.getElementById("btn-retry-filtered");');
    LB.Append('if(rf)rf.addEventListener("click",function(){');
    LB.Append('if(!confirm("Re-enfileirar os jobs filtrados?"))return;');
    LB.Append('fetch("/api/dlq/retry-filtered"+dlqFilterQs(),{method:"POST"}).then(function(){loadDlq();loadQueues();});});');
    LB.Append('});');

    // Hash navigation
    LB.Append('window.addEventListener("hashchange",function(){');
    LB.Append('var h=window.location.hash.replace("#","");');
    LB.Append('if(SECS.indexOf(h)>=0)showSection(h);');
    LB.Append('});');

    // Auto-refresh: WebSocket → SSE → polling (fallback chain)
    LB.Append('(function(){');
    LB.Append('var status=document.getElementById("conn-status");');
    LB.Append('function setStatus(t){if(status)status.textContent=t;}');
    LB.Append('function startPolling(){');
    LB.Append('setStatus("Polling 5s");');
    LB.Append('setInterval(function(){loadSection(cur);},5000);}');
    LB.Append('function openSSE(){');
    LB.Append('if(!window.EventSource){startPolling();return;}');
    LB.Append('setStatus("SSE...");');
    LB.Append('var retries=0;');
    LB.Append('var es=new EventSource("/api/events");');
    LB.Append('es.onmessage=function(){retries=0;setStatus("SSE •");loadSection(cur);};');
    LB.Append('es.onerror=function(){es.close();retries++;');
    LB.Append('if(retries<5)setTimeout(openSSE,3000);else startPolling();};}');
    LB.Append('function connectWS(){');
    LB.Append('if(!window.WebSocket){openSSE();return;}');
    LB.Append('setStatus("WS...");');
    LB.Append('var wsUrl=(location.protocol==="https:"?"wss":"ws")+"://"+location.host+"/ws";');
    LB.Append('var ws;');
    LB.Append('try{ws=new WebSocket(wsUrl);}catch(e){openSSE();return;}');
    LB.Append('ws.onopen=function(){setStatus("WS ●");};');
    LB.Append('ws.onmessage=function(e){');
    LB.Append('try{var d=JSON.parse(e.data);}catch(ex){return;}');
    LB.Append('if(cur==="overview"){');
    LB.Append('var h="";');
    LB.Append('h+=mkCard("Workers Ativos",d.workers+" / "+d.max_concurrency,"primary");');
    LB.Append('h+=mkCard("Enfileirados",d.enqueued,"info");');
    LB.Append('h+=mkCard("Dead Letter",d.dlq_count,"danger");');
    LB.Append('h+=mkCard("Agendados",d.scheduled_count,"warning");');
    LB.Append('h+=mkCard("Processados",d.processed_1h,"success");');
    LB.Append('h+=mkCard("Falhos",d.failed_1h,"danger");');
    LB.Append('h+=mkCard("Leader",d.is_leader?"Sim":"N&atilde;o",d.is_leader?"success":"secondary");');
    LB.Append('h+=mkCard("Draining",d.is_draining?"Sim":"N&atilde;o",d.is_draining?"warning":"secondary");');
    LB.Append('document.getElementById("overview-cards").innerHTML=h;');
    LB.Append('}};');
    LB.Append('ws.onerror=function(){ws.close();};');
    LB.Append('ws.onclose=function(){setStatus("WS reconect...");setTimeout(connectWS,3000);};');
    LB.Append('}');
    LB.Append('connectWS();');
    LB.Append('})();');

    // Init
    LB.Append('(function(){');
    LB.Append('var h=window.location.hash.replace("#","");');
    LB.Append('showSection(SECS.indexOf(h)>=0?h:"overview");');
    LB.Append('})();');

    LB.Append('<\/script>');
    LB.Append('</body></html>');

    Result := LB.ToString;
  finally
    LB.Free;
  end;
end;

end.
