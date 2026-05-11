unit Sidekiq4D.Ingress.HTTP;

// Queue adapter que escuta HTTP e converte requests em jobs.
// Zero dependencias de terceiros — requer injecao de ISidekiqHTTPServer.
//
// Uso:
//   Adapter := TSidekiqHTTPIngressAdapter.New
//     .Host('127.0.0.1')
//     .Port(9090)
//     .Targets(['elasticsearch', 'datadog'])
//     .UseHTTPServer(TSidekiqIndyHTTPServer.New);  // inject Indy impl
//
//   TSidekiqServer.New.UseQueue(Adapter)...
//
// Endpoints:
//   POST /events    -> enfileira 1 evento como job (1 job por target)
//   POST /batch     -> enfileira N eventos (N * targets jobs)
//   GET  /health    -> 200 OK

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.SyncObjs,
  System.Generics.Collections,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces;

type
  // Minimal HTTP request context passed to the server callback.
  TSidekiqHTTPContext = record
    Method: string;      // 'GET', 'POST', etc.
    Path: string;        // '/events', '/health', etc.
    AuthHeader: string;  // Authorization header value
    Body: string;        // Request body (may be empty for GET)
  end;

  // Callback type: handle a request and write back status + body.
  TSidekiqHTTPOnRequest = procedure(
    const AContext: TSidekiqHTTPContext;
    out AStatus: Integer;
    out AResponseBody: string) of object;

  // Port: HTTP server that calls the OnRequest callback for each request.
  ISidekiqHTTPServer = interface
    ['{3C4D5E6F-7A8B-9C0D-1E2F-3A4B5C6D7E8F}']
    procedure SetOnRequest(const ACallback: TSidekiqHTTPOnRequest);
    procedure Start(const AHost: string; APort: Word);
    procedure Stop;
  end;

  TSidekiqHTTPIngressAdapter = class(TInterfacedObject, ISidekiqQueueAdapter)
  private
    FLock: TCriticalSection;
    FHost: string;
    FPort: Word;
    FServer: ISidekiqHTTPServer;
    FQueue: TThreadedQueue<ISidekiqJobEnvelope>;
    FTargets: TArray<string>;
    FToken: string;
    // FRunning: protected by FLock in Start/Stop.
    FRunning: Boolean;
    // FJobCount: updated via TInterlocked.Increment; read via TInterlocked.Read.
    FJobCount: Int64;

    procedure HandleRequest(
      const AContext: TSidekiqHTTPContext;
      out AStatus: Integer;
      out AResponseBody: string);
    procedure EnqueueEvent(const ABody: string);
    procedure EnqueueBatch(const ABody: string);
    function CreateEnvelope(const AAction, ABody: string): ISidekiqJobEnvelope;
    function IsAuthorized(const AAuthHeader: string): Boolean;
    function GetRunning: Boolean;
    function GetJobCount: Int64;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqHTTPIngressAdapter;

    function Host(const AValue: string): TSidekiqHTTPIngressAdapter;
    function Port(AValue: Word): TSidekiqHTTPIngressAdapter;
    function Targets(const AValue: TArray<string>): TSidekiqHTTPIngressAdapter;
    function Token(const AValue: string): TSidekiqHTTPIngressAdapter;
    function UseHTTPServer(
      const AServer: ISidekiqHTTPServer): TSidekiqHTTPIngressAdapter;

    procedure Start;
    procedure Stop;

    function Name: string;
    function Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);
  end;

implementation

uses
  System.DateUtils;

type
  EHTTPIngressQueueFull = class(Exception);

  THTTPJobEnvelope = class(TInterfacedObject, ISidekiqJobEnvelope)
  private
    FId: string;
    FAction: string;
    FBody: string;
    FAttempts: Integer;
    FAttributes: TStrings;
  public
    constructor Create(const AId, AAction, ABody: string);
    destructor Destroy; override;
    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Action: string;
    function Body: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

{ THTTPJobEnvelope }

constructor THTTPJobEnvelope.Create(const AId, AAction, ABody: string);
begin
  inherited Create;
  FId := AId;
  FAction := AAction;
  FBody := ABody;
  FAttempts := 0;
  FAttributes := TStringList.Create;
end;

destructor THTTPJobEnvelope.Destroy;
begin
  FAttributes.Free;
  inherited;
end;

function THTTPJobEnvelope.Id: string;          begin Result := FId;           end;
function THTTPJobEnvelope.QueueName: string;   begin Result := 'http-ingress'; end;
function THTTPJobEnvelope.ReceiptHandle: string;begin Result := FId;           end;
function THTTPJobEnvelope.Action: string;      begin Result := FAction;       end;
function THTTPJobEnvelope.Body: string;        begin Result := FBody;         end;
function THTTPJobEnvelope.Attempts: Integer;   begin Result := FAttempts;     end;

function THTTPJobEnvelope.Attribute(const AName: string): string;
begin
  Result := FAttributes.Values[AName];
end;

{ TSidekiqHTTPIngressAdapter }

constructor TSidekiqHTTPIngressAdapter.Create;
begin
  inherited Create;
  FLock     := TCriticalSection.Create;
  FHost     := '127.0.0.1';
  FPort     := 9090;
  FTargets  := ['default'];
  FToken    := '';
  FRunning  := False;
  FJobCount := 0;
  FQueue    := TThreadedQueue<ISidekiqJobEnvelope>.Create(10000, 100);
end;

destructor TSidekiqHTTPIngressAdapter.Destroy;
begin
  Stop;
  FQueue.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqHTTPIngressAdapter.New: TSidekiqHTTPIngressAdapter;
begin
  Result := TSidekiqHTTPIngressAdapter.Create;
end;

function TSidekiqHTTPIngressAdapter.Host(
  const AValue: string): TSidekiqHTTPIngressAdapter;
begin
  Result := Self;
  FHost := AValue;
end;

function TSidekiqHTTPIngressAdapter.Port(
  AValue: Word): TSidekiqHTTPIngressAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

function TSidekiqHTTPIngressAdapter.Targets(
  const AValue: TArray<string>): TSidekiqHTTPIngressAdapter;
begin
  Result := Self;
  FTargets := AValue;
end;

function TSidekiqHTTPIngressAdapter.Token(
  const AValue: string): TSidekiqHTTPIngressAdapter;
begin
  Result := Self;
  FToken := AValue;
end;

function TSidekiqHTTPIngressAdapter.UseHTTPServer(
  const AServer: ISidekiqHTTPServer): TSidekiqHTTPIngressAdapter;
begin
  Result := Self;
  FServer := AServer;
end;

procedure TSidekiqHTTPIngressAdapter.Start;
begin
  FLock.Acquire;
  try
    if FRunning then Exit;
    if not Assigned(FServer) then
      raise EArgumentException.Create(
        'No HTTP server configured. Call .UseHTTPServer(TSidekiqIndyHTTPServer.New).');
    FServer.SetOnRequest(HandleRequest);
    FServer.Start(FHost, FPort);
    FRunning := True;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqHTTPIngressAdapter.Stop;
begin
  FLock.Acquire;
  try
    if not FRunning then Exit;
    FServer.Stop;
    FRunning := False;
  finally
    FLock.Release;
  end;
end;

function TSidekiqHTTPIngressAdapter.GetRunning: Boolean;
begin
  FLock.Acquire;
  try
    Result := FRunning;
  finally
    FLock.Release;
  end;
end;

function TSidekiqHTTPIngressAdapter.GetJobCount: Int64;
begin
  Result := TInterlocked.Read(FJobCount);
end;

function TSidekiqHTTPIngressAdapter.IsAuthorized(
  const AAuthHeader: string): Boolean;
begin
  if FToken.IsEmpty then
    Exit(True);
  Result := SameText(AAuthHeader, 'Bearer ' + FToken);
end;

procedure TSidekiqHTTPIngressAdapter.HandleRequest(
  const AContext: TSidekiqHTTPContext;
  out AStatus: Integer;
  out AResponseBody: string);
var
  LPath: string;
begin
  LPath := AContext.Path.ToLower;

  if SameText(AContext.Method, 'GET') and (LPath = '/health') then
  begin
    AStatus       := 200;
    AResponseBody := Format('{"status":"ok","jobs_received":%d}', [TInterlocked.Read(FJobCount)]);
    Exit;
  end;

  if not IsAuthorized(AContext.AuthHeader) then
  begin
    AStatus       := 401;
    AResponseBody := '{"error":"unauthorized"}';
    Exit;
  end;

  if not SameText(AContext.Method, 'POST') then
  begin
    AStatus       := 405;
    AResponseBody := '{"error":"method not allowed"}';
    Exit;
  end;

  try
    if LPath = '/events' then
    begin
      EnqueueEvent(AContext.Body);
      AStatus       := 202;
      AResponseBody := '{"status":"accepted"}';
    end
    else if LPath = '/batch' then
    begin
      EnqueueBatch(AContext.Body);
      AStatus       := 202;
      AResponseBody := '{"status":"accepted"}';
    end
    else
    begin
      AStatus       := 404;
      AResponseBody := '{"error":"not found"}';
    end;
  except
    on E: EHTTPIngressQueueFull do
    begin
      AStatus       := 429;
      AResponseBody := '{"status":"overloaded","message":"queue full, retry later"}';
    end;
    on E: Exception do
    begin
      var LErrJson: TJSONObject;
      LErrJson := TJSONObject.Create;
      try
        LErrJson.AddPair('error', E.Message);
        AResponseBody := LErrJson.ToJSON;
      finally
        LErrJson.Free;
      end;
      AStatus := 400;
    end;
  end;
end;

procedure TSidekiqHTTPIngressAdapter.EnqueueEvent(const ABody: string);
var
  LTarget: string;
begin
  for LTarget in FTargets do
  begin
    if FQueue.PushItem(CreateEnvelope(LTarget, ABody)) <> wrSignaled then
      raise EHTTPIngressQueueFull.Create('queue full');
    TInterlocked.Increment(FJobCount);
  end;
end;

procedure TSidekiqHTTPIngressAdapter.EnqueueBatch(const ABody: string);
var
  LArray: TJSONArray;
  LItem: TJSONValue;
  LTarget: string;
begin
  LArray := TJSONObject.ParseJSONValue(ABody) as TJSONArray;
  if not Assigned(LArray) then
    raise EArgumentException.Create('Body must be a JSON array');
  try
    for LItem in LArray do
      for LTarget in FTargets do
      begin
        if FQueue.PushItem(CreateEnvelope(LTarget, LItem.ToJSON)) <> wrSignaled then
          raise EHTTPIngressQueueFull.Create('queue full');
        TInterlocked.Increment(FJobCount);
      end;
  finally
    LArray.Free;
  end;
end;

function TSidekiqHTTPIngressAdapter.CreateEnvelope(
  const AAction, ABody: string): ISidekiqJobEnvelope;
begin
  Result := THTTPJobEnvelope.Create(
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', ''),
    AAction, ABody);
end;

function TSidekiqHTTPIngressAdapter.Name: string;
begin
  Result := Format('http-ingress:%s:%d', [FHost, FPort]);
end;

function TSidekiqHTTPIngressAdapter.Fetch(
  const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
var
  LJob: ISidekiqJobEnvelope;
  LCount: Integer;
begin
  if not FRunning then
    Start;

  SetLength(Result, AOptions.BatchSize);
  LCount := 0;
  while (LCount < AOptions.BatchSize) and
        (FQueue.PopItem(LJob) = wrSignaled) do
  begin
    Result[LCount] := LJob;
    Inc(LCount);
  end;
  SetLength(Result, LCount);
end;

procedure TSidekiqHTTPIngressAdapter.Ack(const AJob: ISidekiqJobEnvelope);
begin
  // HTTP ingress: ack e no-op
end;

procedure TSidekiqHTTPIngressAdapter.Nack(
  const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
begin
  FQueue.PushItem(AJob);
end;

procedure TSidekiqHTTPIngressAdapter.MoveToDeadLetter(
  const AJob: ISidekiqJobEnvelope; const AReason: string);
begin
  // Em producao: persistir em tabela/arquivo de quarentena
end;

end.
