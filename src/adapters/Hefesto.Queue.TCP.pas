unit Hefesto.Queue.TCP;

// Generic TCP queue adapter — zero third-party dependencies.
// Requires injection of IHefestoTCPServer (e.g. THefestoIndyTCPServer).
//
// Protocolo: JSON delimitado por newline (JSONL)
//   Client envia: {"action":"emissao","body":"{\"nota\":1}"}\n
//   Server responde: {"status":"accepted","id":"..."}\n
//
// Uso:
//   THefestoTCPAdapter.New
//     .Host('0.0.0.0')
//     .Port(9100)
//     .MaxConnections(100)
//     .UseTCPServer(THefestoIndyTCPServer.New);

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.SyncObjs,
  System.Generics.Collections,
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  // Callback invoked for each received line; returns a response line.
  THefestoTCPOnLine = function(const ALine: string): string of object;

  // Port: TCP server that calls OnLine for each received JSONL line.
  IHefestoTCPServer = interface
    ['{9A8B7C6D-5E4F-3021-A9B8-C7D6E5F43210}']
    procedure SetOnLine(const ACallback: THefestoTCPOnLine);
    procedure Start(const AHost: string; APort: Word; AMaxConnections: Integer);
    procedure Stop;
  end;

  THefestoTCPAdapter = class(TInterfacedObject, IHefestoQueueAdapter)
  private
    FLock: TCriticalSection;
    FHost: string;
    FPort: Word;
    FMaxConnections: Integer;
    FServer: IHefestoTCPServer;
    FQueue: TThreadedQueue<IHefestoJobEnvelope>;
    // FRunning: protected by FLock in Start/Stop.
    FRunning: Boolean;
    // FJobCount: updated via TInterlocked.Increment; read via TInterlocked.Read.
    FJobCount: Int64;

    function HandleLine(const ALine: string): string;
    function ParseAndEnqueue(const ALine: string): string;
    function CreateEnvelope(
      const AAction, ABody: string;
      const AAttributes: TStrings): IHefestoJobEnvelope;
    function GetJobCount: Int64;
    function GetRunning: Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoTCPAdapter; static;

    function Host(const AValue: string): THefestoTCPAdapter;
    function Port(AValue: Word): THefestoTCPAdapter;
    function MaxConnections(AValue: Integer): THefestoTCPAdapter;
    function UseTCPServer(const AServer: IHefestoTCPServer): THefestoTCPAdapter;

    procedure Start;
    procedure Stop;

    function Name: string;
    function Fetch(const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
    procedure Ack(const AJob: IHefestoJobEnvelope);
    procedure Nack(const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: IHefestoJobEnvelope; const AReason: string);

    property JobCount: Int64 read GetJobCount;
    property Running: Boolean read GetRunning;
  end;

implementation

type
  TTCPJobEnvelope = class(TInterfacedObject, IHefestoJobEnvelope)
  private
    FId: string;
    FAction: string;
    FBody: string;
    FAttempts: Integer;
    FAttributes: TStrings;
  public
    constructor Create(
      const AId, AAction, ABody: string; AAttributes: TStrings);
    destructor Destroy; override;
    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Action: string;
    function Body: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

{ TTCPJobEnvelope }

constructor TTCPJobEnvelope.Create(
  const AId, AAction, ABody: string; AAttributes: TStrings);
begin
  inherited Create;
  FId       := AId;
  FAction   := AAction;
  FBody     := ABody;
  FAttempts := 0;
  FAttributes := TStringList.Create;
  if Assigned(AAttributes) then
    FAttributes.Assign(AAttributes);
end;

destructor TTCPJobEnvelope.Destroy;
begin
  FAttributes.Free;
  inherited;
end;

function TTCPJobEnvelope.Id: string;          begin Result := FId;      end;
function TTCPJobEnvelope.QueueName: string;   begin Result := 'tcp';    end;
function TTCPJobEnvelope.ReceiptHandle: string;begin Result := FId;     end;
function TTCPJobEnvelope.Action: string;      begin Result := FAction;  end;
function TTCPJobEnvelope.Body: string;        begin Result := FBody;    end;
function TTCPJobEnvelope.Attempts: Integer;   begin Result := FAttempts;end;

function TTCPJobEnvelope.Attribute(const AName: string): string;
begin
  Result := FAttributes.Values[AName];
end;

{ THefestoTCPAdapter }

constructor THefestoTCPAdapter.Create;
begin
  inherited Create;
  FLock           := TCriticalSection.Create;
  FHost           := '0.0.0.0';
  FPort           := 9100;
  FMaxConnections := 100;
  FRunning        := False;
  FJobCount       := 0;
  FQueue          := TThreadedQueue<IHefestoJobEnvelope>.Create(50000);
end;

destructor THefestoTCPAdapter.Destroy;
begin
  Stop;
  FQueue.Free;
  FLock.Free;
  inherited;
end;

class function THefestoTCPAdapter.New: THefestoTCPAdapter;
begin
  Result := THefestoTCPAdapter.Create;
end;

function THefestoTCPAdapter.Host(const AValue: string): THefestoTCPAdapter;
begin
  Result := Self;
  FHost := AValue;
end;

function THefestoTCPAdapter.Port(AValue: Word): THefestoTCPAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

function THefestoTCPAdapter.MaxConnections(AValue: Integer): THefestoTCPAdapter;
begin
  Result := Self;
  FMaxConnections := AValue;
end;

function THefestoTCPAdapter.UseTCPServer(
  const AServer: IHefestoTCPServer): THefestoTCPAdapter;
begin
  Result := Self;
  FServer := AServer;
end;

procedure THefestoTCPAdapter.Start;
begin
  FLock.Acquire;
  try
    if FRunning then Exit;
    if not Assigned(FServer) then
      raise EArgumentException.Create(
        'No TCP server configured. Call .UseTCPServer(THefestoIndyTCPServer.New).');
    FServer.SetOnLine(HandleLine);
    FServer.Start(FHost, FPort, FMaxConnections);
    FRunning := True;
  finally
    FLock.Release;
  end;
end;

procedure THefestoTCPAdapter.Stop;
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

function THefestoTCPAdapter.GetRunning: Boolean;
begin
  FLock.Acquire;
  try
    Result := FRunning;
  finally
    FLock.Release;
  end;
end;

function THefestoTCPAdapter.GetJobCount: Int64;
begin
  Result := TInterlocked.Read(FJobCount);
end;

function THefestoTCPAdapter.HandleLine(const ALine: string): string;
begin
  Result := ParseAndEnqueue(ALine.Trim);
end;

function THefestoTCPAdapter.ParseAndEnqueue(const ALine: string): string;
var
  LJson: TJSONObject;
  LAction, LBody: string;
  LAttrs: TStringList;
  LAttrObj: TJSONObject;
  LPair: TJSONPair;
  LId: string;
begin
  LJson := TJSONObject.ParseJSONValue(ALine) as TJSONObject;
  if not Assigned(LJson) then
    Exit('{"status":"error","message":"invalid JSON"}');
  try
    LAction := LJson.GetValue<string>('action', 'default');
    LBody   := LJson.GetValue<string>('body', ALine);

    LAttrs := TStringList.Create;
    try
      LAttrObj := LJson.GetValue<TJSONObject>('attributes', nil);
      if Assigned(LAttrObj) then
        for LPair in LAttrObj do
          LAttrs.Values[LPair.JsonString.Value] := LPair.JsonValue.Value;

      LId := TGUID.NewGuid.ToString.Replace('{', '').Replace('}', '');
      if FQueue.PushItem(
        TTCPJobEnvelope.Create(LId, LAction, LBody, LAttrs), 100) <> wrSignaled then
        Exit('{"status":"overloaded"}');

      TInterlocked.Increment(FJobCount);
      Result := Format('{"status":"accepted","id":"%s"}', [LId]);
    finally
      LAttrs.Free;
    end;
  finally
    LJson.Free;
  end;
end;

function THefestoTCPAdapter.CreateEnvelope(
  const AAction, ABody: string;
  const AAttributes: TStrings): IHefestoJobEnvelope;
begin
  Result := TTCPJobEnvelope.Create(
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', ''),
    AAction, ABody, AAttributes);
end;

function THefestoTCPAdapter.Name: string;
begin
  Result := Format('tcp:%s:%d', [FHost, FPort]);
end;

function THefestoTCPAdapter.Fetch(
  const AOptions: THefestoFetchOptions): TArray<IHefestoJobEnvelope>;
var
  LJob: IHefestoJobEnvelope;
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

procedure THefestoTCPAdapter.Ack(const AJob: IHefestoJobEnvelope);
begin
  // TCP: ack e no-op
end;

procedure THefestoTCPAdapter.Nack(
  const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
begin
  FQueue.PushItem(AJob);
end;

procedure THefestoTCPAdapter.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope; const AReason: string);
begin
  // Em producao: persistir em arquivo/banco de quarentena
end;

end.
