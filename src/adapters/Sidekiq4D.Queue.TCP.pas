unit Sidekiq4D.Queue.TCP;

// Generic TCP queue adapter — zero third-party dependencies.
// Requires injection of ISidekiqTCPServer (e.g. TSidekiqIndyTCPServer).
//
// Protocolo: JSON delimitado por newline (JSONL)
//   Client envia: {"action":"emissao","body":"{\"nota\":1}"}\n
//   Server responde: {"status":"accepted","id":"..."}\n
//
// Uso:
//   TSidekiqTCPAdapter.New
//     .Host('0.0.0.0')
//     .Port(9100)
//     .MaxConnections(100)
//     .UseTCPServer(TSidekiqIndyTCPServer.New);

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
  // Callback invoked for each received line; returns a response line.
  TSidekiqTCPOnLine = function(const ALine: string): string of object;

  // Port: TCP server that calls OnLine for each received JSONL line.
  ISidekiqTCPServer = interface
    ['{9A8B7C6D-5E4F-3021-A9B8-C7D6E5F43210}']
    procedure SetOnLine(const ACallback: TSidekiqTCPOnLine);
    procedure Start(const AHost: string; APort: Word; AMaxConnections: Integer);
    procedure Stop;
  end;

  TSidekiqTCPAdapter = class(TInterfacedObject, ISidekiqQueueAdapter)
  private
    FLock: TCriticalSection;
    FHost: string;
    FPort: Word;
    FMaxConnections: Integer;
    FServer: ISidekiqTCPServer;
    FQueue: TThreadedQueue<ISidekiqJobEnvelope>;
    // FRunning: protected by FLock in Start/Stop.
    FRunning: Boolean;
    // FJobCount: updated via TInterlocked.Increment; read via TInterlocked.Read.
    FJobCount: Int64;

    function HandleLine(const ALine: string): string;
    function ParseAndEnqueue(const ALine: string): string;
    function CreateEnvelope(
      const AAction, ABody: string;
      const AAttributes: TStrings): ISidekiqJobEnvelope;
    function GetJobCount: Int64;
    function GetRunning: Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqTCPAdapter; static;

    function Host(const AValue: string): TSidekiqTCPAdapter;
    function Port(AValue: Word): TSidekiqTCPAdapter;
    function MaxConnections(AValue: Integer): TSidekiqTCPAdapter;
    function UseTCPServer(const AServer: ISidekiqTCPServer): TSidekiqTCPAdapter;

    procedure Start;
    procedure Stop;

    function Name: string;
    function Fetch(const AOptions: TSidekiqFetchOptions): TArray<ISidekiqJobEnvelope>;
    procedure Ack(const AJob: ISidekiqJobEnvelope);
    procedure Nack(const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
    procedure MoveToDeadLetter(const AJob: ISidekiqJobEnvelope; const AReason: string);

    property JobCount: Int64 read GetJobCount;
    property Running: Boolean read GetRunning;
  end;

implementation

type
  TTCPJobEnvelope = class(TInterfacedObject, ISidekiqJobEnvelope)
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

{ TSidekiqTCPAdapter }

constructor TSidekiqTCPAdapter.Create;
begin
  inherited Create;
  FLock           := TCriticalSection.Create;
  FHost           := '0.0.0.0';
  FPort           := 9100;
  FMaxConnections := 100;
  FRunning        := False;
  FJobCount       := 0;
  FQueue          := TThreadedQueue<ISidekiqJobEnvelope>.Create(50000);
end;

destructor TSidekiqTCPAdapter.Destroy;
begin
  Stop;
  FQueue.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqTCPAdapter.New: TSidekiqTCPAdapter;
begin
  Result := TSidekiqTCPAdapter.Create;
end;

function TSidekiqTCPAdapter.Host(const AValue: string): TSidekiqTCPAdapter;
begin
  Result := Self;
  FHost := AValue;
end;

function TSidekiqTCPAdapter.Port(AValue: Word): TSidekiqTCPAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

function TSidekiqTCPAdapter.MaxConnections(AValue: Integer): TSidekiqTCPAdapter;
begin
  Result := Self;
  FMaxConnections := AValue;
end;

function TSidekiqTCPAdapter.UseTCPServer(
  const AServer: ISidekiqTCPServer): TSidekiqTCPAdapter;
begin
  Result := Self;
  FServer := AServer;
end;

procedure TSidekiqTCPAdapter.Start;
begin
  FLock.Acquire;
  try
    if FRunning then Exit;
    if not Assigned(FServer) then
      raise EArgumentException.Create(
        'No TCP server configured. Call .UseTCPServer(TSidekiqIndyTCPServer.New).');
    FServer.SetOnLine(HandleLine);
    FServer.Start(FHost, FPort, FMaxConnections);
    FRunning := True;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqTCPAdapter.Stop;
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

function TSidekiqTCPAdapter.GetRunning: Boolean;
begin
  FLock.Acquire;
  try
    Result := FRunning;
  finally
    FLock.Release;
  end;
end;

function TSidekiqTCPAdapter.GetJobCount: Int64;
begin
  Result := TInterlocked.Read(FJobCount);
end;

function TSidekiqTCPAdapter.HandleLine(const ALine: string): string;
begin
  Result := ParseAndEnqueue(ALine.Trim);
end;

function TSidekiqTCPAdapter.ParseAndEnqueue(const ALine: string): string;
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

function TSidekiqTCPAdapter.CreateEnvelope(
  const AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqJobEnvelope;
begin
  Result := TTCPJobEnvelope.Create(
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', ''),
    AAction, ABody, AAttributes);
end;

function TSidekiqTCPAdapter.Name: string;
begin
  Result := Format('tcp:%s:%d', [FHost, FPort]);
end;

function TSidekiqTCPAdapter.Fetch(
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

procedure TSidekiqTCPAdapter.Ack(const AJob: ISidekiqJobEnvelope);
begin
  // TCP: ack e no-op
end;

procedure TSidekiqTCPAdapter.Nack(
  const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
begin
  FQueue.PushItem(AJob);
end;

procedure TSidekiqTCPAdapter.MoveToDeadLetter(
  const AJob: ISidekiqJobEnvelope; const AReason: string);
begin
  // Em producao: persistir em arquivo/banco de quarentena
end;

end.
