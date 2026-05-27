unit Hefesto.Queue.TCP.Synapse;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.SyncObjs,
  System.Generics.Collections,
  blcksock,
  synsock,
  Hefesto.Job,
  Hefesto.Queue.Interfaces;

type
  // Queue adapter TCP usando Synapse (TTCPBlockSocket).
  // Mais leve que Indy — sem dependencia de VCL, ideal para Linux/console.
  //
  // Protocolo (mesmo do adapter Indy):
  //   Client envia: {"action":"emissao","body":"{\"nota\":1}"}\n
  //   Server responde: {"status":"accepted","id":"..."}\n
  //
  // Uso:
  //   THefestoTCPSynapseAdapter.New
  //     .Host('0.0.0.0')
  //     .Port(9100);
  //
  //   THefestoServer.New.UseQueue(adapter)...
  //
  // Thread-safety:
  //   - Accept loop em thread dedicada
  //   - Cada conexao spawna TThread propria
  //   - TThreadedQueue e thread-safe (producer-consumer)
  THefestoTCPSynapseAdapter = class(TInterfacedObject, IHefestoQueueAdapter)
  private
    FLock: TCriticalSection;
    FHost: string;
    FPort: Word;
    FListenSocket: TTCPBlockSocket;
    FQueue: TThreadedQueue<IHefestoJobEnvelope>;
    FAcceptThread: TThread;
    // FRunning: protected by FLock in Start/Stop.
    FRunning: Boolean;
    FStopFlag: Integer;
    // FJobCount: updated via TInterlocked.Increment; read via TInterlocked.Read.
    FJobCount: Int64;

    procedure AcceptLoop;
    procedure HandleClient(ASocket: TSocket);
    function ParseAndEnqueue(const ALine: string): string;
    function CreateEnvelope(const AAction, ABody: string;
      const AAttributes: TStrings): IHefestoJobEnvelope;
    function GetJobCount: Int64;
    function GetRunning: Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoTCPSynapseAdapter;

    // Configuracao fluente
    function Host(const AValue: string): THefestoTCPSynapseAdapter;
    function Port(AValue: Word): THefestoTCPSynapseAdapter;

    // Lifecycle
    procedure Start;
    procedure Stop;

    // IHefestoQueueAdapter
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
  TSynTCPJobEnvelope = class(TInterfacedObject, IHefestoJobEnvelope)
  private
    FId: string;
    FAction: string;
    FBody: string;
    FAttempts: Integer;
    FAttributes: TStrings;
  public
    constructor Create(const AId, AAction, ABody: string; AAttributes: TStrings);
    destructor Destroy; override;
    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Action: string;
    function Body: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

{ TSynTCPJobEnvelope }

constructor TSynTCPJobEnvelope.Create(const AId, AAction, ABody: string; AAttributes: TStrings);
begin
  inherited Create;
  FId := AId;
  FAction := AAction;
  FBody := ABody;
  FAttempts := 0;
  FAttributes := TStringList.Create;
  if Assigned(AAttributes) then
    FAttributes.Assign(AAttributes);
end;

destructor TSynTCPJobEnvelope.Destroy;
begin
  FAttributes.Free;
  inherited;
end;

function TSynTCPJobEnvelope.Id: string;
begin Result := FId; end;

function TSynTCPJobEnvelope.QueueName: string;
begin Result := 'tcp-synapse'; end;

function TSynTCPJobEnvelope.ReceiptHandle: string;
begin Result := FId; end;

function TSynTCPJobEnvelope.Action: string;
begin Result := FAction; end;

function TSynTCPJobEnvelope.Body: string;
begin Result := FBody; end;

function TSynTCPJobEnvelope.Attribute(const AName: string): string;
begin Result := FAttributes.Values[AName]; end;

function TSynTCPJobEnvelope.Attempts: Integer;
begin Result := FAttempts; end;

{ THefestoTCPSynapseAdapter }

constructor THefestoTCPSynapseAdapter.Create;
begin
  inherited Create;
  FLock         := TCriticalSection.Create;
  FHost         := '0.0.0.0';
  FPort         := 9100;
  FRunning      := False;
  FStopFlag     := 0;
  FJobCount     := 0;
  FQueue        := TThreadedQueue<IHefestoJobEnvelope>.Create(50000);
  FListenSocket := TTCPBlockSocket.Create;
end;

destructor THefestoTCPSynapseAdapter.Destroy;
begin
  Stop;
  FListenSocket.Free;
  FQueue.Free;
  FLock.Free;
  inherited;
end;

class function THefestoTCPSynapseAdapter.New: THefestoTCPSynapseAdapter;
begin
  Result := THefestoTCPSynapseAdapter.Create;
end;

function THefestoTCPSynapseAdapter.Host(const AValue: string): THefestoTCPSynapseAdapter;
begin
  Result := Self;
  FHost := AValue;
end;

function THefestoTCPSynapseAdapter.Port(AValue: Word): THefestoTCPSynapseAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

procedure THefestoTCPSynapseAdapter.Start;
begin
  FLock.Acquire;
  try
    if FRunning then Exit;
    TInterlocked.Exchange(FStopFlag, 0);

    FListenSocket.Bind(FHost, IntToStr(FPort));
    if FListenSocket.LastError <> 0 then
      raise Exception.CreateFmt('TCP bind failed on %s:%d: %s',
        [FHost, FPort, FListenSocket.LastErrorDesc]);

    FListenSocket.Listen;
    if FListenSocket.LastError <> 0 then
      raise Exception.CreateFmt('TCP listen failed: %s',
        [FListenSocket.LastErrorDesc]);

    FRunning := True;

    FAcceptThread := TThread.CreateAnonymousThread(AcceptLoop);
    FAcceptThread.FreeOnTerminate := False;
    FAcceptThread.Start;
  finally
    FLock.Release;
  end;
end;

procedure THefestoTCPSynapseAdapter.Stop;
begin
  FLock.Acquire;
  try
    if not FRunning then Exit;
    TInterlocked.Exchange(FStopFlag, 1);
    FListenSocket.CloseSocket;
    if Assigned(FAcceptThread) then
    begin
      FAcceptThread.WaitFor;
      FAcceptThread.Free;
      FAcceptThread := nil;
    end;
    FRunning := False;
  finally
    FLock.Release;
  end;
end;

function THefestoTCPSynapseAdapter.GetRunning: Boolean;
begin
  FLock.Acquire;
  try
    Result := FRunning;
  finally
    FLock.Release;
  end;
end;

function THefestoTCPSynapseAdapter.GetJobCount: Int64;
begin
  Result := TInterlocked.Read(FJobCount);
end;

procedure THefestoTCPSynapseAdapter.AcceptLoop;
var
  LClientSocket: TSocket;
begin
  while TInterlocked.CompareExchange(FStopFlag, 0, 0) = 0 do
  begin
    if FListenSocket.CanRead(500) then
    begin
      LClientSocket := FListenSocket.Accept;
      if LClientSocket <> INVALID_SOCKET then
      begin
        // Spawna thread para o client
        TThread.CreateAnonymousThread(
          procedure
          begin
            HandleClient(LClientSocket);
          end).Start;
      end;
    end;
  end;
end;

procedure THefestoTCPSynapseAdapter.HandleClient(ASocket: TSocket);
var
  LClient: TTCPBlockSocket;
  LLine: string;
  LResponse: string;
begin
  LClient := TTCPBlockSocket.Create;
  try
    LClient.Socket := ASocket;
    LClient.GetSins;

    while TInterlocked.CompareExchange(FStopFlag, 0, 0) = 0 do
    begin
      LLine := string(LClient.RecvString(5000));
      if LClient.LastError <> 0 then
        Break;
      if LLine.IsEmpty then
        Continue;

      LResponse := ParseAndEnqueue(LLine.Trim);
      LClient.SendString(AnsiString(LResponse + #13#10));
      if LClient.LastError <> 0 then
        Break;
    end;

    LClient.CloseSocket;
  finally
    LClient.Free;
  end;
end;

function THefestoTCPSynapseAdapter.ParseAndEnqueue(const ALine: string): string;
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
    LBody := LJson.GetValue<string>('body', ALine);

    LAttrs := TStringList.Create;
    try
      LAttrObj := LJson.GetValue<TJSONObject>('attributes', nil);
      if Assigned(LAttrObj) then
        for LPair in LAttrObj do
          LAttrs.Values[LPair.JsonString.Value] := LPair.JsonValue.Value;

      LId := TGUID.NewGuid.ToString.Replace('{', '').Replace('}', '');
      FQueue.PushItem(CreateEnvelope(LAction, LBody, LAttrs));
      TInterlocked.Increment(FJobCount);

      Result := Format('{"status":"accepted","id":"%s"}', [LId]);
    finally
      LAttrs.Free;
    end;
  finally
    LJson.Free;
  end;
end;

function THefestoTCPSynapseAdapter.CreateEnvelope(
  const AAction, ABody: string;
  const AAttributes: TStrings): IHefestoJobEnvelope;
begin
  Result := TSynTCPJobEnvelope.Create(
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', ''),
    AAction, ABody, AAttributes);
end;

// IHefestoQueueAdapter

function THefestoTCPSynapseAdapter.Name: string;
begin
  Result := Format('tcp-synapse:%s:%d', [FHost, FPort]);
end;

function THefestoTCPSynapseAdapter.Fetch(
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

procedure THefestoTCPSynapseAdapter.Ack(const AJob: IHefestoJobEnvelope);
begin
  // TCP: no-op
end;

procedure THefestoTCPSynapseAdapter.Nack(
  const AJob: IHefestoJobEnvelope; const ADelaySeconds: Integer);
begin
  FQueue.PushItem(AJob);
end;

procedure THefestoTCPSynapseAdapter.MoveToDeadLetter(
  const AJob: IHefestoJobEnvelope; const AReason: string);
begin
  // Em producao: persistir em arquivo/banco
end;

end.
