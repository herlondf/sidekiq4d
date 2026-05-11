unit Sidekiq4D.Queue.TCP.Synapse;

interface

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.SyncObjs,
  System.Generics.Collections,
  blcksock,
  synsock,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces;

type
  // Queue adapter TCP usando Synapse (TTCPBlockSocket).
  // Mais leve que Indy — sem dependencia de VCL, ideal para Linux/console.
  //
  // Protocolo (mesmo do adapter Indy):
  //   Client envia: {"action":"emissao","body":"{\"nota\":1}"}\n
  //   Server responde: {"status":"accepted","id":"..."}\n
  //
  // Uso:
  //   TSidekiqTCPSynapseAdapter.New
  //     .Host('0.0.0.0')
  //     .Port(9100);
  //
  //   TSidekiqServer.New.UseQueue(adapter)...
  //
  // Thread-safety:
  //   - Accept loop em thread dedicada
  //   - Cada conexao spawna TThread propria
  //   - TThreadedQueue e thread-safe (producer-consumer)
  TSidekiqTCPSynapseAdapter = class(TInterfacedObject, ISidekiqQueueAdapter)
  private
    FLock: TCriticalSection;
    FHost: string;
    FPort: Word;
    FListenSocket: TTCPBlockSocket;
    FQueue: TThreadedQueue<ISidekiqJobEnvelope>;
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
      const AAttributes: TStrings): ISidekiqJobEnvelope;
    function GetJobCount: Int64;
    function GetRunning: Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqTCPSynapseAdapter;

    // Configuracao fluente
    function Host(const AValue: string): TSidekiqTCPSynapseAdapter;
    function Port(AValue: Word): TSidekiqTCPSynapseAdapter;

    // Lifecycle
    procedure Start;
    procedure Stop;

    // ISidekiqQueueAdapter
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
  TSynTCPJobEnvelope = class(TInterfacedObject, ISidekiqJobEnvelope)
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

{ TSidekiqTCPSynapseAdapter }

constructor TSidekiqTCPSynapseAdapter.Create;
begin
  inherited Create;
  FLock         := TCriticalSection.Create;
  FHost         := '0.0.0.0';
  FPort         := 9100;
  FRunning      := False;
  FStopFlag     := 0;
  FJobCount     := 0;
  FQueue        := TThreadedQueue<ISidekiqJobEnvelope>.Create(50000);
  FListenSocket := TTCPBlockSocket.Create;
end;

destructor TSidekiqTCPSynapseAdapter.Destroy;
begin
  Stop;
  FListenSocket.Free;
  FQueue.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqTCPSynapseAdapter.New: TSidekiqTCPSynapseAdapter;
begin
  Result := TSidekiqTCPSynapseAdapter.Create;
end;

function TSidekiqTCPSynapseAdapter.Host(const AValue: string): TSidekiqTCPSynapseAdapter;
begin
  Result := Self;
  FHost := AValue;
end;

function TSidekiqTCPSynapseAdapter.Port(AValue: Word): TSidekiqTCPSynapseAdapter;
begin
  Result := Self;
  FPort := AValue;
end;

procedure TSidekiqTCPSynapseAdapter.Start;
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

procedure TSidekiqTCPSynapseAdapter.Stop;
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

function TSidekiqTCPSynapseAdapter.GetRunning: Boolean;
begin
  FLock.Acquire;
  try
    Result := FRunning;
  finally
    FLock.Release;
  end;
end;

function TSidekiqTCPSynapseAdapter.GetJobCount: Int64;
begin
  Result := TInterlocked.Read(FJobCount);
end;

procedure TSidekiqTCPSynapseAdapter.AcceptLoop;
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

procedure TSidekiqTCPSynapseAdapter.HandleClient(ASocket: TSocket);
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

function TSidekiqTCPSynapseAdapter.ParseAndEnqueue(const ALine: string): string;
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

function TSidekiqTCPSynapseAdapter.CreateEnvelope(
  const AAction, ABody: string;
  const AAttributes: TStrings): ISidekiqJobEnvelope;
begin
  Result := TSynTCPJobEnvelope.Create(
    TGUID.NewGuid.ToString.Replace('{', '').Replace('}', ''),
    AAction, ABody, AAttributes);
end;

// ISidekiqQueueAdapter

function TSidekiqTCPSynapseAdapter.Name: string;
begin
  Result := Format('tcp-synapse:%s:%d', [FHost, FPort]);
end;

function TSidekiqTCPSynapseAdapter.Fetch(
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

procedure TSidekiqTCPSynapseAdapter.Ack(const AJob: ISidekiqJobEnvelope);
begin
  // TCP: no-op
end;

procedure TSidekiqTCPSynapseAdapter.Nack(
  const AJob: ISidekiqJobEnvelope; const ADelaySeconds: Integer);
begin
  FQueue.PushItem(AJob);
end;

procedure TSidekiqTCPSynapseAdapter.MoveToDeadLetter(
  const AJob: ISidekiqJobEnvelope; const AReason: string);
begin
  // Em producao: persistir em arquivo/banco
end;

end.
