unit Sidekiq4D.Outbox.FireDAC;

{
  Transactional Outbox backed by FireDAC (Postgres, SQLite, MySQL, etc.).

  ATENÇÃO — Threading model:
    • TSidekiqFireDACOutbox.Save() usa a CONEXÃO DO CHAMADOR e participa
      da transação ativa nela. Não abre nem faz commit de transação.
    • TSidekiqFireDACOutboxPoller usa uma CONEXÃO EXCLUSIVA do thread de
      polling (FPollConnection). Não compartilhe a conexão de polling
      com outros threads.

  DDL criada automaticamente na primeira chamada (CREATE TABLE IF NOT EXISTS):

    CREATE TABLE IF NOT EXISTS sidekiq_outbox (
      id           TEXT PRIMARY KEY,
      queue_name   TEXT NOT NULL,
      action       TEXT NOT NULL,
      payload      TEXT NOT NULL,
      created_at   TEXT NOT NULL,
      published_at TEXT,
      status       TEXT NOT NULL DEFAULT 'pending'
    )
}

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.DateUtils,
  FireDAC.Comp.Client,
  FireDAC.Stan.Param,
  Sidekiq4D.Outbox,
  Sidekiq4D.Queue.Interfaces;

type
  TSidekiqFireDACOutbox = class(TInterfacedObject, ISidekiqOutbox)
  private
    FTableName   : string;   // Configuration field: set once at construction.
    FTableEnsured: Integer;  // 0 = not yet created; 1 = created. Atomic.
    FLock        : TCriticalSection;

    procedure EnsureTable(const AConn: TFDConnection);
  public
    constructor Create(const ATableName: string = 'sidekiq_outbox');
    destructor Destroy; override;

    class function New(
      const ATableName: string = 'sidekiq_outbox'): ISidekiqOutbox;

    { ISidekiqOutbox }
    procedure Save(
      const AQueueName : string;
      const AAction    : string;
      const APayload   : string;
      const AConnection: TObject);
  end;

  TSidekiqFireDACOutboxPoller = class(TInterfacedObject, ISidekiqOutboxPoller)
  private
    FPollConnection : TFDConnection; // Conexão exclusiva do thread de polling
    FTableName      : string;        // Configuration field: set once.
    FPublisher      : ISidekiqQueuePublisher;
    FPollIntervalMs : Integer;
    FBatchSize      : Integer;
    FThread         : TThread;
    FStopEvent      : TEvent;
    FLock           : TCriticalSection;
    FRunning        : Boolean;

    procedure RunLoop;
    function  FetchAndPublishBatch(const AConn: TFDConnection): Integer;
    procedure MarkPublished(
      const AConn: TFDConnection;
      const AIds : TArray<string>);
  public
    constructor Create(
      const APollConnection: TFDConnection;
      const APublisher     : ISidekiqQueuePublisher;
      const ATableName     : string  = 'sidekiq_outbox';
      const APollIntervalMs: Integer = 1000;
      const ABatchSize     : Integer = 100);
    destructor Destroy; override;

    class function New(
      const APollConnection: TFDConnection;
      const APublisher     : ISidekiqQueuePublisher;
      const ATableName     : string  = 'sidekiq_outbox';
      const APollIntervalMs: Integer = 1000;
      const ABatchSize     : Integer = 100): ISidekiqOutboxPoller;

    { ISidekiqOutboxPoller }
    procedure Start;
    procedure Stop;
    function  PollOnce: Integer;
  end;

implementation

uses
  FireDAC.Stan.Option,
  FireDAC.DApt;

const
  DDL_CREATE_TABLE =
    'CREATE TABLE IF NOT EXISTS %s (' +
    '  id           TEXT PRIMARY KEY,'  +
    '  queue_name   TEXT NOT NULL,'     +
    '  action       TEXT NOT NULL,'     +
    '  payload      TEXT NOT NULL,'     +
    '  created_at   TEXT NOT NULL,'     +
    '  published_at TEXT,'              +
    '  status       TEXT NOT NULL DEFAULT ''pending''' +
    ')';

  SQL_INSERT =
    'INSERT INTO %s (id, queue_name, action, payload, created_at, status) ' +
    'VALUES (:id, :queue_name, :action, :payload, :created_at, ''pending'')';

  SQL_SELECT_PENDING =
    'SELECT id, queue_name, action, payload ' +
    'FROM %s WHERE status = ''pending'' ' +
    'ORDER BY created_at LIMIT %d';

  SQL_MARK_PUBLISHED =
    'UPDATE %s SET status = ''published'', published_at = :published_at ' +
    'WHERE id = :id';

{ TSidekiqFireDACOutbox }

constructor TSidekiqFireDACOutbox.Create(const ATableName: string);
begin
  inherited Create;
  FTableName    := ATableName;
  FTableEnsured := 0;
  FLock         := TCriticalSection.Create;
end;

destructor TSidekiqFireDACOutbox.Destroy;
begin
  FLock.Free;
  inherited;
end;

class function TSidekiqFireDACOutbox.New(
  const ATableName: string): ISidekiqOutbox;
begin
  Result := TSidekiqFireDACOutbox.Create(ATableName);
end;

procedure TSidekiqFireDACOutbox.EnsureTable(const AConn: TFDConnection);
begin
  // Double-checked pattern: fast path first (atomic read).
  if TInterlocked.CompareExchange(FTableEnsured, 0, 0) <> 0 then
    Exit;

  FLock.Acquire;
  try
    if TInterlocked.CompareExchange(FTableEnsured, 0, 0) <> 0 then
      Exit;
    AConn.ExecSQL(Format(DDL_CREATE_TABLE, [FTableName]));
    TInterlocked.Exchange(FTableEnsured, 1);
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqFireDACOutbox.Save(
  const AQueueName : string;
  const AAction    : string;
  const APayload   : string;
  const AConnection: TObject);
var
  LConn: TFDConnection;
  LId  : string;
begin
  if not (AConnection is TFDConnection) then
    raise EInvalidCast.Create(
      'TSidekiqFireDACOutbox.Save: AConnection must be a TFDConnection');

  LConn := TFDConnection(AConnection);
  EnsureTable(LConn);

  LId := TGUID.NewGuid.ToString(True).Replace('{','').Replace('}','').ToLower;

  LConn.ExecSQL(
    Format(SQL_INSERT, [FTableName]),
    [LId, AQueueName, AAction, APayload,
     FormatDateTime('yyyy-mm-dd"T"hh:nn:ss', Now)]);
end;

{ TSidekiqFireDACOutboxPoller }

constructor TSidekiqFireDACOutboxPoller.Create(
  const APollConnection: TFDConnection;
  const APublisher     : ISidekiqQueuePublisher;
  const ATableName     : string;
  const APollIntervalMs: Integer;
  const ABatchSize     : Integer);
begin
  inherited Create;
  FPollConnection := APollConnection;
  FPublisher      := APublisher;
  FTableName      := ATableName;
  FPollIntervalMs := APollIntervalMs;
  FBatchSize      := ABatchSize;
  FStopEvent      := TEvent.Create(nil, True, False, '');
  FLock           := TCriticalSection.Create;
  FRunning        := False;
end;

destructor TSidekiqFireDACOutboxPoller.Destroy;
begin
  Stop;
  FStopEvent.Free;
  FLock.Free;
  inherited;
end;

class function TSidekiqFireDACOutboxPoller.New(
  const APollConnection: TFDConnection;
  const APublisher     : ISidekiqQueuePublisher;
  const ATableName     : string;
  const APollIntervalMs: Integer;
  const ABatchSize     : Integer): ISidekiqOutboxPoller;
begin
  Result := TSidekiqFireDACOutboxPoller.Create(
    APollConnection, APublisher, ATableName, APollIntervalMs, ABatchSize);
end;

procedure TSidekiqFireDACOutboxPoller.Start;
var
  LThis: TSidekiqFireDACOutboxPoller;
begin
  FLock.Acquire;
  try
    if FRunning then
      Exit;
    FRunning := True;
    FStopEvent.ResetEvent;
    LThis := Self;
    FThread := TThread.CreateAnonymousThread(
      procedure
      begin
        LThis.RunLoop;
      end);
    FThread.FreeOnTerminate := False;
    FThread.Start;
  finally
    FLock.Release;
  end;
end;

procedure TSidekiqFireDACOutboxPoller.Stop;
var
  LThread: TThread;
begin
  FLock.Acquire;
  try
    if not FRunning then
      Exit;
    FRunning := False;
    FStopEvent.SetEvent;
    LThread  := FThread;
    FThread  := nil;
  finally
    FLock.Release;
  end;

  if Assigned(LThread) then
  begin
    LThread.WaitFor;
    LThread.Free;
  end;
end;

procedure TSidekiqFireDACOutboxPoller.RunLoop;
begin
  // Abre a conexão exclusivamente neste thread.
  if not FPollConnection.Connected then
    FPollConnection.Open;
  try
    while FStopEvent.WaitFor(FPollIntervalMs) = wrTimeout do
    begin
      try
        FetchAndPublishBatch(FPollConnection);
      except
        { Ignora erros transientes — próximo ciclo tenta novamente. }
      end;
    end;
  finally
    FPollConnection.Close;
  end;
end;

function TSidekiqFireDACOutboxPoller.FetchAndPublishBatch(
  const AConn: TFDConnection): Integer;
var
  LQuery  : TFDQuery;
  LIds    : TArray<string>;
  LRequest: TSidekiqPublishRequest;
  LCount  : Integer;
begin
  Result := 0;
  LQuery := TFDQuery.Create(nil);
  try
    LQuery.Connection := AConn;
    LQuery.SQL.Text   := Format(SQL_SELECT_PENDING, [FTableName, FBatchSize]);
    LQuery.Open;
    try
      SetLength(LIds, 0);
      LCount := 0;
      while not LQuery.Eof do
      begin
        LRequest.QueueName    := LQuery.FieldByName('queue_name').AsString;
        LRequest.Action       := LQuery.FieldByName('action').AsString;
        LRequest.Body         := LQuery.FieldByName('payload').AsString;
        LRequest.DelaySeconds := 0;
        SetLength(LRequest.Attributes, 0);

        try
          FPublisher.Publish(LRequest);
          SetLength(LIds, LCount + 1);
          LIds[LCount] := LQuery.FieldByName('id').AsString;
          Inc(LCount);
          Inc(Result);
        except
          { Job individual falhou — não inclui no batch de marcação. }
        end;

        LQuery.Next;
      end;
    finally
      LQuery.Close;
    end;

    if LCount > 0 then
      MarkPublished(AConn, LIds);
  finally
    LQuery.Free;
  end;
end;

procedure TSidekiqFireDACOutboxPoller.MarkPublished(
  const AConn: TFDConnection;
  const AIds : TArray<string>);
var
  LNow : string;
  LId  : string;
begin
  if Length(AIds) = 0 then
    Exit;

  LNow := FormatDateTime('yyyy-mm-dd"T"hh:nn:ss', Now);
  for LId in AIds do
    AConn.ExecSQL(
      Format(SQL_MARK_PUBLISHED, [FTableName]),
      [LNow, LId]);
end;

function TSidekiqFireDACOutboxPoller.PollOnce: Integer;
begin
  if not FPollConnection.Connected then
    FPollConnection.Open;
  Result := FetchAndPublishBatch(FPollConnection);
end;

end.
