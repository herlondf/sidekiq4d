unit Hefesto.VCLDemo.Main;

interface

uses
  Winapi.Windows, Winapi.Messages, Winapi.ActiveX,
  System.SysUtils, System.Classes, System.Diagnostics, System.SyncObjs,
  System.Generics.Collections, System.Threading,
  Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, Vcl.ExtCtrls,
  Vcl.ComCtrls,
  Hefesto.Job,
  Hefesto.Context,
  Hefesto.Handler,
  Hefesto.Options,
  Hefesto.Queue.Interfaces,
  Hefesto.Queue.InMemory,
  Hefesto.Retry,
  Hefesto.Telemetry,
  Hefesto.Dispatcher,
  Hefesto.Server;

type
  TFrmMain = class(TForm)
    PnlTop: TPanel;
    PnlLeft: TPanel;
    PnlRight: TPanel;
    Splitter1: TSplitter;
    LblTitle: TLabel;
    LblLeft: TLabel;
    LblRight: TLabel;
    MemoLeft: TMemo;
    MemoRight: TMemo;
    PnlControls: TPanel;
    LblMessages: TLabel;
    SpinMessages: TEdit;
    BtnStart: TButton;
    BtnSeed: TButton;
    LblWorkMs: TLabel;
    SpinWorkMs: TEdit;
    PnlStatusLeft: TPanel;
    PnlStatusRight: TPanel;
    LblConcurrency: TLabel;
    SpinConcurrency: TEdit;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure BtnSeedClick(Sender: TObject);
    procedure BtnStartClick(Sender: TObject);
  private
    FQueueTraditional: THefestoInMemoryQueueAdapter;
    FQueueHefesto: THefestoInMemoryQueueAdapter;
    FRunning: Boolean;
    FTraditionalCount: Integer;
    FHefestoCount: Integer;
    FTraditionalWatch: TStopwatch;
    FHefestoWatch: TStopwatch;
    FTotalMessages: Integer;

    procedure LogLeft(const AMsg: string);
    procedure LogRight(const AMsg: string);
    procedure UpdateStatusLeft;
    procedure UpdateStatusRight;
    procedure RunTraditional;
    procedure RunHefesto;
    procedure SeedQueues;
  end;

var
  FrmMain: TFrmMain;

implementation

{$R *.dfm}

type
  TDemoHandler = class(TInterfacedObject, IHefestoJobHandler)
  private
    FWorkMs: Integer;
    FCounter: PInteger;
    FOnProcessed: TProc<Integer, string>;
  public
    constructor Create(AWorkMs: Integer; ACounter: PInteger;
      AOnProcessed: TProc<Integer, string>);
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

constructor TDemoHandler.Create(AWorkMs: Integer; ACounter: PInteger;
  AOnProcessed: TProc<Integer, string>);
begin
  inherited Create;
  FWorkMs := AWorkMs;
  FCounter := ACounter;
  FOnProcessed := AOnProcessed;
end;

function TDemoHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := True;
end;

procedure TDemoHandler.Perform(const AContext: IHefestoJobContext);
var
  LCount: Integer;
begin
  Sleep(FWorkMs);
  LCount := TInterlocked.Increment(FCounter^);
  if Assigned(FOnProcessed) then
    FOnProcessed(LCount, AContext.Job.Body);
end;

{ TFrmMain }

procedure TFrmMain.FormCreate(Sender: TObject);
begin
  FRunning := False;
  FQueueTraditional := nil;
  FQueueHefesto := nil;
  SpinMessages.Text := '100';
  SpinWorkMs.Text := '50';
  SpinConcurrency.Text := '4';
  MemoLeft.Clear;
  MemoRight.Clear;
end;

procedure TFrmMain.FormDestroy(Sender: TObject);
begin
  FRunning := False;
end;

procedure TFrmMain.LogLeft(const AMsg: string);
begin
  System.Classes.TThread.Queue(nil,
    procedure
    begin
      MemoLeft.Lines.Add(AMsg);
      SendMessage(MemoLeft.Handle, WM_VSCROLL, SB_BOTTOM, 0);
    end);
end;

procedure TFrmMain.LogRight(const AMsg: string);
begin
  System.Classes.TThread.Queue(nil,
    procedure
    begin
      MemoRight.Lines.Add(AMsg);
      SendMessage(MemoRight.Handle, WM_VSCROLL, SB_BOTTOM, 0);
    end);
end;

procedure TFrmMain.UpdateStatusLeft;
var
  LCount: Integer;
  LElapsed: Double;
begin
  LCount := FTraditionalCount;
  LElapsed := FTraditionalWatch.Elapsed.TotalSeconds;
  System.Classes.TThread.Queue(nil,
    procedure
    begin
      if LElapsed > 0 then
        PnlStatusLeft.Caption := Format('%d/%d | %.1f msgs/s | %.1fs',
          [LCount, FTotalMessages, LCount / LElapsed, LElapsed])
      else
        PnlStatusLeft.Caption := Format('%d/%d', [LCount, FTotalMessages]);
    end);
end;

procedure TFrmMain.UpdateStatusRight;
var
  LCount: Integer;
  LElapsed: Double;
begin
  LCount := FHefestoCount;
  LElapsed := FHefestoWatch.Elapsed.TotalSeconds;
  System.Classes.TThread.Queue(nil,
    procedure
    begin
      if LElapsed > 0 then
        PnlStatusRight.Caption := Format('%d/%d | %.1f msgs/s | %.1fs',
          [LCount, FTotalMessages, LCount / LElapsed, LElapsed])
      else
        PnlStatusRight.Caption := Format('%d/%d', [LCount, FTotalMessages]);
    end);
end;

procedure TFrmMain.SeedQueues;
var
  I: Integer;
  LBody: string;
begin
  FTotalMessages := StrToIntDef(SpinMessages.Text, 100);
  FQueueTraditional := THefestoInMemoryQueueAdapter.New;
  FQueueHefesto := THefestoInMemoryQueueAdapter.New;

  for I := 1 to FTotalMessages do
  begin
    LBody := Format('{"nfce_id":%d,"empresa":"ACME","serie":1,"numero":%d}', [I, 1000 + I]);
    FQueueTraditional.Enqueue('process_nfce', LBody);
    FQueueHefesto.Enqueue('process_nfce', LBody);
  end;
end;

procedure TFrmMain.BtnSeedClick(Sender: TObject);
begin
  MemoLeft.Clear;
  MemoRight.Clear;
  PnlStatusLeft.Caption := 'Aguardando...';
  PnlStatusRight.Caption := 'Aguardando...';
  SeedQueues;
  LogLeft(Format('[SEED] %d mensagens na fila (modo tradicional)', [FTotalMessages]));
  LogRight(Format('[SEED] %d mensagens na fila (Hefesto)', [FTotalMessages]));
end;

procedure TFrmMain.BtnStartClick(Sender: TObject);
begin
  if FRunning then Exit;
  if not Assigned(FQueueTraditional) then
  begin
    ShowMessage('Clique em "Inserir na Fila" primeiro.');
    Exit;
  end;

  FRunning := True;
  BtnStart.Enabled := False;
  BtnSeed.Enabled := False;
  FTraditionalCount := 0;
  FHefestoCount := 0;

  // Roda ambos em paralelo
  TTask.Run(RunTraditional);
  TTask.Run(RunHefesto);
end;

procedure TFrmMain.RunTraditional;
var
  LWorkMs: Integer;
  LOptions: THefestoFetchOptions;
  LJobs: TArray<IHefestoJobEnvelope>;
  LJob: IHefestoJobEnvelope;
  LEmptyCycles: Integer;
begin
  CoInitialize(nil);
  try
    LWorkMs := StrToIntDef(SpinWorkMs.Text, 50);
    LEmptyCycles := 0;
    FTraditionalWatch := TStopwatch.StartNew;

    LogLeft('[START] Modo tradicional: Get(1) + Sleep(15s)');
    LogLeft(Format('[CONFIG] 1 msg/fetch, sequencial, work=%dms', [LWorkMs]));
    LogLeft('');

    LOptions.BatchSize := 1;
    LOptions.WaitTimeSeconds := 0;
    LOptions.VisibilityTimeout := 30;

    while LEmptyCycles < 2 do
    begin
      if not FRunning then Break;

      LJobs := FQueueTraditional.Fetch(LOptions);

      if Length(LJobs) = 0 then
      begin
        Inc(LEmptyCycles);
        if LEmptyCycles < 2 then
        begin
          LogLeft('[IDLE] Fila vazia - Sleep(15s)...');
          Sleep(15000);
        end;
        Continue;
      end;

      LEmptyCycles := 0;
      for LJob in LJobs do
      begin
        Sleep(LWorkMs);
        FQueueTraditional.Ack(LJob);
        TInterlocked.Increment(FTraditionalCount);

        if (FTraditionalCount mod 10) = 0 then
        begin
          LogLeft(Format('[PROC] %d/%d processadas', [FTraditionalCount, FTotalMessages]));
          UpdateStatusLeft;
        end;
      end;
    end;

    FTraditionalWatch.Stop;
    LogLeft('');
    LogLeft(Format('[DONE] %d msgs em %.1fs (%.1f msgs/s)',
      [FTraditionalCount, FTraditionalWatch.Elapsed.TotalSeconds,
       FTraditionalCount / FTraditionalWatch.Elapsed.TotalSeconds]));
    UpdateStatusLeft;
  finally
    CoUninitialize;
  end;
end;

procedure TFrmMain.RunHefesto;
var
  LWorkMs: Integer;
  LConcurrency: Integer;
  LServer: IHefestoServer;
begin
  CoInitialize(nil);
  try
    LWorkMs := StrToIntDef(SpinWorkMs.Text, 50);
    LConcurrency := StrToIntDef(SpinConcurrency.Text, 4);
    FHefestoWatch := TStopwatch.StartNew;

    LogRight('[START] Hefesto');
    LogRight(Format('[CONFIG] BatchSize=10, Concurrency=%d, work=%dms',
      [LConcurrency, LWorkMs]));
    LogRight('');

    LServer := THefestoServer.New
      .UseQueue(FQueueHefesto)
      .Concurrency(LConcurrency)
      .BatchSize(10)
      .IdleDelayMs(100)
      .StopWhenIdle
      .RegisterHandler('process_nfce',
        TDemoHandler.Create(LWorkMs, @FHefestoCount,
          procedure(ACount: Integer; ABody: string)
          begin
            if (ACount mod 10) = 0 then
            begin
              LogRight(Format('[PROC] %d/%d processadas', [ACount, FTotalMessages]));
              UpdateStatusRight;
            end;
          end));

    LServer.Run;

    FHefestoWatch.Stop;
    LogRight('');
    LogRight(Format('[DONE] %d msgs em %.1fs (%.1f msgs/s)',
      [FHefestoCount, FHefestoWatch.Elapsed.TotalSeconds,
       FHefestoCount / FHefestoWatch.Elapsed.TotalSeconds]));
    UpdateStatusRight;

    System.Classes.TThread.Queue(nil,
      procedure
      begin
        BtnStart.Enabled := True;
        BtnSeed.Enabled := True;
        FRunning := False;
      end);
  finally
    CoUninitialize;
  end;
end;

end.
