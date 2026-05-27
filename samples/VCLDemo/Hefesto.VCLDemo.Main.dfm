object FrmMain: TFrmMain
  Left = 0
  Top = 0
  Caption = 'Sidekiq4D - Demo Comparativo (Tradicional vs Sidekiq4D)'
  ClientHeight = 600
  ClientWidth = 1100
  Color = clWhite
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  TextHeight = 15
  object Splitter1: TSplitter
    Left = 548
    Top = 90
    Width = 4
    Height = 510
    Color = clSilver
    ParentColor = False
  end
  object PnlTop: TPanel
    Left = 0
    Top = 0
    Width = 1100
    Height = 90
    Align = alTop
    BevelOuter = bvNone
    Color = 3546921
    ParentBackground = False
    TabOrder = 0
    object LblTitle: TLabel
      Left = 20
      Top = 10
      Width = 400
      Height = 25
      Caption = 'Sidekiq4D - Comparativo de Processamento SQS'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clWhite
      Font.Height = -18
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object PnlControls: TPanel
      Left = 20
      Top = 45
      Width = 1060
      Height = 35
      BevelOuter = bvNone
      Color = 3546921
      ParentBackground = False
      TabOrder = 0
      object LblMessages: TLabel
        Left = 0
        Top = 8
        Width = 62
        Height = 15
        Caption = 'Mensagens:'
        Font.Color = clWhite
        ParentFont = False
      end
      object LblWorkMs: TLabel
        Left = 180
        Top = 8
        Width = 85
        Height = 15
        Caption = 'Work (ms/msg):'
        Font.Color = clWhite
        ParentFont = False
      end
      object LblConcurrency: TLabel
        Left = 370
        Top = 8
        Width = 77
        Height = 15
        Caption = 'Concurrency:'
        Font.Color = clWhite
        ParentFont = False
      end
      object SpinMessages: TEdit
        Left = 70
        Top = 5
        Width = 80
        Height = 24
        Text = '100'
        
        TabOrder = 0
        
      end
      object SpinWorkMs: TEdit
        Left = 272
        Top = 5
        Width = 70
        Height = 24
        Text = '50'
        
        TabOrder = 1
        
      end
      object SpinConcurrency: TEdit
        Left = 455
        Top = 5
        Width = 50
        Height = 24
        Text = '4'
        
        TabOrder = 2
        
      end
      object BtnSeed: TButton
        Left = 560
        Top = 3
        Width = 130
        Height = 28
        Caption = 'Inserir na Fila'
        Font.Style = [fsBold]
        ParentFont = False
        TabOrder = 3
        OnClick = BtnSeedClick
      end
      object BtnStart: TButton
        Left = 700
        Top = 3
        Width = 160
        Height = 28
        Caption = 'Iniciar Comparativo'
        Font.Color = clGreen
        Font.Style = [fsBold]
        ParentFont = False
        TabOrder = 4
        OnClick = BtnStartClick
      end
    end
  end
  object PnlLeft: TPanel
    Left = 0
    Top = 90
    Width = 548
    Height = 510
    Align = alLeft
    BevelOuter = bvNone
    TabOrder = 1
    object LblLeft: TLabel
      Left = 10
      Top = 5
      Width = 300
      Height = 20
      Caption = 'TRADICIONAL (Get=1, Sleep=15s, Sequencial)'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clMaroon
      Font.Height = -14
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object MemoLeft: TMemo
      Left = 5
      Top = 30
      Width = 538
      Height = 440
      Color = 2105376
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clLime
      Font.Height = -11
      Font.Name = 'Consolas'
      Font.Style = []
      ParentFont = False
      ReadOnly = True
      ScrollBars = ssVertical
      TabOrder = 0
    end
    object PnlStatusLeft: TPanel
      Left = 5
      Top = 475
      Width = 538
      Height = 30
      BevelOuter = bvLowered
      Caption = 'Aguardando...'
      Color = clInfoBk
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clMaroon
      Font.Height = -13
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
      TabOrder = 1
    end
  end
  object PnlRight: TPanel
    Left = 552
    Top = 90
    Width = 548
    Height = 510
    Align = alClient
    BevelOuter = bvNone
    TabOrder = 2
    object LblRight: TLabel
      Left = 10
      Top = 5
      Width = 330
      Height = 20
      Caption = 'SIDEKIQ4D (Batch=10, Concurrency=N, Long-poll)'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clNavy
      Font.Height = -14
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object MemoRight: TMemo
      Left = 5
      Top = 30
      Width = 538
      Height = 440
      Color = 2105376
      Font.Charset = DEFAULT_CHARSET
      Font.Color = 16776960
      Font.Height = -11
      Font.Name = 'Consolas'
      Font.Style = []
      ParentFont = False
      ReadOnly = True
      ScrollBars = ssVertical
      TabOrder = 0
    end
    object PnlStatusRight: TPanel
      Left = 5
      Top = 475
      Width = 538
      Height = 30
      BevelOuter = bvLowered
      Caption = 'Aguardando...'
      Color = clInfoBk
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clNavy
      Font.Height = -13
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
      TabOrder = 1
    end
  end
end
