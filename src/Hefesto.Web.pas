unit Hefesto.Web;

{
  HefestoWeb4D — Dashboard web embutido no processo worker.

  Expõe um servidor HTTP leve com SPA de observabilidade completa:
    GET  /                        — página HTML (Bootstrap 5 + Chart.js)
    GET  /api/overview            — visão geral: workers, filas, DLQ, agendados
    GET  /api/queues              — profundidade e status paused de cada fila
    GET  /api/workers             — workers ativos via server_reliability:active:*
    GET  /api/scheduled           — jobs no scheduled store
    GET  /api/periodic            — definições de jobs periódicos
    GET  /api/batches             — estado de batches via state store
    GET  /api/metrics             — snapshots históricos (Chart.js)
    GET  /api/locks               — chaves de lock ativas
    GET  /api/dlq                 — entradas da Dead Letter Queue
    POST /api/queues/:name/pause  — pausa fila
    POST /api/queues/:name/resume — retoma fila
    POST /api/dlq/retry-all       — re-enfileira todos os jobs da DLQ
    DELETE /api/dlq               — apaga toda a DLQ
    POST /api/dlq/:id/retry       — re-enfileira job específico
    DELETE /api/dlq/:id           — remove job específico da DLQ
}

interface

uses
  System.SysUtils,
  Hefesto.Store.Interfaces,
  Hefesto.DeadLetter,
  Hefesto.Queue.Interfaces,
  Hefesto.Telemetry,
  Hefesto.Scheduled,
  Hefesto.Periodic;

type
  {
    Interface mínima que o dashboard precisa do servidor.
    Usa nomes distintos dos métodos fluentes de IHefestoServer para
    evitar conflitos de assinatura na implementação de múltiplas interfaces.
  }
  IHefestoDashboardServer = interface
    ['{C1D2E3F4-A5B6-4C7D-8E9F-0A1B2C3D4E5F}']
    function ActiveWorkerCount: Integer;
    function MaxConcurrencyCount: Integer;
    function IsServerLeader: Boolean;
    function IsServerDraining: Boolean;
    procedure PauseQueueByName(const AQueueName: string);
    procedure ResumeQueueByName(const AQueueName: string);
    function IsQueuePausedByName(const AQueueName: string): Boolean;
    function ListPeriodicJobs: TArray<THefestoPeriodicDefinition>;
  end;

  THefestoWebDashboardConfig = record
    StateStore      : IHefestoStateStore;
    DeadLetterQueue : IHefestoDeadLetterQueue;
    Queues          : TArray<IHefestoQueueAdapter>;
    DashboardServer : IHefestoDashboardServer;
    Metrics         : IHefestoHistoricalMetrics;
    ScheduledStore  : IHefestoScheduledStore;
  end;

  IHefestoWebSocketHub = interface
    ['{A1B2C3D4-E5F6-4A7B-8C9D-0E1F2A3B4C5D}']
    { Envia JSON para todos os clientes WebSocket conectados. }
    procedure BroadcastMetrics(const AJson: string);
    { Número de clientes WebSocket conectados no momento. }
    function ConnectedClients: Integer;
    { Registra um IOHandler recém-promovido a WebSocket. }
    procedure RegisterClient(const AHandler: TObject);
  end;

  IHefestoWebDashboard = interface
    ['{E5F6A7B8-C9D0-4E1F-AF2B-3C4D5E6F7A8B}']
    { Abre a porta HTTP e começa a aceitar conexões. }
    procedure Start(const APort: Word);

    { Fecha o servidor e libera a porta. }
    procedure Stop;

    { Retorna True enquanto o servidor está aceitando conexões. }
    function IsRunning: Boolean;
  end;

implementation

end.
