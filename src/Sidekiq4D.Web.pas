unit Sidekiq4D.Web;

{
  SidekiqWeb4D — Dashboard web embutido no processo worker.

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
  Sidekiq4D.Store.Interfaces,
  Sidekiq4D.DeadLetter,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Telemetry,
  Sidekiq4D.Scheduled,
  Sidekiq4D.Periodic;

type
  {
    Interface mínima que o dashboard precisa do servidor.
    Usa nomes distintos dos métodos fluentes de ISidekiqServer para
    evitar conflitos de assinatura na implementação de múltiplas interfaces.
  }
  ISidekiqDashboardServer = interface
    ['{C1D2E3F4-A5B6-4C7D-8E9F-0A1B2C3D4E5F}']
    function ActiveWorkerCount: Integer;
    function MaxConcurrencyCount: Integer;
    function IsServerLeader: Boolean;
    function IsServerDraining: Boolean;
    procedure PauseQueueByName(const AQueueName: string);
    procedure ResumeQueueByName(const AQueueName: string);
    function IsQueuePausedByName(const AQueueName: string): Boolean;
    function ListPeriodicJobs: TArray<TSidekiqPeriodicDefinition>;
  end;

  TSidekiqWebDashboardConfig = record
    StateStore      : ISidekiqStateStore;
    DeadLetterQueue : ISidekiqDeadLetterQueue;
    Queues          : TArray<ISidekiqQueueAdapter>;
    DashboardServer : ISidekiqDashboardServer;
    Metrics         : ISidekiqHistoricalMetrics;
    ScheduledStore  : ISidekiqScheduledStore;
  end;

  ISidekiqWebDashboard = interface
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
