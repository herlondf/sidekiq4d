unit Sidekiq4D.Options;

interface

type
  { Configuração de uma fila com peso para round-robin ponderado.
    Use com ISidekiqServer.UseQueues para definir filas e pesos em uma
    chamada só:
      Server.UseQueues([
        TSidekiqQueueConfig.Create('critical', 4),
        TSidekiqQueueConfig.Create('default',  2),
        TSidekiqQueueConfig.Create('low',      1)
      ]); }
  TSidekiqQueueConfig = record
    Name  : string;
    Weight: Integer;

    class function Create(
      const AName  : string;
      const AWeight: Integer = 1): TSidekiqQueueConfig; static;
  end;

  TSidekiqServerOptions = record
    MaxCycles: Integer;
    IdleDelayMs: Integer;
    StopWhenIdle: Boolean;
    Concurrency: Integer;
    ClientOutboxBudget: Integer;
    BatchCallbackPromotionBudget: Integer;
    PeriodicPromotionBudget: Integer;
    ScheduledPromotionBudget: Integer;
    LeaderElectionEnabled: Boolean;
    LeaderName: string;
    LeaderLeaseTtlSeconds: Integer;

    class function Default: TSidekiqServerOptions; static;
  end;

implementation

{ TSidekiqQueueConfig }

class function TSidekiqQueueConfig.Create(
  const AName  : string;
  const AWeight: Integer): TSidekiqQueueConfig;
begin
  Result.Name   := AName;
  Result.Weight := AWeight;
end;

{ TSidekiqServerOptions }

class function TSidekiqServerOptions.Default: TSidekiqServerOptions;
begin
  Result.MaxCycles := 0;
  Result.IdleDelayMs := 1000;
  Result.StopWhenIdle := False;
  Result.Concurrency := 1;
  Result.ClientOutboxBudget := 100;
  Result.BatchCallbackPromotionBudget := 100;
  Result.PeriodicPromotionBudget := 100;
  Result.ScheduledPromotionBudget := 100;
  Result.LeaderElectionEnabled := False;
  Result.LeaderName := 'default';
  Result.LeaderLeaseTtlSeconds := 30;
end;

end.
