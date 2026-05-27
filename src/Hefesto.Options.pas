unit Hefesto.Options;

interface

type
  { Configuração de uma fila com peso para round-robin ponderado.
    Use com IHefestoServer.UseQueues para definir filas e pesos em uma
    chamada só:
      Server.UseQueues([
        THefestoQueueConfig.Create('critical', 4),
        THefestoQueueConfig.Create('default',  2),
        THefestoQueueConfig.Create('low',      1)
      ]); }
  THefestoQueueConfig = record
    Name  : string;
    Weight: Integer;

    class function Create(
      const AName  : string;
      const AWeight: Integer = 1): THefestoQueueConfig; static;
  end;

  THefestoServerOptions = record
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

    class function Default: THefestoServerOptions; static;
  end;

implementation

{ THefestoQueueConfig }

class function THefestoQueueConfig.Create(
  const AName  : string;
  const AWeight: Integer): THefestoQueueConfig;
begin
  Result.Name   := AName;
  Result.Weight := AWeight;
end;

{ THefestoServerOptions }

class function THefestoServerOptions.Default: THefestoServerOptions;
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
