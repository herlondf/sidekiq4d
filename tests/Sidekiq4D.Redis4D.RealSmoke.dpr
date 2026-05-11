program Sidekiq4D.Redis4D.RealSmoke;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  Sidekiq4D.Job in '..\src\Sidekiq4D.Job.pas',
  Sidekiq4D.Metadata in '..\src\Sidekiq4D.Metadata.pas',
  Sidekiq4D.Context in '..\src\Sidekiq4D.Context.pas',
  Sidekiq4D.Handler in '..\src\Sidekiq4D.Handler.pas',
  Sidekiq4D.Options in '..\src\Sidekiq4D.Options.pas',
  Sidekiq4D.Store.Interfaces in '..\src\Sidekiq4D.Store.Interfaces.pas',
  Sidekiq4D.Redis.Client in '..\src\adapters\Sidekiq4D.Redis.Client.pas',
  Sidekiq4D.Redis4D.Client in '..\src\adapters\Sidekiq4D.Redis4D.Client.pas',
  Sidekiq4D.Store.Redis4D in '..\src\adapters\Sidekiq4D.Store.Redis4D.pas',
  Sidekiq4D.Locking in '..\src\Sidekiq4D.Locking.pas',
  Sidekiq4D.Locking.Redis4D in '..\src\adapters\Sidekiq4D.Locking.Redis4D.pas',
  Sidekiq4D.Idempotency in '..\src\Sidekiq4D.Idempotency.pas',
  Sidekiq4D.Queue.Interfaces in '..\src\Sidekiq4D.Queue.Interfaces.pas',
  Sidekiq4D.Queue.InMemory in '..\src\Sidekiq4D.Queue.InMemory.pas',
  Sidekiq4D.Dispatcher in '..\src\Sidekiq4D.Dispatcher.pas',
  Sidekiq4D.Retry in '..\src\Sidekiq4D.Retry.pas',
  Sidekiq4D.Telemetry in '..\src\Sidekiq4D.Telemetry.pas',
  Sidekiq4D.Middleware in '..\src\Sidekiq4D.Middleware.pas',
  Sidekiq4D.Scheduled in '..\src\Sidekiq4D.Scheduled.pas',
  Sidekiq4D.Scheduled.Redis4D in '..\src\adapters\Sidekiq4D.Scheduled.Redis4D.pas',
  Sidekiq4D.ClientReliability in '..\src\Sidekiq4D.ClientReliability.pas',
  Sidekiq4D.ClientReliability.Redis4D in '..\src\adapters\Sidekiq4D.ClientReliability.Redis4D.pas',
  Sidekiq4D.Periodic in '..\src\Sidekiq4D.Periodic.pas',
  Sidekiq4D.Batch in '..\src\Sidekiq4D.Batch.pas',
  Sidekiq4D.Executor in '..\src\Sidekiq4D.Executor.pas',
  Sidekiq4D.WorkerPool in '..\src\Sidekiq4D.WorkerPool.pas',
  Sidekiq4D.Server in '..\src\Sidekiq4D.Server.pas';

type
  TCountingHandler = class(TInterfacedObject, ISidekiqJobHandler)
  public
    Calls: Integer;
    LastBody: string;
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

procedure AssertTrue(const ACondition: Boolean; const AMessage: string);
begin
  if not ACondition then
    raise Exception.Create(AMessage);
end;

function NewRedisStore: ISidekiqStateStore;
var
  LStore: TSidekiqRedis4DStateStore;
begin
  LStore := TSidekiqRedis4DStateStore.New
    .RedisClient(TRedis4DClientBridge.NewFromConnectionString('redis://127.0.0.1:6379/0'));
  if not Supports(LStore, ISidekiqStateStore, Result) then
    raise Exception.Create('StateStore Redis4D nao expôs ISidekiqStateStore.');
end;

function NewRedisLockProvider: ISidekiqLockProvider;
var
  LProvider: TSidekiqRedis4DLockProvider;
  LRecoverable: ISidekiqRecoverableLockProvider;
begin
  LProvider := TSidekiqRedis4DLockProvider.New
    .ConnectionString('redis://127.0.0.1:6379/0');
  if not Supports(LProvider, ISidekiqRecoverableLockProvider, LRecoverable) then
    raise Exception.Create('Provider Redis4D nao expôs ISidekiqRecoverableLockProvider.');
  Result := LRecoverable;
end;

procedure CleanupPrefix(const AStore: ISidekiqStateStore; const APrefix: string);
var
  LKey: string;
begin
  for LKey in AStore.ListKeys(APrefix) do
    AStore.Delete(LKey);
end;

procedure TestIdempotencyAndLockingWithRedis;
var
  LStore: ISidekiqStateStore;
  LLockProvider: ISidekiqLockProvider;
  LProbeHandle: ISidekiqLockHandle;
  LQueue: TSidekiqInMemoryQueueAdapter;
  LServer: ISidekiqServer;
  LHandler: TCountingHandler;
  LAttributes: TStringList;
begin
  WriteLn('  smoke: preparando state store');
  LStore := NewRedisStore;
  WriteLn('  smoke: preparando lock provider');
  LLockProvider := NewRedisLockProvider;
  WriteLn('  smoke: testando acquire/release direto do provider');
  LProbeHandle := LLockProvider.TryAcquire('smoke:probe', 5);
  AssertTrue(Assigned(LProbeHandle), 'Provider Redis4D nao conseguiu adquirir lock de probe.');
  LProbeHandle.Release;
  WriteLn('  smoke: limpando prefixos');
  CleanupPrefix(LStore, 'idempotency:smoke:');
  CleanupPrefix(LStore, 'lock:smoke:');

  WriteLn('  smoke: criando fila in-memory');
  LQueue := TSidekiqInMemoryQueueAdapter.New('critical');
  WriteLn('  smoke: criando handler');
  LHandler := TCountingHandler.Create;
  WriteLn('  smoke: criando atributos');
  LAttributes := TStringList.Create;
  try
    WriteLn('  smoke: criando queue/handler');
    LAttributes.Values[TSidekiqJobAttribute.IdempotencyKey] := 'smoke:idempo:1';
    LAttributes.Values[TSidekiqJobAttribute.LockKey] := 'smoke:lock:1';
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);

    WriteLn('  smoke: criando server');
    try
      LServer := TSidekiqServer.New;
    except
      on E: Exception do
        raise Exception.Create('falha em TSidekiqServer.New: ' + E.ClassName + ': ' + E.Message);
    end;
    WriteLn('  smoke: aplicando queue');
    LServer.UseQueue(LQueue);
    WriteLn('  smoke: aplicando state store');
    LServer.StateStore(LStore);
    WriteLn('  smoke: aplicando lock provider');
    LServer.LockProvider(LLockProvider);
    WriteLn('  smoke: aplicando batch size');
    LServer.BatchSize(2);
    WriteLn('  smoke: registrando handler');
    LServer.RegisterHandler('email', LHandler);

    WriteLn('  smoke: executando RunOnce com Redis4D');
    AssertTrue(LServer.RunOnce = 2, 'Runtime Redis4D deveria buscar as duas entregas.');
    AssertTrue(LHandler.Calls = 1, 'Idempotencia com Redis4D deveria evitar processamento duplicado.');
    AssertTrue(LQueue.AckedCount = 2, 'Ambas as entregas deveriam ser ackadas.');
    AssertTrue(
      SameText(LStore.Get('idempotency:smoke:idempo:1'), 'completed'),
      'Estado de idempotencia deveria ser persistido como completed.');
    AssertTrue(
      not LStore.Exists('lock:smoke:lock:1'),
      'Lock distribuido deveria ser liberado ao final do processamento.');
  finally
    LAttributes.Free;
    CleanupPrefix(LStore, 'idempotency:smoke:');
    CleanupPrefix(LStore, 'lock:smoke:');
  end;
end;

procedure TestScheduledStoreWithRedis;
var
  LStore: ISidekiqScheduledStore;
  LNow: TDateTime;
  LResult: TArray<TSidekiqScheduledEntry>;
begin
  WriteLn('  smoke: criando scheduled store Redis4D');
  LStore := TSidekiqRedis4DScheduledStore.New
    .ConnectionString('redis://127.0.0.1:6379/0');

  LStore.Clear;

  LNow := Now;
  LStore.Schedule(MakeScheduledEntry('q1', 'job_a', '{"id":1}', nil, LNow - 3 / (24 * 60)));
  LStore.Schedule(MakeScheduledEntry('q2', 'job_b', '{"id":2}', nil, LNow - 2 / (24 * 60)));
  LStore.Schedule(MakeScheduledEntry('q3', 'job_c', '{"id":3}', nil, LNow - 1 / (24 * 60)));
  LStore.Schedule(MakeScheduledEntry('q4', 'job_d', '{"id":4}', nil, LNow + 10 / (24 * 60)));
  LStore.Schedule(MakeScheduledEntry('q5', 'job_e', '{"id":5}', nil, LNow + 20 / (24 * 60)));

  AssertTrue(LStore.Count = 5,
    Format('Esperado 5 entries antes do pop, obtido %d.', [LStore.Count]));

  LResult := LStore.PopDue(LNow, 10);
  AssertTrue(Length(LResult) = 3,
    Format('PopDue deveria retornar 3 entries vencidas, retornou %d.', [Length(LResult)]));
  AssertTrue(LStore.Count = 2,
    Format('Esperado 2 entries apos pop dos vencidos, obtido %d.', [LStore.Count]));

  // Test List — retorna snapshot sem remover
  LResult := LStore.List;
  AssertTrue(Length(LResult) = 2,
    Format('List deveria retornar 2 entries restantes, retornou %d.', [Length(LResult)]));

  // Test Delete — remove entrada específica
  LStore.Delete('q4', 'job_d', LNow + 10 / (24 * 60));
  AssertTrue(LStore.Count = 1,
    Format('Delete deveria reduzir para 1 entry, encontrado %d.', [LStore.Count]));

  LResult := LStore.List;
  AssertTrue(Length(LResult) = 1, 'List apos Delete deveria retornar 1 entry.');
  AssertTrue(SameText(LResult[0].Action, 'job_e'),
    'Entry restante deveria ser job_e.');

  LStore.Clear;
  WriteLn('  smoke: scheduled store Redis4D validado (List + Delete incluidos).');
end;

procedure TestOutboxWithRedis;
var
  LOutbox: ISidekiqClientOutbox;
  LRequest: TSidekiqPublishRequest;
  LEntries: TArray<TSidekiqOutboxEntry>;
  LFirstId: string;
begin
  WriteLn('  smoke: criando outbox Redis4D');
  LOutbox := TSidekiqRedis4DClientOutbox.New
    .ConnectionString('redis://127.0.0.1:6379/0');

  LOutbox.Clear;
  AssertTrue(LOutbox.Count = 0, 'Outbox deveria estar vazio apos Clear.');

  LRequest.QueueName    := 'critical';
  LRequest.Action       := 'email';
  LRequest.Body         := '{"id":1}';
  LRequest.DelaySeconds := 0;
  SetLength(LRequest.Attributes, 0);

  LOutbox.Save(LRequest);
  LRequest.Body := '{"id":2}';
  LOutbox.Save(LRequest);

  AssertTrue(LOutbox.Count = 2, 'Outbox deveria ter 2 entries apos 2 Saves.');

  LEntries := LOutbox.Entries;
  AssertTrue(Length(LEntries) = 2, 'Entries() deveria retornar 2 elementos.');
  AssertTrue(SameText(LEntries[0].Request.Action, 'email'), 'Action da primeira entry incorreta.');
  LFirstId := LEntries[0].EntryId;

  LOutbox.Remove(LFirstId);
  AssertTrue(LOutbox.Count = 1, 'Outbox deveria ter 1 entry apos Remove da primeira.');

  LEntries := LOutbox.Entries;
  AssertTrue(not SameText(LEntries[0].EntryId, LFirstId), 'Entry removida nao deveria aparecer em Entries.');

  LOutbox.Clear;
  AssertTrue(LOutbox.Count = 0, 'Outbox deveria estar vazio apos Clear final.');
  WriteLn('  smoke: outbox Redis4D validado.');
end;

procedure TestLeaderElectionFailoverWithRedis;
var
  LStore: ISidekiqStateStore;
  LServer1: ISidekiqServer;
  LServer2: ISidekiqServer;
  LQueue1: TSidekiqInMemoryQueueAdapter;
  LQueue2: TSidekiqInMemoryQueueAdapter;
begin
  WriteLn('  smoke: preparando leader election');
  LStore := NewRedisStore;
  CleanupPrefix(LStore, 'leader:smoke-cluster');

  LQueue1 := TSidekiqInMemoryQueueAdapter.New('critical');
  LQueue2 := TSidekiqInMemoryQueueAdapter.New('critical');

  LServer1 := TSidekiqServer.New
    .UseQueue(LQueue1)
    .StateStore(LStore)
    .LockProvider(NewRedisLockProvider)
    .LeaderName('smoke-cluster')
    .LeaderLeaseTtlSeconds(1)
    .UseLeaderElection;

  LServer2 := TSidekiqServer.New
    .UseQueue(LQueue2)
    .StateStore(LStore)
    .LockProvider(NewRedisLockProvider)
    .LeaderName('smoke-cluster')
    .LeaderLeaseTtlSeconds(1)
    .UseLeaderElection;

  AssertTrue(LServer1.RunOnce = 0, 'Servidor 1 deveria adquirir a lideranca sem jobs.');
  AssertTrue(LServer1.IsLeader, 'Servidor 1 deveria ser lider inicial.');

  AssertTrue(LServer2.RunOnce = 0, 'Servidor 2 deveria permanecer follower enquanto o lease estiver ativo.');
  AssertTrue(not LServer2.IsLeader, 'Servidor 2 nao deveria assumir antes da expiracao.');

  Sleep(1100);

  AssertTrue(LServer2.RunOnce = 0, 'Servidor 2 deveria conseguir assumir apos expiracao do lease.');
  AssertTrue(LServer2.IsLeader, 'Servidor 2 deveria assumir a lideranca apos failover.');
  AssertTrue(not LServer1.IsLeader, 'Servidor 1 deveria observar a expiracao do lease.');

  CleanupPrefix(LStore, 'leader:smoke-cluster');
end;

{ TCountingHandler }

function TCountingHandler.CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, 'email');
end;

procedure TCountingHandler.Perform(const AContext: ISidekiqJobContext);
begin
  Inc(Calls);
  LastBody := AContext.Job.Body;
end;

begin
  try
    WriteLn('Sidekiq4D.Redis4D.RealSmoke');
    Flush(Output);
    WriteLn('step 1');
    Flush(Output);
    TestIdempotencyAndLockingWithRedis;
    WriteLn('step 2');
    Flush(Output);
    TestLeaderElectionFailoverWithRedis;
    WriteLn('step 3');
    Flush(Output);
    TestScheduledStoreWithRedis;
    WriteLn('step 4');
    Flush(Output);
    TestOutboxWithRedis;
    WriteLn('Adapters Redis4D validados em runtime real.');
  except
    on E: Exception do
    begin
      WriteLn(E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
