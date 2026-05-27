program Hefesto.Redis4D.RealSmoke;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  Hefesto.Job in '..\src\Hefesto.Job.pas',
  Hefesto.Metadata in '..\src\Hefesto.Metadata.pas',
  Hefesto.Context in '..\src\Hefesto.Context.pas',
  Hefesto.Handler in '..\src\Hefesto.Handler.pas',
  Hefesto.Options in '..\src\Hefesto.Options.pas',
  Hefesto.Store.Interfaces in '..\src\Hefesto.Store.Interfaces.pas',
  Hefesto.Redis.Client in '..\src\adapters\Hefesto.Redis.Client.pas',
  Hefesto.Redis4D.Client in '..\src\adapters\Hefesto.Redis4D.Client.pas',
  Hefesto.Store.Redis4D in '..\src\adapters\Hefesto.Store.Redis4D.pas',
  Hefesto.Locking in '..\src\Hefesto.Locking.pas',
  Hefesto.Locking.Redis4D in '..\src\adapters\Hefesto.Locking.Redis4D.pas',
  Hefesto.Idempotency in '..\src\Hefesto.Idempotency.pas',
  Hefesto.Queue.Interfaces in '..\src\Hefesto.Queue.Interfaces.pas',
  Hefesto.Queue.InMemory in '..\src\Hefesto.Queue.InMemory.pas',
  Hefesto.Dispatcher in '..\src\Hefesto.Dispatcher.pas',
  Hefesto.Retry in '..\src\Hefesto.Retry.pas',
  Hefesto.Telemetry in '..\src\Hefesto.Telemetry.pas',
  Hefesto.Middleware in '..\src\Hefesto.Middleware.pas',
  Hefesto.Scheduled in '..\src\Hefesto.Scheduled.pas',
  Hefesto.Scheduled.Redis4D in '..\src\adapters\Hefesto.Scheduled.Redis4D.pas',
  Hefesto.ClientReliability in '..\src\Hefesto.ClientReliability.pas',
  Hefesto.ClientReliability.Redis4D in '..\src\adapters\Hefesto.ClientReliability.Redis4D.pas',
  Hefesto.Periodic in '..\src\Hefesto.Periodic.pas',
  Hefesto.Batch in '..\src\Hefesto.Batch.pas',
  Hefesto.Executor in '..\src\Hefesto.Executor.pas',
  Hefesto.WorkerPool in '..\src\Hefesto.WorkerPool.pas',
  Hefesto.Server in '..\src\Hefesto.Server.pas';

type
  TCountingHandler = class(TInterfacedObject, IHefestoJobHandler)
  public
    Calls: Integer;
    LastBody: string;
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

procedure AssertTrue(const ACondition: Boolean; const AMessage: string);
begin
  if not ACondition then
    raise Exception.Create(AMessage);
end;

function NewRedisStore: IHefestoStateStore;
var
  LStore: THefestoRedis4DStateStore;
begin
  LStore := THefestoRedis4DStateStore.New
    .RedisClient(TRedis4DClientBridge.NewFromConnectionString('redis://127.0.0.1:6379/0'));
  if not Supports(LStore, IHefestoStateStore, Result) then
    raise Exception.Create('StateStore Redis4D nao expôs IHefestoStateStore.');
end;

function NewRedisLockProvider: IHefestoLockProvider;
var
  LProvider: THefestoRedis4DLockProvider;
  LRecoverable: IHefestoRecoverableLockProvider;
begin
  LProvider := THefestoRedis4DLockProvider.New
    .ConnectionString('redis://127.0.0.1:6379/0');
  if not Supports(LProvider, IHefestoRecoverableLockProvider, LRecoverable) then
    raise Exception.Create('Provider Redis4D nao expôs IHefestoRecoverableLockProvider.');
  Result := LRecoverable;
end;

procedure CleanupPrefix(const AStore: IHefestoStateStore; const APrefix: string);
var
  LKey: string;
begin
  for LKey in AStore.ListKeys(APrefix) do
    AStore.Delete(LKey);
end;

procedure TestIdempotencyAndLockingWithRedis;
var
  LStore: IHefestoStateStore;
  LLockProvider: IHefestoLockProvider;
  LProbeHandle: IHefestoLockHandle;
  LQueue: THefestoInMemoryQueueAdapter;
  LServer: IHefestoServer;
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
  LQueue := THefestoInMemoryQueueAdapter.New('critical');
  WriteLn('  smoke: criando handler');
  LHandler := TCountingHandler.Create;
  WriteLn('  smoke: criando atributos');
  LAttributes := TStringList.Create;
  try
    WriteLn('  smoke: criando queue/handler');
    LAttributes.Values[THefestoJobAttribute.IdempotencyKey] := 'smoke:idempo:1';
    LAttributes.Values[THefestoJobAttribute.LockKey] := 'smoke:lock:1';
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);
    LQueue.EnqueueWithAttributes('email', '{"id":1}', LAttributes);

    WriteLn('  smoke: criando server');
    try
      LServer := THefestoServer.New;
    except
      on E: Exception do
        raise Exception.Create('falha em THefestoServer.New: ' + E.ClassName + ': ' + E.Message);
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
  LStore: IHefestoScheduledStore;
  LNow: TDateTime;
  LResult: TArray<THefestoScheduledEntry>;
begin
  WriteLn('  smoke: criando scheduled store Redis4D');
  LStore := THefestoRedis4DScheduledStore.New
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
  LOutbox: IHefestoClientOutbox;
  LRequest: THefestoPublishRequest;
  LEntries: TArray<THefestoOutboxEntry>;
  LFirstId: string;
begin
  WriteLn('  smoke: criando outbox Redis4D');
  LOutbox := THefestoRedis4DClientOutbox.New
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
  LStore: IHefestoStateStore;
  LServer1: IHefestoServer;
  LServer2: IHefestoServer;
  LQueue1: THefestoInMemoryQueueAdapter;
  LQueue2: THefestoInMemoryQueueAdapter;
begin
  WriteLn('  smoke: preparando leader election');
  LStore := NewRedisStore;
  CleanupPrefix(LStore, 'leader:smoke-cluster');

  LQueue1 := THefestoInMemoryQueueAdapter.New('critical');
  LQueue2 := THefestoInMemoryQueueAdapter.New('critical');

  LServer1 := THefestoServer.New
    .UseQueue(LQueue1)
    .StateStore(LStore)
    .LockProvider(NewRedisLockProvider)
    .LeaderName('smoke-cluster')
    .LeaderLeaseTtlSeconds(1)
    .UseLeaderElection;

  LServer2 := THefestoServer.New
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

function TCountingHandler.CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
begin
  Result := SameText(AJob.Action, 'email');
end;

procedure TCountingHandler.Perform(const AContext: IHefestoJobContext);
begin
  Inc(Calls);
  LastBody := AContext.Job.Body;
end;

begin
  try
    WriteLn('Hefesto.Redis4D.RealSmoke');
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
