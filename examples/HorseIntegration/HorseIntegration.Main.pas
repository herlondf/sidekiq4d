unit HorseIntegration.Main;

{
  Exemplo: integração Sidekiq4D + Horse

  Demonstra o padrão clássico HTTP → Job:
    POST /jobs/email   → enfileira EmailSendJob, retorna 202 Accepted
    GET  /jobs/:id     → consulta status do job pelo StateStore
    GET  /health       → health-check do servidor

  Pré-requisitos:
    1. Instale o Horse via GetIt ou adicione o search path manualmente:
       https://github.com/HashLoad/horse
    2. Certifique-se de que o Redis (ou outro store) está acessível.
    3. O Sidekiq4D worker processa os jobs em background.

  Como executar (em dois processos separados ou em threads):
    Processo 1: este servidor HTTP na porta 8080
    Processo 2: TSidekiqServer.New.UseQueue(...).Run
}

interface

procedure StartServer(const APort: Word = 8080);

implementation

uses
  System.SysUtils,
  System.Classes,
  Horse,
  Horse.Request,
  Horse.Response,
  Sidekiq4D,
  Sidekiq4D.Middleware.Horse;

var
  GServer: ISidekiqServer;

// ── Handlers ─────────────────────────────────────────────────────────────

procedure HandlePostEmailJob(AReq: THorseRequest; ARes: THorseResponse);
var
  LBody: string;
begin
  LBody := AReq.Body;
  if LBody.IsEmpty then
    LBody := '{}';

  // Enfileira o job; o correlation_id é injetado automaticamente pelo
  // TSidekiqHorseCorrelationMiddleware se X-Request-ID estiver presente.
  GServer.Enqueue('emails', 'email.send', LBody);

  ARes
    .Status(202)
    .ContentType('application/json')
    .Send('{"queued":true,"queue":"emails","action":"email.send"}');
end;

procedure HandleGetJobStatus(AReq: THorseRequest; ARes: THorseResponse);
var
  LJobId: string;
  LStatus: string;
begin
  LJobId := AReq.Params['id'];

  // Consulta o StateStore — jobs completados registram status via
  // ISidekiqIdempotency (chave 'idempotency:<hash>').
  // Este exemplo consulta diretamente pelo ID (chave 'job:<id>').
  // Adapte conforme o atributo idempotency_key que você usa.
  LStatus := GServer.StateStore.Get('job:status:' + LJobId);
  if LStatus.IsEmpty then
    LStatus := 'unknown';

  ARes
    .Status(200)
    .ContentType('application/json')
    .Send('{"job_id":"' + LJobId + '","status":"' + LStatus + '"}');
end;

procedure HandleHealth(AReq: THorseRequest; ARes: THorseResponse);
begin
  ARes.Status(200).Send('{"status":"ok"}');
end;

// ── Setup ─────────────────────────────────────────────────────────────────

procedure StartServer(const APort: Word);
begin
  // Configura o servidor Sidekiq4D (apenas para enfileirar via cliente;
  // o worker pode rodar em processo/thread separado).
  GServer := TSidekiqServer.New
    .UseQueue(TSidekiqInMemoryQueueAdapter.New('emails'))
    // Para usar Redis como store:
    // .StateStore(TSidekiqPostgresStateStore.New
    //   .Backend(TSidekiqFireDACBackend.New(FDConnection)))
    .UseClientMiddleware(TSidekiqHorseCorrelationMiddleware.New);

  // Middleware Horse para injetar correlation ID
  THorse.Use(TSidekiqHorseMiddleware.CorrelationId);

  // Rotas
  THorse.Post('/jobs/email',  HandlePostEmailJob);
  THorse.Get('/jobs/:id',     HandleGetJobStatus);
  THorse.Get('/health',       HandleHealth);

  Writeln(Format('Horse+Sidekiq4D rodando na porta %d', [APort]));
  Writeln('POST /jobs/email   — enfileira EmailSendJob');
  Writeln('GET  /jobs/:id     — consulta status');
  Writeln('GET  /health       — health check');
  Writeln('Ctrl+C para parar.');
  THorse.Listen(APort);
end;

end.
