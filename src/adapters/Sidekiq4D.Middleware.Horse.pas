unit Sidekiq4D.Middleware.Horse;

{
  Integração com o framework Horse (https://github.com/HashLoad/horse).

  Fornece dois elementos:

  1. TSidekiqHorseMiddleware.CorrelationId — THorseCallback que extrai o
     header X-Request-ID de cada requisição HTTP e o armazena em uma
     threadvar. Como o Horse processa cada request em um thread Indy
     dedicado, o valor fica disponível para qualquer código executado
     naquele mesmo thread dentro do handler.

  2. TSidekiqHorseCorrelationMiddleware — ISidekiqClientMiddleware que
     lê a threadvar e injeta o valor como atributo 'correlation_id' no
     job que está sendo enfileirado. Registre-o com
     Server.UseClientMiddleware(TSidekiqHorseCorrelationMiddleware.New).

  Uso típico:

    THorse.Use(TSidekiqHorseMiddleware.CorrelationId);

    Server
      .UseClientMiddleware(TSidekiqHorseCorrelationMiddleware.New)
      ...;

    THorse.Post('/jobs/email',
      procedure(AReq: THorseRequest; ARes: THorseResponse)
      begin
        Server.Enqueue('emails', 'email.send', AReq.Body);
        ARes.Status(202).Send('{"queued":true}');
      end);

  REQUISITO: a unit Horse deve estar no search path do projeto.
  Instale via GetIt: "Horse — Minimalist web framework for Delphi"
  ou adicione manualmente o caminho do repositório.
}

interface

uses
  System.SysUtils,
  System.Classes,
  Horse,
  Horse.Request,
  Horse.Response,
  Sidekiq4D.Middleware;

type
  { ISidekiqClientMiddleware que injeta correlation_id nos atributos do job. }
  TSidekiqHorseCorrelationMiddleware = class(TInterfacedObject,
    ISidekiqClientMiddleware)
  public
    class function New: ISidekiqClientMiddleware;

    { ISidekiqClientMiddleware }
    procedure Call(
      const AQueueName : string;
      const AAction    : string;
      const ABody      : string;
      const AAttributes: TStrings;
      const ANext      : TSidekiqNextProc);
  end;

  { Fábrica de callbacks Horse para injeção de correlation ID. }
  TSidekiqHorseMiddleware = class
  public
    { Retorna um THorseCallback que armazena X-Request-ID na threadvar
      antes de invocar o próximo middleware/handler. }
    class function CorrelationId: THorseCallback; static;
  end;

implementation

{ threadvar — por ser específica ao thread do request Horse, o valor é
  automaticamente isolado entre requisições concorrentes. }
threadvar
  _CorrelationId: string;

{ TSidekiqHorseCorrelationMiddleware }

class function TSidekiqHorseCorrelationMiddleware.New: ISidekiqClientMiddleware;
begin
  Result := TSidekiqHorseCorrelationMiddleware.Create;
end;

procedure TSidekiqHorseCorrelationMiddleware.Call(
  const AQueueName : string;
  const AAction    : string;
  const ABody      : string;
  const AAttributes: TStrings;
  const ANext      : TSidekiqNextProc);
var
  LCorrelation: string;
begin
  LCorrelation := _CorrelationId;
  if Assigned(AAttributes) and not LCorrelation.IsEmpty then
    AAttributes.Values['correlation_id'] := LCorrelation;
  ANext;
end;

{ TSidekiqHorseMiddleware }

class function TSidekiqHorseMiddleware.CorrelationId: THorseCallback;
begin
  Result :=
    procedure(AReq: THorseRequest; ARes: THorseResponse; ANext: TProc)
    begin
      _CorrelationId := AReq.Headers['X-Request-ID'];
      try
        ANext;
      finally
        _CorrelationId := '';
      end;
    end;
end;

end.
