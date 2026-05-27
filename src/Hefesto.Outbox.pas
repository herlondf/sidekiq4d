unit Hefesto.Outbox;

{
  Transactional Outbox — interfaces puras, sem dependências externas.

  Padrão: o job é persistido DENTRO da mesma transação de banco de dados
  do código de negócio.  Um poller separado lê as entradas não publicadas
  e as enfileira no Hefesto.  Dessa forma, o job nunca se perde em caso
  de crash entre o commit e o Enqueue.

  Fluxo completo:
    1. Código de negócio abre transação no banco
    2. Insere/atualiza as tabelas de negócio
    3. Chama IHefestoOutbox.Save(..., AConnection) — insere na tabela
       sidekiq_outbox dentro da mesma transação
    4. Commit — ambas as escritas (negócio + outbox) são atômicas
    5. IHefestoOutboxPoller.PollOnce detecta a linha status='pending',
       chama Hefesto.Enqueue e marca status='published'

  AConnection é TObject para manter esta unit livre de dependências
  FireDAC.  O adapter concreto (Hefesto.Outbox.FireDAC) faz o cast
  para TFDConnection internamente.
}

interface

uses
  System.SysUtils;

type
  IHefestoOutbox = interface
    ['{C3D4E5F6-A7B8-4C9D-8E0F-1A2B3C4D5E6F}']
    { Persiste o job pendente usando a conexão/transação fornecida.
      AConnection deve ser do tipo suportado pelo adapter concreto
      (tipicamente TFDConnection).  A transação NÃO é commitada aqui —
      o chamador é responsável pelo commit. }
    procedure Save(
      const AQueueName: string;
      const AAction   : string;
      const APayload  : string;
      const AConnection: TObject);
  end;

  IHefestoOutboxPoller = interface
    ['{D4E5F6A7-B8C9-4D0E-9F1A-2B3C4D5E6F7A}']
    { Inicia o loop de polling em background. }
    procedure Start;

    { Para o loop e aguarda a thread encerrar. }
    procedure Stop;

    { Processa um lote de entradas pendentes.
      Retorna o número de jobs publicados nesta chamada. }
    function PollOnce: Integer;
  end;

implementation

end.
