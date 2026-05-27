unit Hefesto.DeadLetter;

{
  Dead Letter Queue (DLQ) — interface e entry record.

  Quando um job esgota todas as tentativas de retry, o executor o empurra
  para a DLQ em vez de descartá-lo silenciosamente.  A partir daí, o
  operador pode inspecionar, reprocessar (Retry) ou descartar (Delete)
  cada entrada individualmente — via código ou pelo dashboard web.

  THefestoDeadLetterEntry é um value record (sem heap allocation).
  IHefestoDeadLetterQueue é a única interface pública desta unit;
  implementações concretas ficam nos adapters.
}

interface

uses
  System.SysUtils;

type
  THefestoDeadLetterEntry = record
    JobId        : string;    // ID único do job
    JobJson      : string;    // corpo original (payload)
    Action       : string;    // nome do handler
    OriginalQueue: string;    // fila de origem
    ErrorMessage : string;    // mensagem da última exceção
    RetryCount   : Integer;   // número de tentativas executadas
    FailedAt     : TDateTime; // momento da falha definitiva
  end;

  IHefestoDeadLetterQueue = interface
    ['{A4B2C1D5-E6F7-4890-AB3C-EF0123456789}']
    { Registra uma entrada na DLQ. Thread-safe. }
    procedure Push(const AEntry: THefestoDeadLetterEntry);

    { Lê e remove atomicamente uma entrada (usado internamente). }
    function Pop(const AJobId: string): THefestoDeadLetterEntry;

    { Retorna todas as entradas presentes na DLQ. }
    function List: TArray<THefestoDeadLetterEntry>;

    { Re-enfileira o job na fila original e remove da DLQ. }
    procedure Retry(const AJobId: string);

    { Remove permanentemente uma entrada da DLQ. }
    procedure Delete(const AJobId: string);

    { Número de entradas na DLQ. }
    function Count: Integer;
  end;

implementation

end.
