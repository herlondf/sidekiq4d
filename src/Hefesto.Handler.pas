unit Hefesto.Handler;

interface

uses
  Hefesto.Job,
  Hefesto.Context;

type
  IHefestoJobHandler = interface
    ['{3CB4886C-67A7-4C1F-9C2F-3E7E1D1F292A}']
    function CanHandle(const AJob: IHefestoJobEnvelope): Boolean;
    procedure Perform(const AContext: IHefestoJobContext);
  end;

implementation

end.
