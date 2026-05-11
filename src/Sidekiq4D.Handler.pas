unit Sidekiq4D.Handler;

interface

uses
  Sidekiq4D.Job,
  Sidekiq4D.Context;

type
  ISidekiqJobHandler = interface
    ['{3CB4886C-67A7-4C1F-9C2F-3E7E1D1F292A}']
    function CanHandle(const AJob: ISidekiqJobEnvelope): Boolean;
    procedure Perform(const AContext: ISidekiqJobContext);
  end;

implementation

end.
