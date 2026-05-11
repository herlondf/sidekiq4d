unit Sidekiq4D.Ack;

interface

uses
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Telemetry;

type
  ISidekiqAckPolicy = interface
    ['{20D9FF81-E68D-4868-BBB1-75E1CC658B01}']
    procedure OnAlreadyCompleted(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnSuccess(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnRetry(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ADelaySeconds: Integer;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnDeadLetter(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AReason: string;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnDiscard(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AReason: string;
      const ATelemetry: ISidekiqTelemetry);
  end;

  TSidekiqDefaultAckPolicy = class(TInterfacedObject, ISidekiqAckPolicy)
  public
    class function New: ISidekiqAckPolicy;

    procedure OnAlreadyCompleted(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnSuccess(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnRetry(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ADelaySeconds: Integer;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnDeadLetter(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AReason: string;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnDiscard(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AReason: string;
      const ATelemetry: ISidekiqTelemetry);
  end;

  TSidekiqNoAckPolicy = class(TInterfacedObject, ISidekiqAckPolicy)
  public
    class function New: ISidekiqAckPolicy;

    procedure OnAlreadyCompleted(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnSuccess(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnRetry(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const ADelaySeconds: Integer;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnDeadLetter(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AReason: string;
      const ATelemetry: ISidekiqTelemetry);
    procedure OnDiscard(
      const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope;
      const AReason: string;
      const ATelemetry: ISidekiqTelemetry);
  end;

implementation

{ TSidekiqDefaultAckPolicy }

class function TSidekiqDefaultAckPolicy.New: ISidekiqAckPolicy;
begin
  Result := TSidekiqDefaultAckPolicy.Create;
end;

procedure TSidekiqDefaultAckPolicy.OnAlreadyCompleted(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ATelemetry: ISidekiqTelemetry);
begin
  OnSuccess(AQueue, AJob, ATelemetry);
end;

procedure TSidekiqDefaultAckPolicy.OnDeadLetter(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AReason: string;
  const ATelemetry: ISidekiqTelemetry);
begin
  AQueue.MoveToDeadLetter(AJob, AReason);
  ATelemetry.JobDeadLettered(AJob, AReason);
end;

procedure TSidekiqDefaultAckPolicy.OnDiscard(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AReason: string;
  const ATelemetry: ISidekiqTelemetry);
begin
  AQueue.Ack(AJob);
  ATelemetry.JobAcked(AJob);
  ATelemetry.JobDiscarded(AJob, AReason);
end;

procedure TSidekiqDefaultAckPolicy.OnRetry(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer;
  const ATelemetry: ISidekiqTelemetry);
begin
  AQueue.Nack(AJob, ADelaySeconds);
  ATelemetry.JobRetried(AJob, ADelaySeconds);
end;

procedure TSidekiqDefaultAckPolicy.OnSuccess(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ATelemetry: ISidekiqTelemetry);
begin
  AQueue.Ack(AJob);
  ATelemetry.JobAcked(AJob);
end;

{ TSidekiqNoAckPolicy }

class function TSidekiqNoAckPolicy.New: ISidekiqAckPolicy;
begin
  Result := TSidekiqNoAckPolicy.Create;
end;

procedure TSidekiqNoAckPolicy.OnAlreadyCompleted(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ATelemetry: ISidekiqTelemetry);
begin
end;

procedure TSidekiqNoAckPolicy.OnDeadLetter(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AReason: string;
  const ATelemetry: ISidekiqTelemetry);
begin
end;

procedure TSidekiqNoAckPolicy.OnDiscard(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const AReason: string;
  const ATelemetry: ISidekiqTelemetry);
begin
end;

procedure TSidekiqNoAckPolicy.OnRetry(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ADelaySeconds: Integer;
  const ATelemetry: ISidekiqTelemetry);
begin
end;

procedure TSidekiqNoAckPolicy.OnSuccess(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope;
  const ATelemetry: ISidekiqTelemetry);
begin
end;

end.
