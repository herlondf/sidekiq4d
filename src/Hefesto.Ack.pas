unit Hefesto.Ack;

interface

uses
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Telemetry;

type
  IHefestoAckPolicy = interface
    ['{20D9FF81-E68D-4868-BBB1-75E1CC658B01}']
    procedure OnAlreadyCompleted(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ATelemetry: IHefestoTelemetry);
    procedure OnSuccess(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ATelemetry: IHefestoTelemetry);
    procedure OnRetry(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ADelaySeconds: Integer;
      const ATelemetry: IHefestoTelemetry);
    procedure OnDeadLetter(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AReason: string;
      const ATelemetry: IHefestoTelemetry);
    procedure OnDiscard(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AReason: string;
      const ATelemetry: IHefestoTelemetry);
  end;

  THefestoDefaultAckPolicy = class(TInterfacedObject, IHefestoAckPolicy)
  public
    class function New: IHefestoAckPolicy;

    procedure OnAlreadyCompleted(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ATelemetry: IHefestoTelemetry);
    procedure OnSuccess(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ATelemetry: IHefestoTelemetry);
    procedure OnRetry(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ADelaySeconds: Integer;
      const ATelemetry: IHefestoTelemetry);
    procedure OnDeadLetter(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AReason: string;
      const ATelemetry: IHefestoTelemetry);
    procedure OnDiscard(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AReason: string;
      const ATelemetry: IHefestoTelemetry);
  end;

  THefestoNoAckPolicy = class(TInterfacedObject, IHefestoAckPolicy)
  public
    class function New: IHefestoAckPolicy;

    procedure OnAlreadyCompleted(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ATelemetry: IHefestoTelemetry);
    procedure OnSuccess(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ATelemetry: IHefestoTelemetry);
    procedure OnRetry(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const ADelaySeconds: Integer;
      const ATelemetry: IHefestoTelemetry);
    procedure OnDeadLetter(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AReason: string;
      const ATelemetry: IHefestoTelemetry);
    procedure OnDiscard(
      const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope;
      const AReason: string;
      const ATelemetry: IHefestoTelemetry);
  end;

implementation

{ THefestoDefaultAckPolicy }

class function THefestoDefaultAckPolicy.New: IHefestoAckPolicy;
begin
  Result := THefestoDefaultAckPolicy.Create;
end;

procedure THefestoDefaultAckPolicy.OnAlreadyCompleted(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ATelemetry: IHefestoTelemetry);
begin
  OnSuccess(AQueue, AJob, ATelemetry);
end;

procedure THefestoDefaultAckPolicy.OnDeadLetter(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AReason: string;
  const ATelemetry: IHefestoTelemetry);
begin
  AQueue.MoveToDeadLetter(AJob, AReason);
  ATelemetry.JobDeadLettered(AJob, AReason);
end;

procedure THefestoDefaultAckPolicy.OnDiscard(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AReason: string;
  const ATelemetry: IHefestoTelemetry);
begin
  AQueue.Ack(AJob);
  ATelemetry.JobAcked(AJob);
  ATelemetry.JobDiscarded(AJob, AReason);
end;

procedure THefestoDefaultAckPolicy.OnRetry(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer;
  const ATelemetry: IHefestoTelemetry);
begin
  AQueue.Nack(AJob, ADelaySeconds);
  ATelemetry.JobRetried(AJob, ADelaySeconds);
end;

procedure THefestoDefaultAckPolicy.OnSuccess(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ATelemetry: IHefestoTelemetry);
begin
  AQueue.Ack(AJob);
  ATelemetry.JobAcked(AJob);
end;

{ THefestoNoAckPolicy }

class function THefestoNoAckPolicy.New: IHefestoAckPolicy;
begin
  Result := THefestoNoAckPolicy.Create;
end;

procedure THefestoNoAckPolicy.OnAlreadyCompleted(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ATelemetry: IHefestoTelemetry);
begin
end;

procedure THefestoNoAckPolicy.OnDeadLetter(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AReason: string;
  const ATelemetry: IHefestoTelemetry);
begin
end;

procedure THefestoNoAckPolicy.OnDiscard(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const AReason: string;
  const ATelemetry: IHefestoTelemetry);
begin
end;

procedure THefestoNoAckPolicy.OnRetry(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ADelaySeconds: Integer;
  const ATelemetry: IHefestoTelemetry);
begin
end;

procedure THefestoNoAckPolicy.OnSuccess(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope;
  const ATelemetry: IHefestoTelemetry);
begin
end;

end.
