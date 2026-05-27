unit Hefesto.Job;

interface

uses
  System.SysUtils,
  System.Generics.Collections;

type
  THefestoJobAttributes = TDictionary<string, string>;

  IHefestoJobEnvelope = interface
    ['{57A7F4DE-A599-4AB5-9D2D-6F0ADBC04C9F}']
    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Body: string;
    function Action: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

  THefestoJobEnvelope = class(TInterfacedObject, IHefestoJobEnvelope)
  private
    FId: string;
    FQueueName: string;
    FReceiptHandle: string;
    FBody: string;
    FAction: string;
    FAttempts: Integer;
    FAttributes: THefestoJobAttributes;
  public
    constructor Create(
      const AId: string;
      const AQueueName: string;
      const AReceiptHandle: string;
      const ABody: string;
      const AAction: string;
      const AAttempts: Integer = 0);
    destructor Destroy; override;

    class function New(
      const AId: string;
      const AQueueName: string;
      const AReceiptHandle: string;
      const ABody: string;
      const AAction: string;
      const AAttempts: Integer = 0): IHefestoJobEnvelope;

    function AddAttribute(const AName, AValue: string): THefestoJobEnvelope;

    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Body: string;
    function Action: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

implementation

{ THefestoJobEnvelope }

function THefestoJobEnvelope.Action: string;
begin
  Result := FAction;
end;

function THefestoJobEnvelope.AddAttribute(
  const AName, AValue: string): THefestoJobEnvelope;
begin
  Result := Self;
  FAttributes.AddOrSetValue(AName, AValue);
end;

function THefestoJobEnvelope.Attempts: Integer;
begin
  Result := FAttempts;
end;

function THefestoJobEnvelope.Attribute(const AName: string): string;
begin
  if not FAttributes.TryGetValue(AName, Result) then
    Result := EmptyStr;
end;

function THefestoJobEnvelope.Body: string;
begin
  Result := FBody;
end;

constructor THefestoJobEnvelope.Create(
  const AId,
  AQueueName,
  AReceiptHandle,
  ABody,
  AAction: string;
  const AAttempts: Integer);
begin
  inherited Create;
  FId := AId;
  FQueueName := AQueueName;
  FReceiptHandle := AReceiptHandle;
  FBody := ABody;
  FAction := AAction;
  FAttempts := AAttempts;
  FAttributes := THefestoJobAttributes.Create;
end;

destructor THefestoJobEnvelope.Destroy;
begin
  FAttributes.Free;
  inherited;
end;

function THefestoJobEnvelope.Id: string;
begin
  Result := FId;
end;

class function THefestoJobEnvelope.New(
  const AId,
  AQueueName,
  AReceiptHandle,
  ABody,
  AAction: string;
  const AAttempts: Integer): IHefestoJobEnvelope;
begin
  Result := THefestoJobEnvelope.Create(
    AId,
    AQueueName,
    AReceiptHandle,
    ABody,
    AAction,
    AAttempts);
end;

function THefestoJobEnvelope.QueueName: string;
begin
  Result := FQueueName;
end;

function THefestoJobEnvelope.ReceiptHandle: string;
begin
  Result := FReceiptHandle;
end;

end.
