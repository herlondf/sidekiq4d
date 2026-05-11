unit Sidekiq4D.Job;

interface

uses
  System.SysUtils,
  System.Generics.Collections;

type
  TSidekiqJobAttributes = TDictionary<string, string>;

  ISidekiqJobEnvelope = interface
    ['{57A7F4DE-A599-4AB5-9D2D-6F0ADBC04C9F}']
    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Body: string;
    function Action: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

  TSidekiqJobEnvelope = class(TInterfacedObject, ISidekiqJobEnvelope)
  private
    FId: string;
    FQueueName: string;
    FReceiptHandle: string;
    FBody: string;
    FAction: string;
    FAttempts: Integer;
    FAttributes: TSidekiqJobAttributes;
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
      const AAttempts: Integer = 0): ISidekiqJobEnvelope;

    function AddAttribute(const AName, AValue: string): TSidekiqJobEnvelope;

    function Id: string;
    function QueueName: string;
    function ReceiptHandle: string;
    function Body: string;
    function Action: string;
    function Attribute(const AName: string): string;
    function Attempts: Integer;
  end;

implementation

{ TSidekiqJobEnvelope }

function TSidekiqJobEnvelope.Action: string;
begin
  Result := FAction;
end;

function TSidekiqJobEnvelope.AddAttribute(
  const AName, AValue: string): TSidekiqJobEnvelope;
begin
  Result := Self;
  FAttributes.AddOrSetValue(AName, AValue);
end;

function TSidekiqJobEnvelope.Attempts: Integer;
begin
  Result := FAttempts;
end;

function TSidekiqJobEnvelope.Attribute(const AName: string): string;
begin
  if not FAttributes.TryGetValue(AName, Result) then
    Result := EmptyStr;
end;

function TSidekiqJobEnvelope.Body: string;
begin
  Result := FBody;
end;

constructor TSidekiqJobEnvelope.Create(
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
  FAttributes := TSidekiqJobAttributes.Create;
end;

destructor TSidekiqJobEnvelope.Destroy;
begin
  FAttributes.Free;
  inherited;
end;

function TSidekiqJobEnvelope.Id: string;
begin
  Result := FId;
end;

class function TSidekiqJobEnvelope.New(
  const AId,
  AQueueName,
  AReceiptHandle,
  ABody,
  AAction: string;
  const AAttempts: Integer): ISidekiqJobEnvelope;
begin
  Result := TSidekiqJobEnvelope.Create(
    AId,
    AQueueName,
    AReceiptHandle,
    ABody,
    AAction,
    AAttempts);
end;

function TSidekiqJobEnvelope.QueueName: string;
begin
  Result := FQueueName;
end;

function TSidekiqJobEnvelope.ReceiptHandle: string;
begin
  Result := FReceiptHandle;
end;

end.
