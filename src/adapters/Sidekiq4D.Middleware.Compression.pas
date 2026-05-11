unit Sidekiq4D.Middleware.Compression;

interface

uses
  System.SysUtils,
  System.Classes,
  System.ZLib,
  Sidekiq4D.Job,
  Sidekiq4D.Queue.Interfaces,
  Sidekiq4D.Middleware;

type
  TSidekiqCompressionClientMiddleware = class(TInterfacedObject, ISidekiqClientMiddleware)
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqCompressionClientMiddleware;

    // ISidekiqClientMiddleware
    procedure Call(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const ANext: TSidekiqNextProc);
  end;

  TSidekiqCompressionServerMiddleware = class(TInterfacedObject, ISidekiqServerMiddleware)
  public
    constructor Create;
    destructor Destroy; override;

    class function New: TSidekiqCompressionServerMiddleware;

    // ISidekiqServerMiddleware
    procedure Call(const AQueue: ISidekiqQueueAdapter;
      const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
  end;

implementation

uses
  System.NetEncoding;

{ TSidekiqCompressionClientMiddleware }

constructor TSidekiqCompressionClientMiddleware.Create;
begin
  inherited Create;
end;

destructor TSidekiqCompressionClientMiddleware.Destroy;
begin
  inherited;
end;

class function TSidekiqCompressionClientMiddleware.New: TSidekiqCompressionClientMiddleware;
begin
  Result := TSidekiqCompressionClientMiddleware.Create;
end;

procedure TSidekiqCompressionClientMiddleware.Call(
  const AQueueName: string;
  const AAction: string;
  const ABody: string;
  const AAttributes: TStrings;
  const ANext: TSidekiqNextProc);
var
  LInput: TBytesStream;
  LOutput: TBytesStream;
  LCompressor: TZCompressionStream;
  LCompressedBase64: string;
begin
  LInput := TBytesStream.Create(TEncoding.UTF8.GetBytes(ABody));
  LOutput := TBytesStream.Create;
  try
    LCompressor := TZCompressionStream.Create(clDefault, LOutput);
    try
      LCompressor.CopyFrom(LInput, 0);
    finally
      LCompressor.Free;
    end;

    LCompressedBase64 := TNetEncoding.Base64.EncodeBytesToString(
      Copy(LOutput.Bytes, 0, LOutput.Size));

    AAttributes.Values['compressed'] := 'zlib';
    AAttributes.Values['compressed_body'] := LCompressedBase64;
  finally
    LInput.Free;
    LOutput.Free;
  end;

  ANext;
end;

{ TSidekiqCompressionServerMiddleware }

constructor TSidekiqCompressionServerMiddleware.Create;
begin
  inherited Create;
end;

destructor TSidekiqCompressionServerMiddleware.Destroy;
begin
  inherited;
end;

class function TSidekiqCompressionServerMiddleware.New: TSidekiqCompressionServerMiddleware;
begin
  Result := TSidekiqCompressionServerMiddleware.Create;
end;

procedure TSidekiqCompressionServerMiddleware.Call(
  const AQueue: ISidekiqQueueAdapter;
  const AJob: ISidekiqJobEnvelope; const ANext: TSidekiqNextProc);
var
  LCompressed: string;
  LCompressedBytes: TBytes;
  LInput: TBytesStream;
  LOutput: TBytesStream;
  LDecompressor: TZDecompressionStream;
  LDecompressedSize: Int64;
begin
  LCompressed := AJob.Attribute('compressed');

  if not SameText(LCompressed, 'zlib') then
  begin
    ANext;
    Exit;
  end;

  // Decompress for demo/logging purposes (Body is read-only on the envelope)
  LCompressedBytes := TNetEncoding.Base64.DecodeStringToBytes(
    AJob.Attribute('compressed_body'));

  LInput := TBytesStream.Create(LCompressedBytes);
  LOutput := TBytesStream.Create;
  try
    LDecompressor := TZDecompressionStream.Create(LInput);
    try
      LOutput.CopyFrom(LDecompressor, 0);
    finally
      LDecompressor.Free;
    end;

    LDecompressedSize := LOutput.Size;
    WriteLn(Format('[Compression] Job %s: decompressed body size = %d bytes',
      [AJob.Id, LDecompressedSize]));
  finally
    LInput.Free;
    LOutput.Free;
  end;

  ANext;
end;

end.
