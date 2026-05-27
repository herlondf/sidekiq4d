unit Hefesto.Middleware.Compression;

interface

uses
  System.SysUtils,
  System.Classes,
  System.ZLib,
  Hefesto.Job,
  Hefesto.Queue.Interfaces,
  Hefesto.Middleware;

type
  THefestoCompressionClientMiddleware = class(TInterfacedObject, IHefestoClientMiddleware)
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoCompressionClientMiddleware;

    // IHefestoClientMiddleware
    procedure Call(
      const AQueueName: string;
      const AAction: string;
      const ABody: string;
      const AAttributes: TStrings;
      const ANext: THefestoNextProc);
  end;

  THefestoCompressionServerMiddleware = class(TInterfacedObject, IHefestoServerMiddleware)
  public
    constructor Create;
    destructor Destroy; override;

    class function New: THefestoCompressionServerMiddleware;

    // IHefestoServerMiddleware
    procedure Call(const AQueue: IHefestoQueueAdapter;
      const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
  end;

implementation

uses
  System.NetEncoding;

{ THefestoCompressionClientMiddleware }

constructor THefestoCompressionClientMiddleware.Create;
begin
  inherited Create;
end;

destructor THefestoCompressionClientMiddleware.Destroy;
begin
  inherited;
end;

class function THefestoCompressionClientMiddleware.New: THefestoCompressionClientMiddleware;
begin
  Result := THefestoCompressionClientMiddleware.Create;
end;

procedure THefestoCompressionClientMiddleware.Call(
  const AQueueName: string;
  const AAction: string;
  const ABody: string;
  const AAttributes: TStrings;
  const ANext: THefestoNextProc);
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

{ THefestoCompressionServerMiddleware }

constructor THefestoCompressionServerMiddleware.Create;
begin
  inherited Create;
end;

destructor THefestoCompressionServerMiddleware.Destroy;
begin
  inherited;
end;

class function THefestoCompressionServerMiddleware.New: THefestoCompressionServerMiddleware;
begin
  Result := THefestoCompressionServerMiddleware.Create;
end;

procedure THefestoCompressionServerMiddleware.Call(
  const AQueue: IHefestoQueueAdapter;
  const AJob: IHefestoJobEnvelope; const ANext: THefestoNextProc);
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
