unit Sidekiq4D.AWS.Signing;

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  Sidekiq4D.HTTP;

type
  TSidekiqAwsV4Signer = class
  private
    const
      ALGORITHM = 'AWS4-HMAC-SHA256';
  private
    FAccessKey: string;
    FSecretKey: string;
    FRegion: string;
    FService: string;
    FSessionToken: string;

    class function NormalizeHeaderValue(const AValue: string): string; static;
    class function HashSHA256Hex(const AValue: string): string; static;
    class function HmacSHA256(
      const AData: string;
      const AKey: TBytes): TBytes; overload; static;
    class function HmacSHA256(
      const AData, AKey: string): TBytes; overload; static;
    class function PercentEncode(const AValue: string): string; static;
    class function BuildCanonicalQuery(const AQuery: string): string; static;
    class function BuildCanonicalHeaders(
      const AHeaders: TDictionary<string, string>;
      out ASignedHeaders: string): string; static;
    class function BuildSigningKey(
      const ASecretKey, ADateStamp, ARegion, AService: string): TBytes; static;
  public
    constructor Create;

    class function New: TSidekiqAwsV4Signer;

    function AccessKey(const AValue: string): TSidekiqAwsV4Signer;
    function SecretKey(const AValue: string): TSidekiqAwsV4Signer;
    function Region(const AValue: string): TSidekiqAwsV4Signer;
    function Service(const AValue: string): TSidekiqAwsV4Signer;
    function SessionToken(const AValue: string): TSidekiqAwsV4Signer;

    procedure Sign(const ARequest: TSidekiqHttpRequest);
  end;

implementation

uses
  System.Hash,
  System.Classes,
  System.DateUtils,
  System.StrUtils,
  System.Net.URLClient;

{ TSidekiqAwsV4Signer }

function TSidekiqAwsV4Signer.AccessKey(
  const AValue: string): TSidekiqAwsV4Signer;
begin
  Result := Self;
  FAccessKey := AValue.Trim;
end;

class function TSidekiqAwsV4Signer.BuildCanonicalHeaders(
  const AHeaders: TDictionary<string, string>;
  out ASignedHeaders: string): string;
var
  LKeys: TArray<string>;
  LKey: string;
  LBuilder: TStringBuilder;
begin
  LKeys := AHeaders.Keys.ToArray;
  TArray.Sort<string>(LKeys);

  ASignedHeaders := LowerCase(string.Join(';', LKeys));
  LBuilder := TStringBuilder.Create;
  try
    for LKey in LKeys do
      LBuilder.Append(LowerCase(LKey))
        .Append(':')
        .Append(NormalizeHeaderValue(AHeaders.Items[LKey]))
        .Append(#10);

    Result := LBuilder.ToString + #10 + ASignedHeaders;
  finally
    LBuilder.Free;
  end;
end;

class function TSidekiqAwsV4Signer.BuildCanonicalQuery(
  const AQuery: string): string;
var
  LQuery: string;
  LParts: TArray<string>;
  LEncoded: TArray<string>;
  LPart: string;
  LPos: Integer;
  LIndex: Integer;
  LName: string;
  LValue: string;
begin
  Result := EmptyStr;
  LQuery := AQuery.Trim;
  if LQuery.StartsWith('?') then
    Delete(LQuery, 1, 1);

  if LQuery.IsEmpty then
    Exit;

  LParts := LQuery.Split(['&']);
  SetLength(LEncoded, Length(LParts));
  for LIndex := 0 to Pred(Length(LParts)) do
  begin
    LPart := LParts[LIndex];
    LPos := Pos('=', LPart);
    if LPos > 0 then
    begin
      LName := Copy(LPart, 1, LPos - 1);
      LValue := Copy(LPart, LPos + 1, MaxInt);
    end
    else
    begin
      LName := LPart;
      LValue := EmptyStr;
    end;

    LEncoded[LIndex] := PercentEncode(LName) + '=' + PercentEncode(LValue);
  end;

  TArray.Sort<string>(LEncoded);
  Result := string.Join('&', LEncoded);
end;

class function TSidekiqAwsV4Signer.BuildSigningKey(
  const ASecretKey, ADateStamp, ARegion, AService: string): TBytes;
var
  LDateKey: TBytes;
  LRegionKey: TBytes;
  LServiceKey: TBytes;
begin
  LDateKey := HmacSHA256(ADateStamp, 'AWS4' + ASecretKey);
  LRegionKey := HmacSHA256(ARegion, LDateKey);
  LServiceKey := HmacSHA256(AService, LRegionKey);
  Result := HmacSHA256('aws4_request', LServiceKey);
end;

constructor TSidekiqAwsV4Signer.Create;
begin
  inherited Create;
  FRegion := 'us-east-1';
  FService := 'sqs';
end;

class function TSidekiqAwsV4Signer.HashSHA256Hex(const AValue: string): string;
begin
  Result := THashSHA2.GetHashString(AValue, THashSHA2.TSHA2Version.SHA256);
end;

class function TSidekiqAwsV4Signer.HmacSHA256(
  const AData: string;
  const AKey: TBytes): TBytes;
begin
  Result := THashSHA2.GetHMACAsBytes(AData, AKey, THashSHA2.TSHA2Version.SHA256);
end;

class function TSidekiqAwsV4Signer.HmacSHA256(
  const AData, AKey: string): TBytes;
begin
  Result := THashSHA2.GetHMACAsBytes(AData, AKey, THashSHA2.TSHA2Version.SHA256);
end;

class function TSidekiqAwsV4Signer.New: TSidekiqAwsV4Signer;
begin
  Result := TSidekiqAwsV4Signer.Create;
end;

class function TSidekiqAwsV4Signer.NormalizeHeaderValue(
  const AValue: string): string;
var
  LChar: Char;
begin
  Result := AValue;
  for LChar in [#9, #10, #13] do
    Result := Result.Replace(LChar, ' ');
  Result := string.Join(' ', Result.Trim.Split([' '], TStringSplitOptions.ExcludeEmpty));
end;

class function TSidekiqAwsV4Signer.PercentEncode(const AValue: string): string;
const
  HexMap: array[0..15] of Char = ('0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'A', 'B', 'C', 'D', 'E', 'F');
var
  LBytes: TBytes;
  LByte: Byte;
begin
  Result := EmptyStr;
  if AValue.IsEmpty then
    Exit;

  LBytes := TEncoding.UTF8.GetBytes(AValue);
  for LByte in LBytes do
  begin
    if CharInSet(Char(LByte), ['A'..'Z', 'a'..'z', '0'..'9', '-', '_', '.', '~']) then
      Result := Result + Char(LByte)
    else
      Result := Result + '%' + HexMap[LByte shr 4] + HexMap[LByte and $0F];
  end;
end;

function TSidekiqAwsV4Signer.Region(const AValue: string): TSidekiqAwsV4Signer;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FRegion := AValue.Trim;
end;

function TSidekiqAwsV4Signer.SecretKey(
  const AValue: string): TSidekiqAwsV4Signer;
begin
  Result := Self;
  FSecretKey := AValue.Trim;
end;

function TSidekiqAwsV4Signer.Service(const AValue: string): TSidekiqAwsV4Signer;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FService := AValue.Trim;
end;

function TSidekiqAwsV4Signer.SessionToken(
  const AValue: string): TSidekiqAwsV4Signer;
begin
  Result := Self;
  FSessionToken := AValue.Trim;
end;

procedure TSidekiqAwsV4Signer.Sign(const ARequest: TSidekiqHttpRequest);
var
  LUri: TURI;
  LNowUtc: TDateTime;
  LAmzDate: string;
  LDateStamp: string;
  LCanonicalHeadersSource: TDictionary<string, string>;
  LCanonicalHeaders: string;
  LSignedHeaders: string;
  LPayloadHash: string;
  LCanonicalUri: string;
  LCanonicalRequest: string;
  LCredentialScope: string;
  LStringToSign: string;
  LSigningKey: TBytes;
  LSignature: string;
begin
  if FAccessKey.IsEmpty then
    raise Exception.Create('AWS access key not configured.');
  if FSecretKey.IsEmpty then
    raise Exception.Create('AWS secret key not configured.');

  LUri := TURI.Create(ARequest.RequestUrl);
  LNowUtc := TTimeZone.Local.ToUniversalTime(Now);
  LAmzDate := FormatDateTime('yyyymmdd"T"hhnnss"Z"', LNowUtc);
  LDateStamp := FormatDateTime('yyyymmdd', LNowUtc);
  LPayloadHash := HashSHA256Hex(ARequest.RequestBody);
  LCanonicalUri := IfThen(LUri.Path.IsEmpty, '/', LUri.Path);

  ARequest
    .Header('Host', LUri.Host)
    .Header('X-Amz-Date', LAmzDate)
    .Header('X-Amz-Content-Sha256', LPayloadHash);

  if not FSessionToken.IsEmpty then
    ARequest.Header('X-Amz-Security-Token', FSessionToken);

  LCanonicalHeadersSource := TDictionary<string, string>.Create;
  try
    LCanonicalHeadersSource.Add('content-type', ARequest.RequestContentType);
    LCanonicalHeadersSource.Add('host', LUri.Host);
    LCanonicalHeadersSource.Add('x-amz-content-sha256', LPayloadHash);
    LCanonicalHeadersSource.Add('x-amz-date', LAmzDate);
    if not FSessionToken.IsEmpty then
      LCanonicalHeadersSource.Add('x-amz-security-token', FSessionToken);

    LCanonicalHeaders := BuildCanonicalHeaders(LCanonicalHeadersSource, LSignedHeaders);
  finally
    LCanonicalHeadersSource.Free;
  end;

  LCanonicalRequest :=
    ARequest.HttpMethod + #10 +
    LCanonicalUri + #10 +
    BuildCanonicalQuery(LUri.Query) + #10 +
    LCanonicalHeaders + #10 +
    LPayloadHash;

  LCredentialScope := Format('%s/%s/%s/aws4_request', [LDateStamp, FRegion, FService]);
  LStringToSign :=
    ALGORITHM + #10 +
    LAmzDate + #10 +
    LCredentialScope + #10 +
    HashSHA256Hex(LCanonicalRequest);

  LSigningKey := BuildSigningKey(FSecretKey, LDateStamp, FRegion, FService);
  LSignature := THash.DigestAsString(HmacSHA256(LStringToSign, LSigningKey));

  ARequest.Header(
    'Authorization',
    Format(
      '%s Credential=%s/%s, SignedHeaders=%s, Signature=%s',
      [ALGORITHM, FAccessKey, LCredentialScope, LSignedHeaders, LSignature]));
end;

end.
