unit Hefesto.HTTP;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections;

type
  THefestoHttpHeaders = TDictionary<string, string>;

  THefestoHttpRequest = class
  private
    FMethod: string;
    FUrl: string;
    FContentType: string;
    FBody: string;
    FHeaders: THefestoHttpHeaders;
  public
    constructor Create;
    destructor Destroy; override;

    function Method(const AValue: string): THefestoHttpRequest;
    function Url(const AValue: string): THefestoHttpRequest;
    function ContentType(const AValue: string): THefestoHttpRequest;
    function Body(const AValue: string): THefestoHttpRequest;
    function Header(const AName, AValue: string): THefestoHttpRequest;

    property HttpMethod: string read FMethod;
    property RequestUrl: string read FUrl;
    property RequestContentType: string read FContentType;
    property RequestBody: string read FBody;
    property Headers: THefestoHttpHeaders read FHeaders;
  end;

  THefestoHttpResponse = class
  private
    FStatusCode: Integer;
    FBody: string;
    FHeaders: THefestoHttpHeaders;
  public
    constructor Create;
    destructor Destroy; override;

    function StatusCode(const AValue: Integer): THefestoHttpResponse;
    function Body(const AValue: string): THefestoHttpResponse;
    function Header(const AName, AValue: string): THefestoHttpResponse;
    function HeaderValue(const AName: string): string;

    property ResponseStatusCode: Integer read FStatusCode;
    property ResponseBody: string read FBody;
    property Headers: THefestoHttpHeaders read FHeaders;
  end;

  THefestoHttpClient = class
  public
    class function Send(const ARequest: THefestoHttpRequest): THefestoHttpResponse; static;
  end;

implementation

uses
  System.Net.URLClient,
  System.Net.HttpClient;

{ THefestoHttpRequest }

function THefestoHttpRequest.Body(const AValue: string): THefestoHttpRequest;
begin
  Result := Self;
  FBody := AValue;
end;

function THefestoHttpRequest.ContentType(
  const AValue: string): THefestoHttpRequest;
begin
  Result := Self;
  FContentType := AValue;
end;

constructor THefestoHttpRequest.Create;
begin
  inherited Create;
  FHeaders := THefestoHttpHeaders.Create;
  FMethod := 'POST';
  FContentType := 'application/x-www-form-urlencoded; charset=utf-8';
end;

destructor THefestoHttpRequest.Destroy;
begin
  FHeaders.Free;
  inherited;
end;

function THefestoHttpRequest.Header(
  const AName, AValue: string): THefestoHttpRequest;
begin
  Result := Self;
  FHeaders.AddOrSetValue(AName, AValue);
end;

function THefestoHttpRequest.Method(const AValue: string): THefestoHttpRequest;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FMethod := AValue.Trim.ToUpper;
end;

function THefestoHttpRequest.Url(const AValue: string): THefestoHttpRequest;
begin
  Result := Self;
  FUrl := AValue.Trim;
end;

{ THefestoHttpResponse }

function THefestoHttpResponse.Body(const AValue: string): THefestoHttpResponse;
begin
  Result := Self;
  FBody := AValue;
end;

constructor THefestoHttpResponse.Create;
begin
  inherited Create;
  FHeaders := THefestoHttpHeaders.Create;
end;

destructor THefestoHttpResponse.Destroy;
begin
  FHeaders.Free;
  inherited;
end;

function THefestoHttpResponse.Header(
  const AName, AValue: string): THefestoHttpResponse;
begin
  Result := Self;
  FHeaders.AddOrSetValue(AName, AValue);
end;

function THefestoHttpResponse.HeaderValue(const AName: string): string;
begin
  if not FHeaders.TryGetValue(AName, Result) then
    Result := EmptyStr;
end;

function THefestoHttpResponse.StatusCode(
  const AValue: Integer): THefestoHttpResponse;
begin
  Result := Self;
  FStatusCode := AValue;
end;

{ THefestoHttpClient }

class function THefestoHttpClient.Send(
  const ARequest: THefestoHttpRequest): THefestoHttpResponse;
var
  LClient: THTTPClient;
  LBody: TStringStream;
  LResponse: IHTTPResponse;
  LHeaders: TNetHeaders;
  LRequestHeader: TPair<string, string>;
  LResponseHeader: TNameValuePair;
  LIndex: Integer;
begin
  Result := THefestoHttpResponse.Create;
  LClient := THTTPClient.Create;
  LClient.ConnectionTimeout := 5000;   // 5s to establish connection
  LClient.ResponseTimeout   := 30000;  // 30s to receive response
  try
    SetLength(LHeaders, ARequest.Headers.Count + 1);
    LHeaders[0].Name := 'Content-Type';
    LHeaders[0].Value := ARequest.RequestContentType;

    LIndex := 1;
    for LRequestHeader in ARequest.Headers do
    begin
      LHeaders[LIndex].Name := LRequestHeader.Key;
      LHeaders[LIndex].Value := LRequestHeader.Value;
      Inc(LIndex);
    end;

    LBody := TStringStream.Create(ARequest.RequestBody, TEncoding.UTF8);
    try
      if SameText(ARequest.HttpMethod, 'POST') then
        LResponse := LClient.Post(ARequest.RequestUrl, LBody, nil, LHeaders)
      else
        raise Exception.CreateFmt('HTTP method %s not supported.', [ARequest.HttpMethod]);
    finally
      LBody.Free;
    end;

    Result
      .StatusCode(LResponse.StatusCode)
      .Body(LResponse.ContentAsString(TEncoding.UTF8));

    for LResponseHeader in LResponse.Headers do
      Result.Header(LResponseHeader.Name, LResponseHeader.Value);
  finally
    LClient.Free;
  end;
end;

end.
