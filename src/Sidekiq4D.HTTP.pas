unit Sidekiq4D.HTTP;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections;

type
  TSidekiqHttpHeaders = TDictionary<string, string>;

  TSidekiqHttpRequest = class
  private
    FMethod: string;
    FUrl: string;
    FContentType: string;
    FBody: string;
    FHeaders: TSidekiqHttpHeaders;
  public
    constructor Create;
    destructor Destroy; override;

    function Method(const AValue: string): TSidekiqHttpRequest;
    function Url(const AValue: string): TSidekiqHttpRequest;
    function ContentType(const AValue: string): TSidekiqHttpRequest;
    function Body(const AValue: string): TSidekiqHttpRequest;
    function Header(const AName, AValue: string): TSidekiqHttpRequest;

    property HttpMethod: string read FMethod;
    property RequestUrl: string read FUrl;
    property RequestContentType: string read FContentType;
    property RequestBody: string read FBody;
    property Headers: TSidekiqHttpHeaders read FHeaders;
  end;

  TSidekiqHttpResponse = class
  private
    FStatusCode: Integer;
    FBody: string;
    FHeaders: TSidekiqHttpHeaders;
  public
    constructor Create;
    destructor Destroy; override;

    function StatusCode(const AValue: Integer): TSidekiqHttpResponse;
    function Body(const AValue: string): TSidekiqHttpResponse;
    function Header(const AName, AValue: string): TSidekiqHttpResponse;
    function HeaderValue(const AName: string): string;

    property ResponseStatusCode: Integer read FStatusCode;
    property ResponseBody: string read FBody;
    property Headers: TSidekiqHttpHeaders read FHeaders;
  end;

  TSidekiqHttpClient = class
  public
    class function Send(const ARequest: TSidekiqHttpRequest): TSidekiqHttpResponse; static;
  end;

implementation

uses
  System.Net.URLClient,
  System.Net.HttpClient;

{ TSidekiqHttpRequest }

function TSidekiqHttpRequest.Body(const AValue: string): TSidekiqHttpRequest;
begin
  Result := Self;
  FBody := AValue;
end;

function TSidekiqHttpRequest.ContentType(
  const AValue: string): TSidekiqHttpRequest;
begin
  Result := Self;
  FContentType := AValue;
end;

constructor TSidekiqHttpRequest.Create;
begin
  inherited Create;
  FHeaders := TSidekiqHttpHeaders.Create;
  FMethod := 'POST';
  FContentType := 'application/x-www-form-urlencoded; charset=utf-8';
end;

destructor TSidekiqHttpRequest.Destroy;
begin
  FHeaders.Free;
  inherited;
end;

function TSidekiqHttpRequest.Header(
  const AName, AValue: string): TSidekiqHttpRequest;
begin
  Result := Self;
  FHeaders.AddOrSetValue(AName, AValue);
end;

function TSidekiqHttpRequest.Method(const AValue: string): TSidekiqHttpRequest;
begin
  Result := Self;
  if not AValue.Trim.IsEmpty then
    FMethod := AValue.Trim.ToUpper;
end;

function TSidekiqHttpRequest.Url(const AValue: string): TSidekiqHttpRequest;
begin
  Result := Self;
  FUrl := AValue.Trim;
end;

{ TSidekiqHttpResponse }

function TSidekiqHttpResponse.Body(const AValue: string): TSidekiqHttpResponse;
begin
  Result := Self;
  FBody := AValue;
end;

constructor TSidekiqHttpResponse.Create;
begin
  inherited Create;
  FHeaders := TSidekiqHttpHeaders.Create;
end;

destructor TSidekiqHttpResponse.Destroy;
begin
  FHeaders.Free;
  inherited;
end;

function TSidekiqHttpResponse.Header(
  const AName, AValue: string): TSidekiqHttpResponse;
begin
  Result := Self;
  FHeaders.AddOrSetValue(AName, AValue);
end;

function TSidekiqHttpResponse.HeaderValue(const AName: string): string;
begin
  if not FHeaders.TryGetValue(AName, Result) then
    Result := EmptyStr;
end;

function TSidekiqHttpResponse.StatusCode(
  const AValue: Integer): TSidekiqHttpResponse;
begin
  Result := Self;
  FStatusCode := AValue;
end;

{ TSidekiqHttpClient }

class function TSidekiqHttpClient.Send(
  const ARequest: TSidekiqHttpRequest): TSidekiqHttpResponse;
var
  LClient: THTTPClient;
  LBody: TStringStream;
  LResponse: IHTTPResponse;
  LHeaders: TNetHeaders;
  LRequestHeader: TPair<string, string>;
  LResponseHeader: TNameValuePair;
  LIndex: Integer;
begin
  Result := TSidekiqHttpResponse.Create;
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
