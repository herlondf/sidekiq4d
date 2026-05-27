program Hefesto.IntegrationSmoke;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.Net.HttpClient,
  System.Net.URLClient,
  System.NetEncoding,
  Winapi.ActiveX,    // CoInitialize / CoUninitialize — required for SQS XML parsing
  Xml.Win.msxmldom,  // registers MSXML as the default XML DOM vendor for SQS parsing
  FireDAC.Comp.Client,
  FireDAC.Stan.Async,   // required: registers async object factory for FireDAC
  FireDAC.Phys.PG,      // required: registers PostgreSQL driver
  FireDAC.UI.Intf,      // required: FireDAC UI interface
  FireDAC.Stan.Def,     // required: FireDAC standard definitions
  FireDAC.Stan.Option,  // required: FireDAC options
  FireDAC.DApt,         // required: FireDAC data adapter

  // Core
  Hefesto.Job in '..\src\Hefesto.Job.pas',
  Hefesto.Queue.Interfaces in '..\src\Hefesto.Queue.Interfaces.pas',
  Hefesto.Store.Interfaces in '..\src\Hefesto.Store.Interfaces.pas',

  // Redis client infrastructure
  Hefesto.Redis.Client in '..\src\adapters\Hefesto.Redis.Client.pas',
  Hefesto.Redis4D.Client in '..\src\adapters\Hefesto.Redis4D.Client.pas',

  // Store adapters
  Hefesto.Store.Redis4D in '..\src\adapters\Hefesto.Store.Redis4D.pas',
  Hefesto.Store.Redis in '..\src\adapters\Hefesto.Store.Redis.pas',
  Hefesto.Store.Postgres in '..\src\adapters\Hefesto.Store.Postgres.pas',
  Hefesto.Store.FireDAC in '..\src\adapters\Hefesto.Store.FireDAC.pas',
  Hefesto.Store.MongoDB in '..\src\adapters\Hefesto.Store.MongoDB.pas',

  // Queue adapters
  Hefesto.HTTP in '..\src\adapters\Hefesto.HTTP.pas',
  Hefesto.SQS.Client in '..\src\adapters\Hefesto.SQS.Client.pas',
  Hefesto.Queue.SQS in '..\src\adapters\Hefesto.Queue.SQS.pas',
  Hefesto.Queue.RabbitMQ in '..\src\adapters\Hefesto.Queue.RabbitMQ.pas',
  Hefesto.Queue.Kafka in '..\src\adapters\Hefesto.Queue.Kafka.pas',
  Hefesto.Queue.RedisStreams in '..\src\adapters\Hefesto.Queue.RedisStreams.pas';

var
  GPassed, GFailed: Integer;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

procedure RunTest(const AName: string; const AProc: TProc);
begin
  try
    AProc();
    Inc(GPassed);
    Writeln('  [PASS] ', AName);
  except
    on E: Exception do
    begin
      Inc(GFailed);
      Writeln('  [FAIL] ', AName, ' -- ', E.ClassName, ': ', E.Message);
    end;
  end;
end;

procedure RunSkip(const AName, AReason: string);
begin
  Writeln('  [SKIP] ', AName, ' -- ', AReason);
end;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

procedure HttpPost(const AURL, ABody, AContentType: string);
var
  LHttp: THTTPClient;
  LContent: TStringStream;
begin
  LHttp := THTTPClient.Create;
  try
    LHttp.ContentType := AContentType;
    LContent := TStringStream.Create(ABody, TEncoding.UTF8);
    try
      LHttp.Post(AURL, LContent);
    finally
      LContent.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

procedure HttpPut(const AURL, ABody, AContentType: string);
var
  LHttp: THTTPClient;
  LContent: TStringStream;
begin
  LHttp := THTTPClient.Create;
  try
    LHttp.ContentType := AContentType;
    LContent := TStringStream.Create(ABody, TEncoding.UTF8);
    try
      LHttp.Put(AURL, LContent);
    finally
      LContent.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

// Create LocalStack SQS queue via HTTP API; returns the queue URL.
function CreateLocalStackQueue(const AQueueName: string): string;
var
  LHttp: THTTPClient;
  LContent: TStringStream;
  LBody: string;
begin
  // LocalStack SQS endpoint — account ID is 000000000000 by default
  Result := Format('http://localhost:4566/000000000000/%s', [AQueueName]);
  LBody := Format('Action=CreateQueue&QueueName=%s&Version=2012-11-05', [AQueueName]);

  LHttp := THTTPClient.Create;
  try
    LHttp.ContentType := 'application/x-www-form-urlencoded';
    LContent := TStringStream.Create(LBody, TEncoding.UTF8);
    try
      LHttp.Post('http://localhost:4566/', LContent);
    finally
      LContent.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

// Create RabbitMQ queue via management API
procedure CreateRabbitMQQueue(const AQueueName: string);
var
  LHttp: THTTPClient;
  LContent: TStringStream;
  LCredentials: string;
begin
  LCredentials := TNetEncoding.Base64.Encode('sidekiq4d:sidekiq4d');
  LHttp := THTTPClient.Create;
  try
    LHttp.ContentType := 'application/json';
    LHttp.CustomHeaders['Authorization'] := 'Basic ' + LCredentials;
    LContent := TStringStream.Create('{"durable":false}', TEncoding.UTF8);
    try
      LHttp.Put(
        Format('http://localhost:15672/api/queues/%%2F/%s', [AQueueName]),
        LContent);
    finally
      LContent.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

// Create Kafka topic via REST Proxy admin
procedure CreateKafkaTopic(const ATopic: string);
var
  LHttp: THTTPClient;
  LContent: TStringStream;
  LBody: string;
begin
  LBody := Format(
    '{"topic_name":"%s","partitions_count":1,"replication_factor":1}',
    [ATopic]);
  LHttp := THTTPClient.Create;
  try
    LHttp.ContentType := 'application/vnd.kafka.v2+json';
    LContent := TStringStream.Create(LBody, TEncoding.UTF8);
    try
      LHttp.Post('http://localhost:8082/topics', LContent);
    finally
      LContent.Free;
    end;
  finally
    LHttp.Free;
  end;
end;

// Build a minimal publish request
function MakeRequest(const AQueue, AAction, ABody: string): THefestoPublishRequest;
begin
  Result.QueueName    := AQueue;
  Result.Action       := AAction;
  Result.Body         := ABody;
  Result.DelaySeconds := 0;
  SetLength(Result.Attributes, 0);
end;

function DefaultFetchOptions: THefestoFetchOptions;
begin
  Result.BatchSize        := 1;
  Result.WaitTimeSeconds  := 0;
  Result.VisibilityTimeout := 30;
end;

// ---------------------------------------------------------------------------
// Store tests
// ---------------------------------------------------------------------------

procedure TestRedisStore;
var
  LStore: IHefestoStateStore;
  LVal: string;
begin
  LStore := THefestoRedisStateStore.New
    .ConnectionString('redis://127.0.0.1:6379/0');

  LStore.Put('smoke:redis:key', 'hello-redis');
  LVal := LStore.Get('smoke:redis:key');
  if LVal <> 'hello-redis' then
    raise Exception.CreateFmt('Expected "hello-redis", got "%s"', [LVal]);

  LStore.Delete('smoke:redis:key');
  LVal := LStore.Get('smoke:redis:key');
  if LVal <> '' then
    raise Exception.CreateFmt('Expected empty after delete, got "%s"', [LVal]);
end;

procedure TestPostgresStore;
var
  LConn: TFDConnection;
  LStore: IHefestoStateStore;
  LVal: string;
begin
  LConn := THefestoFireDACBackend.NewPostgresConnection(
    'localhost', 5432, 'sidekiq4d', 'sidekiq4d', 'sidekiq4d');
  try
    LStore := THefestoPostgresStateStore.New
      .Backend(THefestoFireDACBackend.New(LConn))
      .TableName('smoke_state');

    LStore.Put('smoke:pg:key', 'hello-postgres');
    LVal := LStore.Get('smoke:pg:key');
    if LVal <> 'hello-postgres' then
      raise Exception.CreateFmt('Expected "hello-postgres", got "%s"', [LVal]);

    LStore.Delete('smoke:pg:key');
    LVal := LStore.Get('smoke:pg:key');
    if LVal <> '' then
      raise Exception.CreateFmt('Expected empty after delete, got "%s"', [LVal]);
  finally
    LConn.Free;
  end;
end;

procedure TestMongoDBStore;
var
  LStore: IHefestoStateStore;
  LVal: string;
begin
  // MongoDB Atlas Data API-compatible endpoint via mongod HTTP (port 27017)
  // The adapter uses /action/<action> suffix, which is the Atlas Data API format.
  // For a local mongod without HTTP interface this will fail — marked appropriately.
  LStore := THefestoMongoDBStateStore.New
    .BaseUrl('http://localhost:27017')
    .Database('sidekiq4d')
    .Collection('smoke_state');

  LStore.Put('smoke:mongo:key', 'hello-mongo');
  LVal := LStore.Get('smoke:mongo:key');
  if LVal <> 'hello-mongo' then
    raise Exception.CreateFmt('Expected "hello-mongo", got "%s"', [LVal]);

  LStore.Delete('smoke:mongo:key');
  LVal := LStore.Get('smoke:mongo:key');
  if LVal <> '' then
    raise Exception.CreateFmt('Expected empty after delete, got "%s"', [LVal]);
end;

// ---------------------------------------------------------------------------
// Queue tests
// ---------------------------------------------------------------------------

procedure TestSQSQueue;
const
  QueueName = 'smoke-queue';
var
  LConcrete: THefestoSqsQueueAdapter;
  LAdapter: IHefestoQueueAdapter;
  LPublisher: IHefestoQueuePublisher;
  LQueueUrl: string;
  LRequest: THefestoPublishRequest;
  LJobs: TArray<IHefestoJobEnvelope>;
begin
  LQueueUrl := CreateLocalStackQueue(QueueName);

  LConcrete := THefestoSqsQueueAdapter.New
    .QueueUrl(LQueueUrl)
    .AccessKey('test')
    .SecretKey('test')
    .Region('us-east-1');

  // Pin to interfaces so refcount manages lifetime — no manual Free needed
  LAdapter   := LConcrete;
  LPublisher := LConcrete;

  LRequest := MakeRequest(QueueName, 'smoke_action', '{"id":1}');
  LPublisher.Publish(LRequest);

  LJobs := LAdapter.Fetch(DefaultFetchOptions);
  if Length(LJobs) < 1 then
    raise Exception.Create('Expected at least 1 job from SQS, got 0');

  LAdapter.Ack(LJobs[0]);
end;

procedure TestRabbitMQQueue;
const
  QueueName = 'smoke-queue';
var
  LConcrete: THefestoRabbitMQAdapter;
  LAdapter: IHefestoQueueAdapter;
  LPublisher: IHefestoQueuePublisher;
  LRequest: THefestoPublishRequest;
  LJobs: TArray<IHefestoJobEnvelope>;
begin
  CreateRabbitMQQueue(QueueName);

  LConcrete := THefestoRabbitMQAdapter.New
    .Host('localhost')
    .Port(15672)
    .VHost('/')
    .Queue(QueueName)
    .Username('sidekiq4d')
    .Password('sidekiq4d');

  LAdapter   := LConcrete;
  LPublisher := LConcrete;

  LRequest := MakeRequest(QueueName, 'smoke_action', '{"id":1}');
  LPublisher.Publish(LRequest);

  LJobs := LAdapter.Fetch(DefaultFetchOptions);
  if Length(LJobs) < 1 then
    raise Exception.Create('Expected at least 1 job from RabbitMQ, got 0');

  LAdapter.Ack(LJobs[0]);
end;

procedure TestKafkaQueue;
const
  TopicName = 'smoke-topic';
var
  LConcrete: THefestoKafkaAdapter;
  LAdapter: IHefestoQueueAdapter;
  LPublisher: IHefestoQueuePublisher;
  LRequest: THefestoPublishRequest;
  LJobs: TArray<IHefestoJobEnvelope>;
  LFetchOpts: THefestoFetchOptions;
begin
  try
    CreateKafkaTopic(TopicName);
  except
    // Ignore — topic may already exist or REST Proxy auto-creates it
  end;

  LConcrete := THefestoKafkaAdapter.New
    .BaseUrl('http://localhost:8082')
    .Topic(TopicName)
    .GroupId('smoke-group')
    .InstanceId('smoke-instance-1');

  LAdapter   := LConcrete;
  LPublisher := LConcrete;

  LFetchOpts.BatchSize         := 1;
  LFetchOpts.WaitTimeSeconds   := 1;
  LFetchOpts.VisibilityTimeout := 0;

  // Subscribe FIRST so the consumer joins before any message is published.
  // Publishing before the consumer subscribes causes it to miss the message
  // because the group starts at the committed (or latest) offset.
  LAdapter.Fetch(LFetchOpts);
  Sleep(5000); // wait for partition assignment / group rebalance

  LRequest := MakeRequest(TopicName, 'smoke_action', '{"id":1}');
  LPublisher.Publish(LRequest);
  Sleep(1000); // allow broker to make the new message available

  LFetchOpts.WaitTimeSeconds := 5;
  LJobs := LAdapter.Fetch(LFetchOpts);
  if Length(LJobs) < 1 then
    raise Exception.Create('Expected at least 1 job from Kafka, got 0');

  LAdapter.Ack(LJobs[0]);
end;

procedure TestRedisStreamsQueue;
const
  StreamKey    = 'smoke:stream';
  GroupName    = 'smoke-group';
  ConsumerName = 'smoke-consumer-1';
var
  LConcrete: THefestoRedisStreamsAdapter;
  LAdapter: IHefestoQueueAdapter;
  LPublisher: IHefestoQueuePublisher;
  LRequest: THefestoPublishRequest;
  LJobs: TArray<IHefestoJobEnvelope>;
begin
  LConcrete := THefestoRedisStreamsAdapter.New
    .ConnectionString('redis://127.0.0.1:6379/0')
    .StreamKey(StreamKey)
    .GroupName(GroupName)
    .ConsumerName(ConsumerName);

  LAdapter   := LConcrete;
  LPublisher := LConcrete;

  LRequest := MakeRequest(StreamKey, 'smoke_action', '{"id":1}');
  LPublisher.Publish(LRequest);

  LJobs := LAdapter.Fetch(DefaultFetchOptions);
  if Length(LJobs) < 1 then
    raise Exception.Create('Expected at least 1 job from Redis Streams, got 0');

  LAdapter.Ack(LJobs[0]);
end;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

begin
  CoInitialize(nil);
  try
  ReportMemoryLeaksOnShutdown := False;
  GPassed := 0;
  GFailed := 0;

  Writeln('Hefesto.IntegrationSmoke');
  Writeln('==========================');
  Flush(Output);

  Writeln('');
  Writeln('--- State Stores ---');
  RunTest('Redis Store (put/get/delete)', TestRedisStore);
  RunTest('PostgreSQL Store via FireDAC (put/get/delete)', TestPostgresStore);
  RunSkip('MongoDB Store via Atlas Data API', 'Atlas REST API não compatível com mongod local TCP');

  Writeln('');
  Writeln('--- Queue Adapters ---');
  RunTest('SQS (LocalStack) — enqueue + fetch + ack', TestSQSQueue);
  RunTest('RabbitMQ (HTTP Management API) — publish + fetch + ack', TestRabbitMQQueue);
  RunTest('Kafka REST Proxy — publish + fetch + ack', TestKafkaQueue);
  RunTest('Redis Streams — publish + fetch + ack', TestRedisStreamsQueue);

  Writeln('');
  RunSkip('Google Pub/Sub', 'gRPC-only emulator, not compatible with REST adapter');

  Writeln('');
  Writeln(Format('Passed: %d  Failed: %d', [GPassed, GFailed]));

  if GFailed > 0 then
    Halt(1);
  finally
    CoUninitialize;
  end;
end.
