program Sidekiq4D.RedisBenchmark;

// Benchmark: individual commands vs Redis pipeline
// Fase 1 (ANTES): usa Redis4D diretamente
// Fase 2 (DEPOIS): usa ISidekiqRedisClient.BeginPipeline (abstração Sidekiq4D)
//
// Roda contra Redis local em 127.0.0.1:6379 db=15.

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Math,
  System.DateUtils,
  System.Diagnostics,
  Sidekiq4D.Redis.Client,
  Sidekiq4D.Redis4D.Client,
  Redis4D.Client,
  Redis4D.Interfaces,
  Redis4D.Core.RESP,
  Redis4D.Core.Types,
  Redis4D.Net.Interfaces,
  Redis4D.Net.Synapse,
  Redis4D.Features.Interfaces,
  Redis4D.Features.Pipeline,
  Redis4D.Features.Pool,
  Redis4D.Features.Health;

const
  REDIS_HOST = '127.0.0.1';
  REDIS_PORT = 6379;
  BENCH_KEY_LPUSH = 'bench:sidekiq4d:lpush';
  BENCH_KEY_ZADD  = 'bench:sidekiq4d:zadd';
  BENCH_KEY_LLEN  = 'bench:sidekiq4d:';

function MakeClient: IRedis4DClient;
begin
  Result := TRedis4DClient.NewWithPassword(REDIS_HOST, REDIS_PORT, nil, '', 15)
    .ConnectTimeout(3000)
    .ReadTimeout(10000);
end;

procedure PrintHeader(const ATitle: string);
begin
  Writeln;
  Writeln('=== ' + ATitle + ' ===');
  Writeln(Format('  %-42s | %6s | %7s | %12s',
    ['Cenario', 'N', 'ms', 'ops/s']));
  Writeln(StringOfChar('-', 78));
end;

procedure PrintMetric(
  const ALabel: string;
  const ACount: Integer;
  const AElapsedMs: Int64);
var
  LOpsPerSec: Double;
begin
  if AElapsedMs > 0 then
    LOpsPerSec := ACount / (AElapsedMs / 1000.0)
  else
    LOpsPerSec := ACount * 1000.0;
  Writeln(Format('  %-42s | %6d | %7d | %12.0f',
    [ALabel, ACount, AElapsedMs, LOpsPerSec]));
end;

// ---------------------------------------------------------------------------
// CENARIO 1: Publish N jobs — LPUSH individual vs pipeline
// ---------------------------------------------------------------------------

procedure BenchLPushIndividual(
  const AClient: IRedis4DClient;
  const ACount: Integer);
var
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  AClient.Execute(['DEL', BENCH_KEY_LPUSH]);
  LWatch := TStopwatch.StartNew;
  for LIndex := 1 to ACount do
    AClient.LPush(BENCH_KEY_LPUSH, ['{"id":' + IntToStr(LIndex) + ',"action":"email"}']);
  LWatch.Stop;
  AClient.Execute(['DEL', BENCH_KEY_LPUSH]);
  PrintMetric('LPUSH individual (1 round-trip / job)', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchLPushPipeline(
  const AClient: IRedis4DClient;
  const ACount: Integer);
var
  LCommands: TArray<TArray<string>>;
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  AClient.Execute(['DEL', BENCH_KEY_LPUSH]);
  SetLength(LCommands, ACount);
  for LIndex := 0 to Pred(ACount) do
    LCommands[LIndex] := ['LPUSH', BENCH_KEY_LPUSH,
      '{"id":' + IntToStr(LIndex + 1) + ',"action":"email"}'];
  LWatch := TStopwatch.StartNew;
  AClient.ExecutePipeline(LCommands);
  LWatch.Stop;
  AClient.Execute(['DEL', BENCH_KEY_LPUSH]);
  PrintMetric('LPUSH pipeline  (1 round-trip total)', ACount, LWatch.ElapsedMilliseconds);
end;

// ---------------------------------------------------------------------------
// CENARIO 2: Schedule N jobs — ZADD individual vs pipeline
// ---------------------------------------------------------------------------

procedure BenchZAddIndividual(
  const AClient: IRedis4DClient;
  const ACount: Integer);
var
  LIndex: Integer;
  LWatch: TStopwatch;
  LScore: Int64;
begin
  AClient.Execute(['DEL', BENCH_KEY_ZADD]);
  LScore := DateTimeToUnix(Now, False);
  LWatch := TStopwatch.StartNew;
  for LIndex := 1 to ACount do
    AClient.ZAdd(BENCH_KEY_ZADD, LScore + LIndex,
      'job:' + IntToStr(LIndex) + ':email:{}');
  LWatch.Stop;
  AClient.Execute(['DEL', BENCH_KEY_ZADD]);
  PrintMetric('ZADD individual (1 round-trip / job)', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchZAddPipeline(
  const AClient: IRedis4DClient;
  const ACount: Integer);
var
  LCommands: TArray<TArray<string>>;
  LIndex: Integer;
  LWatch: TStopwatch;
  LScore: Int64;
begin
  AClient.Execute(['DEL', BENCH_KEY_ZADD]);
  LScore := DateTimeToUnix(Now, False);
  SetLength(LCommands, ACount);
  for LIndex := 0 to Pred(ACount) do
    LCommands[LIndex] := ['ZADD', BENCH_KEY_ZADD,
      IntToStr(LScore + LIndex + 1),
      'job:' + IntToStr(LIndex + 1) + ':email:{}'];
  LWatch := TStopwatch.StartNew;
  AClient.ExecutePipeline(LCommands);
  LWatch.Stop;
  AClient.Execute(['DEL', BENCH_KEY_ZADD]);
  PrintMetric('ZADD pipeline   (1 round-trip total)', ACount, LWatch.ElapsedMilliseconds);
end;

// ---------------------------------------------------------------------------
// CENARIO 3: Queue monitoring — N LLEN individual vs pipeline
// ---------------------------------------------------------------------------

procedure BenchLLenIndividual(
  const AClient: IRedis4DClient;
  const ACount: Integer);
var
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  LWatch := TStopwatch.StartNew;
  for LIndex := 1 to ACount do
    AClient.LLen(BENCH_KEY_LLEN + 'queue' + IntToStr(LIndex));
  LWatch.Stop;
  PrintMetric('LLEN individual (1 round-trip / fila)', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchLLenPipeline(
  const AClient: IRedis4DClient;
  const ACount: Integer);
var
  LCommands: TArray<TArray<string>>;
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  SetLength(LCommands, ACount);
  for LIndex := 0 to Pred(ACount) do
    LCommands[LIndex] := ['LLEN', BENCH_KEY_LLEN + 'queue' + IntToStr(LIndex + 1)];
  LWatch := TStopwatch.StartNew;
  AClient.ExecutePipeline(LCommands);
  LWatch.Stop;
  PrintMetric('LLEN pipeline   (1 round-trip total)', ACount, LWatch.ElapsedMilliseconds);
end;

// ---------------------------------------------------------------------------
// DEPOIS: usando ISidekiqRedisClient.BeginPipeline (abstração Sidekiq4D)
// ---------------------------------------------------------------------------

procedure BenchSidekiqLPushIndividual(
  const AClient: ISidekiqRedisClient;
  const ACount: Integer);
var
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  AClient.Del(BENCH_KEY_LPUSH);
  LWatch := TStopwatch.StartNew;
  for LIndex := 1 to ACount do
    AClient.RPush(BENCH_KEY_LPUSH,
      ['{"id":' + IntToStr(LIndex) + ',"action":"email"}']);
  LWatch.Stop;
  AClient.Del(BENCH_KEY_LPUSH);
  PrintMetric('[Sidekiq4D] RPush individual', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchSidekiqLPushPipeline(
  const AClient: ISidekiqRedisClient;
  const ACount: Integer);
var
  LPipeline: ISidekiqRedisPipeline;
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  AClient.Del(BENCH_KEY_LPUSH);
  LPipeline := AClient.BeginPipeline;
  for LIndex := 1 to ACount do
    LPipeline.LPush(BENCH_KEY_LPUSH,
      '{"id":' + IntToStr(LIndex) + ',"action":"email"}');
  LWatch := TStopwatch.StartNew;
  LPipeline.Execute;
  LWatch.Stop;
  AClient.Del(BENCH_KEY_LPUSH);
  PrintMetric('[Sidekiq4D] LPush pipeline (BeginPipeline)', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchSidekiqZAddPipeline(
  const AClient: ISidekiqRedisClient;
  const ACount: Integer);
var
  LPipeline: ISidekiqRedisPipeline;
  LIndex: Integer;
  LWatch: TStopwatch;
  LScore: Int64;
begin
  AClient.Del(BENCH_KEY_ZADD);
  LScore := DateTimeToUnix(Now, False);
  LPipeline := AClient.BeginPipeline;
  for LIndex := 1 to ACount do
    LPipeline.ZAdd(BENCH_KEY_ZADD, LScore + LIndex,
      'job:' + IntToStr(LIndex) + ':email:{}');
  LWatch := TStopwatch.StartNew;
  LPipeline.Execute;
  LWatch.Stop;
  AClient.Del(BENCH_KEY_ZADD);
  PrintMetric('[Sidekiq4D] ZAdd pipeline (BeginPipeline)', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchSidekiqLLenBatch(
  const AClient: ISidekiqRedisClient;
  const ACount: Integer);
var
  LKeys: TArray<string>;
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  SetLength(LKeys, ACount);
  for LIndex := 0 to Pred(ACount) do
    LKeys[LIndex] := BENCH_KEY_LLEN + 'queue' + IntToStr(LIndex + 1);
  LWatch := TStopwatch.StartNew;
  AClient.LLenBatch(LKeys);
  LWatch.Stop;
  PrintMetric('[Sidekiq4D] LLenBatch (pipeline)', ACount, LWatch.ElapsedMilliseconds);
end;

procedure BenchSidekiqPooledLPushPipeline(ACount: Integer);
var
  LClient: ISidekiqRedisClient;
  LPipeline: ISidekiqRedisPipeline;
  LIndex: Integer;
  LWatch: TStopwatch;
begin
  LClient := TRedis4DPooledClientBridge.NewWithPool(
    'redis://' + REDIS_HOST + ':' + IntToStr(REDIS_PORT) + '/15', 4, 2);
  LClient.Del(BENCH_KEY_LPUSH);
  LPipeline := LClient.BeginPipeline;
  for LIndex := 1 to ACount do
    LPipeline.LPush(BENCH_KEY_LPUSH,
      '{"id":' + IntToStr(LIndex) + ',"action":"email"}');
  LWatch := TStopwatch.StartNew;
  LPipeline.Execute;
  LWatch.Stop;
  LClient.Del(BENCH_KEY_LPUSH);
  PrintMetric('[Sidekiq4D+Pool] LPush pipeline', ACount, LWatch.ElapsedMilliseconds);
end;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

procedure RunAllBenchmarks;
var
  LClient: IRedis4DClient;
  LSidekiqClient: ISidekiqRedisClient;
  LN: Integer;
  LCounts: array[0..2] of Integer;
begin
  LClient := MakeClient;
  LSidekiqClient := TRedis4DClientBridge.NewFromConnectionString(
    'redis://' + REDIS_HOST + ':' + IntToStr(REDIS_PORT) + '/15');
  LCounts[0] := 100;
  LCounts[1] := 1000;
  LCounts[2] := 5000;

  // CENARIO 1: Publish Jobs (LPUSH)
  PrintHeader('Cenario 1: Publish Jobs (LPUSH) — db 15');
  for LN in LCounts do
  begin
    BenchLPushIndividual(LClient, LN);
    BenchLPushPipeline(LClient, LN);
    BenchSidekiqLPushIndividual(LSidekiqClient, LN);
    BenchSidekiqLPushPipeline(LSidekiqClient, LN);
    BenchSidekiqPooledLPushPipeline(LN);
    Writeln;
  end;

  // CENARIO 2: Schedule Jobs (ZADD)
  PrintHeader('Cenario 2: Schedule Jobs (ZADD) — db 15');
  for LN in LCounts do
  begin
    BenchZAddIndividual(LClient, LN);
    BenchZAddPipeline(LClient, LN);
    BenchSidekiqZAddPipeline(LSidekiqClient, LN);
    Writeln;
  end;

  // CENARIO 3: Queue Monitoring (N filas)
  PrintHeader('Cenario 3: Queue Monitoring (LLEN N filas)');
  for LN in [5, 10, 20] do
  begin
    BenchLLenIndividual(LClient, LN);
    BenchLLenPipeline(LClient, LN);
    BenchSidekiqLLenBatch(LSidekiqClient, LN);
    Writeln;
  end;
end;

begin
  try
    Writeln('Sidekiq4D Redis Benchmark — ANTES e DEPOIS da implementacao de pipeline');
    Writeln('Redis: ' + REDIS_HOST + ':' + IntToStr(REDIS_PORT) + ' (db 15)');
    Writeln(DateTimeToStr(Now));
    RunAllBenchmarks;
    Writeln;
    Writeln('Benchmark concluido.');
  except
    on E: Exception do
    begin
      Writeln('ERRO: ' + E.ClassName + ': ' + E.Message);
      Halt(1);
    end;
  end;
end.
