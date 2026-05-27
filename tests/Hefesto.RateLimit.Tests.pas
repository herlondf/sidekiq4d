unit Hefesto.RateLimit.Tests;

{
  Testes unitários para THefestoNoopRateLimiter e THefestoTokenBucketRateLimiter.
  Cobre: bucket cheio, esgotamento, RetryAfter, e validações de parâmetros.
}

interface

uses
  DUnitX.TestFramework,
  Hefesto.Store.InMemory,
  Hefesto.Store.Interfaces,
  Hefesto.RateLimit;

type
  [TestFixture('NoopRateLimiter')]
  TNoopRateLimiterTests = class
  public
    [Test]
    [Category('Unit')]
    procedure TryAcquire_AlwaysReturnsTrue;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_RetryAfterIsAlwaysZero;
  end;

  [TestFixture('TokenBucketRateLimiter')]
  TTokenBucketRateLimiterTests = class
  private
    FStore: IHefestoStateStore;
    FLimiter: IHefestoRateLimiter;
  public
    [Setup]
    procedure Setup;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_FreshBucket_Succeeds;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_CapacityOne_SecondCallFails;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_CostEqualCapacity_ConsumesEntireBucket;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_WhenFails_ReturnsPositiveRetryAfter;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_DifferentKeys_IndependentBuckets;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_EmptyKey_RaisesException;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_ZeroCapacity_RaisesException;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_ZeroInterval_RaisesException;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_ZeroCost_RaisesException;

    [Test]
    [Category('Unit')]
    procedure TryAcquire_CostGreaterThanCapacity_RaisesException;
  end;

implementation

uses
  System.SysUtils;

{ TNoopRateLimiterTests }

procedure TNoopRateLimiterTests.TryAcquire_AlwaysReturnsTrue;
var
  LLimiter: IHefestoRateLimiter;
  LRetryAfter: Integer;
begin
  LLimiter := THefestoNoopRateLimiter.New;
  Assert.IsTrue(LLimiter.TryAcquire('any-key', 1, 60, 1, LRetryAfter),
    'NoopRateLimiter deve sempre retornar True');
end;

procedure TNoopRateLimiterTests.TryAcquire_RetryAfterIsAlwaysZero;
var
  LLimiter: IHefestoRateLimiter;
  LRetryAfter: Integer;
begin
  LLimiter := THefestoNoopRateLimiter.New;
  LLimiter.TryAcquire('any-key', 1, 60, 1, LRetryAfter);
  Assert.AreEqual(0, LRetryAfter,
    'NoopRateLimiter deve retornar RetryAfter=0 sempre');
end;

{ TTokenBucketRateLimiterTests }

procedure TTokenBucketRateLimiterTests.Setup;
begin
  FStore := THefestoInMemoryStateStore.New;
  FLimiter := THefestoTokenBucketRateLimiter.New(FStore);
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_FreshBucket_Succeeds;
var
  LRetryAfter: Integer;
begin
  Assert.IsTrue(FLimiter.TryAcquire('api', 10, 60, 1, LRetryAfter),
    'Bucket cheio deve permitir a primeira requisição');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_CapacityOne_SecondCallFails;
var
  LRetryAfter: Integer;
begin
  FLimiter.TryAcquire('api-single', 1, 3600, 1, LRetryAfter);
  Assert.IsFalse(FLimiter.TryAcquire('api-single', 1, 3600, 1, LRetryAfter),
    'Bucket com capacity=1 deve falhar na segunda chamada imediata');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_CostEqualCapacity_ConsumesEntireBucket;
var
  LRetryAfter: Integer;
begin
  Assert.IsTrue(FLimiter.TryAcquire('api-full', 5, 60, 5, LRetryAfter),
    'Cost = Capacity deve ser permitido (consome todo o bucket de uma vez)');
  Assert.IsFalse(FLimiter.TryAcquire('api-full', 5, 60, 5, LRetryAfter),
    'Após esgotar o bucket, a próxima chamada deve falhar');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_WhenFails_ReturnsPositiveRetryAfter;
var
  LRetryAfter: Integer;
begin
  FLimiter.TryAcquire('api-retry', 1, 3600, 1, LRetryAfter);
  FLimiter.TryAcquire('api-retry', 1, 3600, 1, LRetryAfter);
  Assert.IsTrue(LRetryAfter > 0,
    'Quando rate limit é atingido, RetryAfterSeconds deve ser > 0');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_DifferentKeys_IndependentBuckets;
var
  LRetryAfter: Integer;
begin
  FLimiter.TryAcquire('key-x', 1, 3600, 1, LRetryAfter);
  Assert.IsTrue(FLimiter.TryAcquire('key-y', 1, 3600, 1, LRetryAfter),
    'Chaves diferentes devem ter buckets independentes');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_EmptyKey_RaisesException;
begin
  Assert.WillRaise(
    procedure
    var LR: Integer;
    begin
      FLimiter.TryAcquire('', 10, 60, 1, LR);
    end,
    EHefestoRateLimiter,
    'Chave vazia deve levantar EHefestoRateLimiter');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_ZeroCapacity_RaisesException;
begin
  Assert.WillRaise(
    procedure
    var LR: Integer;
    begin
      FLimiter.TryAcquire('key', 0, 60, 1, LR);
    end,
    EHefestoRateLimiter,
    'Capacity=0 deve levantar EHefestoRateLimiter');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_ZeroInterval_RaisesException;
begin
  Assert.WillRaise(
    procedure
    var LR: Integer;
    begin
      FLimiter.TryAcquire('key', 10, 0, 1, LR);
    end,
    EHefestoRateLimiter,
    'Interval=0 deve levantar EHefestoRateLimiter');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_ZeroCost_RaisesException;
begin
  Assert.WillRaise(
    procedure
    var LR: Integer;
    begin
      FLimiter.TryAcquire('key', 10, 60, 0, LR);
    end,
    EHefestoRateLimiter,
    'Cost=0 deve levantar EHefestoRateLimiter');
end;

procedure TTokenBucketRateLimiterTests.TryAcquire_CostGreaterThanCapacity_RaisesException;
begin
  Assert.WillRaise(
    procedure
    var LR: Integer;
    begin
      FLimiter.TryAcquire('key', 5, 60, 10, LR);
    end,
    EHefestoRateLimiter,
    'Cost > Capacity deve levantar EHefestoRateLimiter');
end;

initialization
  TDUnitX.RegisterTestFixture(TNoopRateLimiterTests);
  TDUnitX.RegisterTestFixture(TTokenBucketRateLimiterTests);

end.
