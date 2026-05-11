unit Sidekiq4D.Metadata;

interface

type
  TSidekiqJobAttribute = record
  public const
    IdempotencyKey = 'idempotency_key';
    IdempotencyTtlSeconds = 'idempotency_ttl_seconds';
    LockKey = 'lock_key';
    LockTtlSeconds = 'lock_ttl_seconds';
    ConcurrencyKey = 'concurrency_key';
    RateLimitKey = 'rate_limit_key';
    RateLimitCapacity = 'rate_limit_capacity';
    RateLimitIntervalSeconds = 'rate_limit_interval_seconds';
    RateLimitCost = 'rate_limit_cost';
    UniqueStrategy = 'unique_strategy';
    UniqueTtlSeconds = 'unique_ttl_seconds';
    ScheduledAt = 'scheduled_at';
    PeriodicName = 'periodic_name';
    PeriodicCron = 'periodic_cron';
    ServerLeaseTtlSeconds = 'server_lease_ttl_seconds';
    HeartbeatIntervalSeconds = 'heartbeat_interval_seconds';
    BatchId = 'batch_id';
    BatchCallbackKind = 'batch_callback_kind';
    BatchCallbackFor = 'batch_callback_for';
    ExpiresAt = 'expires_at';
    ExpiresInSeconds = 'expires_in_seconds';
  end;

implementation

end.
