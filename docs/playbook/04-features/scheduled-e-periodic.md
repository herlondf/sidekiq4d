# Scheduled and Periodic Jobs

## Scheduled Jobs (single future execution)

Jobs scheduled for a specific date/time. Require an `IHefestoScheduledStore` configured on the server.

### IHefestoScheduledStore interface

```pascal
IHefestoScheduledStore = interface
  procedure Schedule(const AEntry: THefestoScheduledEntry);
  function PopDue(const ANow: TDateTime; ALimit: Integer): TArray<THefestoScheduledEntry>;
  function List: TArray<THefestoScheduledEntry>;
  procedure Delete(const AQueue, AAction: string; const ADueAt: TDateTime);
end;
```

### Scheduling a job

```pascal
var
  LEntry: THefestoScheduledEntry;
begin
  LEntry := MakeScheduledEntry(
    'default',              // queue
    'send_report',          // action
    '{"report_id": 42}',   // body JSON
    [],                     // extra attrs
    Now + (1/24)            // DueAt: 1 hour from now
  );
  LScheduledStore.Schedule(LEntry);
end;
```

### Configuring the server to process scheduled jobs

```pascal
THefestoServer.New
  .UseQueue(TMyAdapter.New)
  .StateStore(LStore)
  // The ScheduledStore is configured separately
  .RegisterHandler('send_report', TSendReportHandler.Create)
  .Run;
```

The server checks `PopDue` periodically and enqueues jobs whose `DueAt` has already passed.

### Management

```pascal
// List scheduled jobs
var LList := LScheduledStore.List;

// Cancel
LScheduledStore.Delete('default', 'send_report', DueAt);
```

Via dashboard REST API: `DELETE /api/scheduled`.

## Periodic Jobs (cron)

Jobs that execute at intervals defined by a cron expression.

### Cron format (5 fields)

```
min  hour  day  month  weekday
```

Examples:
```
*/15  *    *    *    *     every 15 minutes
0     9    *    *    1-5   every weekday at 9am
0     */2  *    *    *     every 2 hours
30    8    1    *    *     1st of each month at 8:30am
0     9    *    *    1,3,5 Monday, Wednesday and Friday at 9am
```

Supported field syntax: `*`, `*/N`, `A-B`, `A,B,C`.

### Registering a periodic job

```pascal
THefestoPeriodicJob.Register(
  'cleanup_temp_files',  // action/name
  '*/30 * * * *',        // cron: every 30 minutes
  'default',             // queue
  '{}',                  // default body
  LScheduledStore
);
```

The scheduler calculates the next `DueAt` based on the cron expression and schedules automatically. After execution, it schedules the next occurrence.

## Troubleshooting

**Scheduled jobs do not fire:**
- Check that `IHefestoScheduledStore` is configured and the scheduler is running
- Check timezone: the scheduler uses the machine's local time
- See [troubleshooting.md](../05-operations-and-runtime/troubleshooting.md)
