# Outbox Pattern

Ensures that messages are published reliably alongside database operations, avoiding the problem of "writing to the database but failing to publish to the queue" (or vice versa).

## Concept

The Outbox Pattern consists of:
1. Writing the message to the outbox table **within the same transaction** that writes the business data
2. A separate process reads the outbox and publishes to the real queue
3. After successful publishing, removes the entry from the outbox

This guarantees at-least-once delivery with transactional consistency.

## IHefestoClientOutbox interface

```pascal
IHefestoClientOutbox = interface
  procedure Save(const ARequest: THefestoPublishRequest);
  function Entries: TArray<THefestoOutboxEntry>;
  procedure Remove(const AEntryId: string);
  procedure Clear;
  function Count: Integer;
end;
```

## Basic usage

```pascal
uses
  Hefesto.Outbox;

var
  LOutbox: IHefestoClientOutbox;
  LRequest: THefestoPublishRequest;
begin
  LOutbox := THefestoStateStoreOutbox.New(LStore);

  // Inside the business transaction:
  LRequest.Queue := 'notifications';
  LRequest.Action := 'send_email';
  LRequest.Body := '{"to": "user@example.com", "subject": "Order confirmed"}';

  LOutbox.Save(LRequest);
  // + write business data to the database (same transaction)
end;
```

## Relay (publishing from the outbox)

The relay reads outbox entries and publishes them to the real queue:

```pascal
var
  LEntries := LOutbox.Entries;
begin
  for var Entry in LEntries do
  begin
    try
      LQueue.Enqueue(Entry.Request.Action, Entry.Request.Body);
      LOutbox.Remove(Entry.Id);
    except
      // keep in outbox to retry later
    end;
  end;
end;
```

The relay can run as a periodic job using [Periodic Jobs](scheduled-e-periodic.md).

## Checking and clearing

```pascal
Writeln('Pending entries: ', LOutbox.Count);

// Clear all (use with care — loses unpublished messages)
LOutbox.Clear;
```

## Persistence

Using `THefestoInMemoryStateStore` for the outbox implies losing messages on restart. For real transactional guarantees, use `THefestoPostgreSQLStateStore` or `THefestoFireDACStateStore` with SQLite, as they can participate in the same database transaction.

See recipe in [06-recipes/outbox.md](../06-recipes/outbox.md).
