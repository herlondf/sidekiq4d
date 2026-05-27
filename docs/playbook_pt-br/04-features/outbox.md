# Outbox Pattern

Garante que mensagens sejam publicadas de forma confiável junto com operações de banco de dados, evitando o problema de "gravar no banco mas falhar ao publicar na fila" (ou vice-versa).

## Conceito

O Outbox Pattern consiste em:
1. Gravar a mensagem na tabela de outbox **dentro da mesma transação** que grava os dados de negócio
2. Um processo separado lê o outbox e publica na fila real
3. Após publicação bem-sucedida, remove a entrada do outbox

Isso garante entrega at-least-once com consistência transacional.

## Interface IHefestoClientOutbox

```pascal
IHefestoClientOutbox = interface
  procedure Save(const ARequest: THefestoPublishRequest);
  function Entries: TArray<THefestoOutboxEntry>;
  procedure Remove(const AEntryId: string);
  procedure Clear;
  function Count: Integer;
end;
```

## Uso básico

```pascal
uses
  Hefesto.Outbox;

var
  LOutbox: IHefestoClientOutbox;
  LRequest: THefestoPublishRequest;
begin
  LOutbox := THefestoStateStoreOutbox.New(LStore);

  // Dentro da transação de negócio:
  LRequest.Queue := 'notifications';
  LRequest.Action := 'send_email';
  LRequest.Body := '{"to": "user@example.com", "subject": "Pedido confirmado"}';

  LOutbox.Save(LRequest);
  // + gravar dados de negócio no banco (mesma transação)
end;
```

## Relay (publicação do outbox)

O relay lê entradas do outbox e as publica na fila real:

```pascal
var
  LEntries := LOutbox.Entries;
begin
  for var Entry in LEntries do
  begin
    try
      LQueue.Enqueue(Entry.Request);
      LOutbox.Remove(Entry.Id);
    except
      // manter no outbox para tentar novamente
    end;
  end;
end;
```

O relay pode rodar como job periódico usando [Periodic Jobs](scheduled-e-periodic.md).

## Verificando e limpando

```pascal
Writeln('Entradas pendentes: ', LOutbox.Count);

// Limpar tudo (usar com cuidado — perde mensagens não publicadas)
LOutbox.Clear;
```

## Persistência

Usar `THefestoInMemoryStateStore` para o outbox implica perda de mensagens ao reiniciar. Para garantias transacionais reais, use `THefestoPostgreSQLStateStore` ou `THefestoFireDACStateStore` com SQLite, pois permitem participar da mesma transação de banco.

Ver receita em [06-receitas/outbox.md](../06-receitas/outbox.md).
