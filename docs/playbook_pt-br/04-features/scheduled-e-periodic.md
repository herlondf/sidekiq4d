# Scheduled e Periodic Jobs

## Scheduled Jobs (execução futura única)

Jobs agendados para uma data/hora específica. Requerem um `IHefestoScheduledStore` configurado no servidor.

### Interface IHefestoScheduledStore

```pascal
IHefestoScheduledStore = interface
  procedure Schedule(const AEntry: THefestoScheduledEntry);
  function PopDue(const ANow: TDateTime; ALimit: Integer): TArray<THefestoScheduledEntry>;
  function List: TArray<THefestoScheduledEntry>;
  procedure Delete(const AQueue, AAction: string; const ADueAt: TDateTime);
end;
```

### Agendando um job

```pascal
var
  LEntry: THefestoScheduledEntry;
begin
  LEntry := MakeScheduledEntry(
    'default',              // queue
    'send_report',          // action
    '{"report_id": 42}',   // body JSON
    [],                     // attrs extras
    Now + (1/24)            // DueAt: daqui a 1 hora
  );
  LScheduledStore.Schedule(LEntry);
end;
```

### Configurando o servidor para processar scheduled jobs

```pascal
THefestoServer.New
  .UseQueue(TMyAdapter.New)
  .StateStore(LStore)
  // O ScheduledStore é configurado separadamente
  .RegisterHandler('send_report', TSendReportHandler.Create)
  .Run;
```

O servidor verifica `PopDue` periodicamente e enfileira jobs cujo `DueAt` já passou.

### Gerenciamento

```pascal
// Listar agendados
var LList := LScheduledStore.List;

// Cancelar
LScheduledStore.Delete('default', 'send_report', DueAt);
```

Via API REST do dashboard: `DELETE /api/scheduled`.

## Periodic Jobs (cron)

Jobs que executam em intervalos definidos por expressão cron.

### Formato cron (5 campos)

```
min  hora  dia  mês  diasemana
```

Exemplos:
```
*/15  *    *    *    *     a cada 15 minutos
0     9    *    *    1-5   toda segunda a sexta às 9h
0     */2  *    *    *     a cada 2 horas
30    8    1    *    *     dia 1 de cada mês às 8h30
0     9    *    *    1,3,5 segunda, quarta e sexta às 9h
```

Campos suportados: `*`, `*/N`, `A-B`, `A,B,C`.

### Registrando um job periódico

```pascal
THefestoPeriodicJob.Register(
  'cleanup_temp_files',  // action/nome
  '*/30 * * * *',        // cron: a cada 30 minutos
  'default',             // queue
  '{}',                  // body padrão
  LScheduledStore
);
```

O scheduler calcula o próximo `DueAt` com base na expressão cron e agenda automaticamente. Após a execução, agenda a próxima ocorrência.

## Troubleshooting

**Jobs agendados não disparam:**
- Verificar se `IHefestoScheduledStore` está configurado e o scheduler está rodando
- Verificar timezone: o scheduler usa horário local da máquina
- Ver [troubleshooting.md](../05-operacao-e-runtime/troubleshooting.md)
