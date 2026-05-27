# Direções de Evolução

Direções possíveis baseadas nos limites atuais e casos de uso observados. Não são compromissos de roadmap.

## Broker e protocolo

**Suporte a múltiplas filas por servidor**
Permitir que um `IHefestoServer` consuma de múltiplas filas com pesos ou prioridades. O dispatcher selecionaria a fila com base em configuração.

**Adapter AMQP nativo para RabbitMQ**
Eliminar dependência do Management HTTP API usando um cliente AMQP puro Delphi ou wrapper de biblioteca C.

**Adapter Kafka nativo**
Integração direta via librdkafka (DLL) ou implementação de protocolo Kafka em Delphi puro, eliminando dependência do Confluent REST Proxy.

## Scheduled Jobs

**Scheduler externo persistente**
Um processo dedicado de scheduling que sobrevive a restarts e garante que jobs agendados durante inatividade são executados com o menor atraso possível após o serviço voltar.

**Suporte a segundos em expressões cron**
Expressões de 6 campos (`sec min hora dia mês diasemana`) para agendamentos sub-minuto.

## Confiabilidade

**Reconexão automática de adapters**
Adaptadores com lógica de retry e backoff para reconexão ao broker em caso de queda temporária.

**Circuit breaker no nível do servidor**
Pausar fetch automaticamente quando o broker sinalizar sobrecarga ou indisponibilidade.

**Graceful shutdown com drain**
Aguardar que todos os jobs em andamento concluam antes de parar, com timeout configurável e persistência dos jobs não iniciados de volta à fila.

## Observabilidade

**Autenticação no dashboard**
Basic Auth ou token configurável para proteger os endpoints web sem necessidade de reverse proxy.

**Persistência de métricas históricas**
Gravar métricas no state store (Redis) para acesso após restart e correlação temporal.

**Logs estruturados com níveis**
Nível de log configurável (DEBUG, INFO, WARN, ERROR) integrado ao `IHefestoTelemetry`.

## Developer Experience

**Delphi Package (BPL)**
Empacotar o core e adapters como BPLs instaláveis pelo IDE, simplificando a configuração do library path.

**Gerador de handler**
Template wizard ou script que cria o scaffold de um novo handler, testes e exemplo com um único comando.

**Configuração via arquivo**
Suporte a `sidekiq4d.json` ou `.env` para configurar o servidor sem recompilação (útil em ambientes containerizados).

## Plataforma

**Linux (Delphi 12 + Linux compiler)**
Testar e ajustar os adapters para compilação com o compilador Linux do Delphi 12, substituindo Synapse por algo mais portável onde necessário.

## Contribuindo

Para implementar qualquer uma destas direções, seguir o processo em [CLAUDE.md](../../CLAUDE.md):
1. Criar a unit de interface primeiro
2. Implementar com testes DUnitX
3. Adicionar exemplo em `examples/`
4. Atualizar `AGENTS.md` com o novo alias
