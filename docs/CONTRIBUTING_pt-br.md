# Contribuindo com o Hefesto

## Escopo

O Hefesto busca ser um framework Delphi de processamento de jobs em background inspirado no Sidekiq do Ruby, focado em:

- **Backends de fila baseados em adapter** — Redis, SQS e outros via `IHefestoQueueAdapter`; troque o broker sem modificar o código dos workers
- **API de configuração fluente** — encadeamento de métodos retornando `Self` para setup legível e composável
- **Worker pool thread-safe** — despacho de jobs e rastreamento de estado protegidos com `TCriticalSection` / `TInterlocked`
- **Middleware plugável** — pipeline server-side via `IHefestoServerMiddleware` para logs, retries e métricas
- **State store plugável** — persistência de estado, retries e histórico de jobs via `IHefestoStateStore`

## Diretrizes técnicas

- Todos os backends de fila devem implementar `IHefestoQueueAdapter` (5 métodos: `Enqueue`, `Dequeue`, `Acknowledge`, `Reject`, `Peek`).
- Todo middleware de servidor deve implementar `IHefestoServerMiddleware` (1 método: `Call`).
- A persistência de estado deve implementar `IHefestoStateStore`.
- Factories usam `class function New` — callers nunca devem instanciar classes concretas diretamente.
- Métodos da API fluente devem retornar `Self` para suportar encadeamento.
- Todo `class var` ou estado compartilhado acessado por múltiplas threads deve ser protegido com `TCriticalSection` ou `TInterlocked`.
- `try/finally` é obrigatório sempre que um objeto é alocado e precisa ser liberado.
- Sem blocos `except` vazios. Logar ou relançar.
- Nomes de units seguem a convenção `Hefesto.<Modulo>.pas` (ex.: `Hefesto.Queue.Redis.pas`).

## Fluxo sugerido

1. Abra uma issue descrevendo o bug, funcionalidade ou novo adapter antes de iniciar trabalho em mudanças maiores.
2. Faça uma branch a partir de `main`.
3. Adicione ou ajuste testes em `tests/` — testes unitários não requerem broker externo; testes de integração/smoke requerem Redis ou SQS (suba com `docker-compose` em `docker/`).
4. Novos adapters vão em `src/adapters/`; novos middlewares em `src/middleware/`; novos state stores em `src/store/`.
5. Se estiver adicionando um novo sample, coloque-o em `samples/NN-nome/` com seu próprio `.dpr` / `.dproj`.
6. Atualize o playbook em `docs/playbook/` quando a mudança afetar uso, opções ou comportamento observável.
7. Envie um pull request com descrição objetiva do problema e da solução.

## Validação mínima

Sempre valide antes de abrir um PR:

- Build da suite de testes:
  ```
  msbuild tests\Hefesto.Tests.dproj /p:Config=Release /p:Platform=Win32
  ```
- Execute os testes unitários (não requer broker).
- Execute os testes de integração/smoke quando a mudança tocar um queue adapter, state store ou o loop de despacho — requer `docker-compose up` em `docker/`.

## Adicionando um novo queue adapter

1. Crie `src/adapters/Hefesto.Queue.<Backend>.pas`.
2. Implemente todos os 5 métodos de `IHefestoQueueAdapter`: `Enqueue`, `Dequeue`, `Acknowledge`, `Reject`, `Peek`.
3. Exponha via `class function New` — sem instanciação direta por callers.
4. Adicione testes de integração em `tests/` (podem ser pulados se o broker não estiver disponível no CI, mas devem passar localmente).
5. Adicione um sample em `samples/` e documente em `docs/playbook/`.

## Adicionando um novo middleware

1. Crie `src/middleware/Hefesto.Middleware.<Nome>.pas`.
2. Implemente `IHefestoServerMiddleware` — um único método `Call` que recebe o contexto do job e uma continuação `next`.
3. Adicione testes unitários cobrindo o comportamento do middleware de forma isolada.
4. Documente a configuração e notas sobre ordem de execução em `docs/playbook/`.

## Adicionando um novo state store

1. Crie `src/store/Hefesto.Store.<Backend>.pas`.
2. Implemente `IHefestoStateStore`.
3. Adicione testes cobrindo persistência, contadores de retry e recuperação de histórico.

## Convenções

- Documentação pública (`playbook/`) em inglês; `playbook_pt-br/` é a tradução direta — manter em sincronia.
- Código e identificadores seguem o estilo atual do projeto (prefixo `THefesto`, `IHefesto`, `EHefesto`).
- Mensagens de commit em pt-BR, formato `tipo(escopo): descrição curta` (Conventional Commits).

> 🇺🇸 Read in English: [CONTRIBUTING.md](./CONTRIBUTING.md)
