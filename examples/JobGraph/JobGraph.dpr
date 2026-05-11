program JobGraph;

{$APPTYPE CONSOLE}

{
  Exemplo: Sidekiq4D.Graph — Job Graph com dependências (DAG)

  Topologia demonstrada (diamante):

      ┌─ validate ─┐
    extract         ├─ save-db
      └─  enrich  ─┘

  Regras:
    - validate e enrich só iniciam após extract concluir
    - save-db só inicia após validate E enrich concluírem
    - Se qualquer nó falhar, todos os filhos são cancelados automaticamente

  Executa três cenários:
    1. Pipeline sequencial com sucesso
    2. Pipeline com falha em "validate" → "save-db" é cancelado
    3. Pipeline paralelo (.Parallel) — validate e enrich correm ao mesmo tempo
}

uses
  System.SysUtils,
  System.SyncObjs,
  Sidekiq4D.Graph in '..\..\src\Sidekiq4D.Graph.pas';

procedure RunSuccessPipeline;
begin
  WriteLn('--- Cenário 1: Sucesso completo ---');
  WriteLn;

  TSidekiqJobGraph.New
    .AddNode('extract',
      procedure
      begin
        WriteLn('[extract]   buscando 1.000 registros da API externa...');
        WriteLn('[extract]   concluído.');
      end)
    .AddNode('validate',
      procedure
      begin
        WriteLn('[validate]  validando schema (depende de extract)...');
        WriteLn('[validate]  0 erros encontrados.');
      end)
      .DependsOn('extract')
    .AddNode('enrich',
      procedure
      begin
        WriteLn('[enrich]    enriquecendo com geocoding (depende de extract)...');
        WriteLn('[enrich]    1.000 registros enriquecidos.');
      end)
      .DependsOn('extract')
    .AddNode('save-db',
      procedure
      begin
        WriteLn('[save-db]   inserindo no PostgreSQL (depende de validate + enrich)...');
        WriteLn('[save-db]   commit OK. 1.000 linhas inseridas.');
      end)
      .DependsOn('validate')
      .DependsOn('enrich')
    .OnNodeFailed(
      procedure(const AId, AErr: string)
      begin
        WriteLn(Format('[FALHA]     nó "%s" → %s', [AId, AErr]));
      end)
    .OnNodeCancelled(
      procedure(const AId: string)
      begin
        WriteLn(Format('[CANCELADO] nó "%s" pulado por dependência com falha.', [AId]));
      end)
    .Execute;

  WriteLn;
  WriteLn('Pipeline 1 finalizado.');
end;

procedure RunFailurePipeline;
begin
  WriteLn('--- Cenário 2: Falha em validate → save-db cancelado ---');
  WriteLn;

  TSidekiqJobGraph.New
    .AddNode('extract',
      procedure
      begin
        WriteLn('[extract]   buscando registros...');
        WriteLn('[extract]   concluído.');
      end)
    .AddNode('validate',
      procedure
      begin
        WriteLn('[validate]  validando...');
        raise Exception.Create('schema inválido: campo "email" ausente em 42 registros');
      end)
      .DependsOn('extract')
    .AddNode('enrich',
      procedure
      begin
        WriteLn('[enrich]    enriquecendo...');
        WriteLn('[enrich]    concluído.');
      end)
      .DependsOn('extract')
    .AddNode('save-db',
      procedure
      begin
        WriteLn('[save-db]   salvando...');
      end)
      .DependsOn('validate')
      .DependsOn('enrich')
    .OnNodeFailed(
      procedure(const AId, AErr: string)
      begin
        WriteLn(Format('[FALHA]     nó "%s" → %s', [AId, AErr]));
      end)
    .OnNodeCancelled(
      procedure(const AId: string)
      begin
        WriteLn(Format('[CANCELADO] nó "%s" pulado por dependência com falha.', [AId]));
      end)
    .Execute;

  WriteLn;
  WriteLn('Pipeline 2 finalizado.');
end;

procedure RunParallelPipeline;
var
  LLock: TCriticalSection;
  LLog: string;
begin
  WriteLn('--- Cenário 3: Execução paralela (.Parallel) ---');
  WriteLn;

  LLog  := '';
  LLock := TCriticalSection.Create;
  try
    TSidekiqJobGraph.New
      .AddNode('extract',
        procedure
        begin
          WriteLn('[extract]   buscando registros...');
          Sleep(100);
          WriteLn('[extract]   concluído.');
          LLock.Acquire; try LLog := LLog + 'extract '; finally LLock.Release; end;
        end)
      .AddNode('validate',
        procedure
        begin
          WriteLn('[validate]  iniciando (paralelo com enrich)...');
          Sleep(80);
          WriteLn('[validate]  concluído.');
          LLock.Acquire; try LLog := LLog + 'validate '; finally LLock.Release; end;
        end)
        .DependsOn('extract')
      .AddNode('enrich',
        procedure
        begin
          WriteLn('[enrich]    iniciando (paralelo com validate)...');
          Sleep(60);
          WriteLn('[enrich]    concluído.');
          LLock.Acquire; try LLog := LLog + 'enrich '; finally LLock.Release; end;
        end)
        .DependsOn('extract')
      .AddNode('save-db',
        procedure
        begin
          WriteLn('[save-db]   inserindo (após validate + enrich)...');
          WriteLn('[save-db]   commit OK.');
          LLock.Acquire; try LLog := LLog + 'save-db'; finally LLock.Release; end;
        end)
        .DependsOn('validate')
        .DependsOn('enrich')
      .Parallel
      .OnNodeFailed(
        procedure(const AId, AErr: string)
        begin
          WriteLn(Format('[FALHA]     nó "%s" → %s', [AId, AErr]));
        end)
      .Execute;

    WriteLn;
    WriteLn('Ordem de conclusão: ', LLog);
    WriteLn('  → validate e enrich executaram em ondas simultâneas');
  finally
    LLock.Free;
  end;

  WriteLn;
  WriteLn('Pipeline 3 finalizado.');
end;

begin
  WriteLn('====================================================');
  WriteLn('  Sidekiq4D — Job Graph / DAG Example');
  WriteLn('====================================================');
  WriteLn;
  try
    RunSuccessPipeline;
    WriteLn;
    RunFailurePipeline;
    WriteLn;
    RunParallelPipeline;
  except
    on E: Exception do
      WriteLn('Erro inesperado: ', E.ClassName, ': ', E.Message);
  end;
  WriteLn;
  Write('Pressione Enter para sair... ');
  ReadLn;
end.
