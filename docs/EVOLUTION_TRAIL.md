# Trilha de Evolucao

## Objetivo

Expandir o projeto sem perder foco operacional, priorizando ganhos de aderencia ao negocio, confiabilidade e continuidade de uso.

## Ordem de execucao

### Trilha 1: Historico Versionado

Status:

- concluida em `2026-03-24`

Objetivo:

- persistir snapshots diarios das previsoes, alertas e sinais regionais;
- criar base historica para backtest e auditoria.

Entregas:

- tabela de snapshots para `regional_monitoring`, `workforce_forecasts` e `alerts_operacionais`;
- rotina de append historico por ciclo;
- consultas simples de comparacao entre ciclos.

Criterio de pronto:

- cada execucao gera historico reconstituivel sem sobrescrever snapshots antigos;
- o dashboard consegue comparar ciclos persistidos.

### Trilha 2: Backtest e Performance do Modelo

Status:

- concluida em `2026-03-24`

Objetivo:

- medir estabilidade real da previsao ao longo do tempo;
- expor desempenho de forma auditavel.

Entregas:

- backtest temporal rolling;
- baseline simples documentado;
- metricas historicas por ciclo;
- aba ou painel resumido de performance.

Criterio de pronto:

- o modelo deixa de ser apenas "ultima metrica" e passa a ter serie de desempenho;
- o dashboard compara modelo vs baseline.

### Trilha 3: Fontes Mais Diretas de Infraestrutura

Status:

- concluida em `2026-03-24`

Objetivo:

- reduzir dependencia de proxies no monitoramento regional.

Entregas:

- conectores oficiais adicionais para rodovia, porto, aeroporto ou fiscal com aderencia direta ao fluxo logistico;
- incorporacao controlada ao composto regional;
- comparacao antes/depois do impacto no risco operacional.

Criterio de pronto:

- pelo menos um eixo regional relevante passa a ter fonte mais direta que o composto atual.

Fechamento:

- os sinais diretos oficiais de rodovia, porto, aeroporto e fiscal passaram a influenciar o `regional_monitoring` de forma controlada;
- o cockpit exibe comparacao entre risco base e risco ajustado por fontes diretas;
- o impacto das fontes diretas ficou auditavel no banco, no dashboard e na suite smoke.

### Trilha 4: Operacao Continua

Status:

- concluida em `2026-03-24`

Objetivo:

- tornar o cockpit mais confiavel para uso recorrente.

Entregas:

- retencao e limpeza de historico;
- logs operacionais melhores;
- status de ciclo mais legivel;
- verificacoes de falha por fonte, retry, fallback local e timeout.

Criterio de pronto:

- o ambiente local consegue executar ciclos repetidos sem manutencao manual frequente.

## Estado Atual

- `Trilha 1` fechada com historico acumulado entre ciclos e comparacao no dashboard;
- `Trilha 2` fechada com baseline simples, backtest rolling persistido por execucao e painel de estabilidade;
- `Trilha 3` fechada com incorporacao controlada de sinais diretos oficiais ao risco regional;
- `Trilha 4` fechada com retry, fallback local, retencao automatica, resumo por ciclo e scheduler local.

## Encerramento da Fase

A fase de evolucao aberta em `2026-03-23` foi encerrada em `2026-03-24`.

Resultado:

- a base historica ficou persistida e comparavel entre ciclos;
- o modelo ficou auditavel com baseline e backtest;
- o monitoramento regional passou a receber reforco controlado de fontes diretas oficiais;
- a operacao local ficou apta a ciclos repetidos com retry, fallback, retencao e resumo executivo por execucao.

## Proximo Ciclo

Qualquer continuidade a partir daqui deve ser tratada como nova fase de evolucao, nao como pendencia desta trilha.
