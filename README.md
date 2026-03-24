# Cockpit de Operacoes Logisticas

Este projeto foi reposicionado para apoiar empresas de terceirizacao de mao de obra em operacoes de carga, descarga e movimentacao no Espirito Santo.

O sistema consolida demanda operacional real, sinais externos oficiais e risco operacional, treina um modelo para prever necessidade de equipe e entrega um dashboard com:

- previsao de pessoas por unidade, cliente, operacao e turno;
- gaps de escala;
- alertas operacionais;
- margem prevista por contrato;
- insights executivos gerados automaticamente;
- historico entre ciclos;
- comparacao de modelo vs baseline.

## Escopo Final

O projeto foi fechado como um cockpit operacional para:

- consolidar sinais reais relevantes para operacoes logisticas no ES;
- estimar pressao operacional por corredor, unidade, cliente e turno;
- prever necessidade de equipe para os proximos 14 dias;
- priorizar alertas operacionais e apoiar decisao tatico-executiva.

Fontes principais dentro do escopo:

- `MDIC Export Demand` para demanda operacional externa;
- `BCB Market Indicators` para sinais macro complementares;
- `Official Transport News` para risco institucional/setorial;
- `Regional Monitoring Derived` como composto operacional principal;
- `INMET Regional Forecast` apenas como enriquecimento climatico complementar;
- `IBGE Localidades ES` para padronizacao territorial;
- `Direct Infrastructure Signals` como reforco oficial controlado do risco regional.

## Fluxo

1. `ingestion`: ingere demanda operacional real, indicadores oficiais, noticias institucionais e sinais climaticos complementares.
2. `processing`: normaliza os dados e monta a base operacional.
3. `ml`: treina o modelo e gera previsoes para os proximos 14 dias.
4. `dashboard`: exibe o cockpit de operacoes.

## Estado Atual

Referencia operacional validada em `2026-03-24`:

- `run_id`: `80d85eb7-2870-4353-919b-a592c9afbf95`
- `operations_daily`: `2790`
- `regional_monitoring`: `930`
- `workforce_forecasts`: `210`
- `alerts_operacionais`: `118`
- `data_quality_checks`: `18`
- `healthy_sources`: `12`
- `failed_sources`: `0`

Historico acumulado:

- `regional_monitoring_history`: `6515`
- `workforce_forecasts_history`: `1470`
- `alerts_operacionais_history`: `826`
- `model_performance_history`: `7`
- `model_backtest_folds`: `24`
- `pipeline_run_summaries`: `4`

Modelo atual:

- `MAE`: `6.19`
- `baseline_MAE`: `189.05`
- `backtest_folds`: `4`
- `backtest_MAE_medio`: `13.44`
- `backtest_baseline_MAE_medio`: `208.72`

Cobertura oficial atual:

- `official_api_catalog`: `64`
- `source_probe`: `86`

Esses numeros sao um snapshot de referencia da ultima validacao completa; eles podem mudar a cada novo ciclo do pipeline.

## Execucao Local

1. Instale dependencias:

```bash
pip install -r requirements.txt
```

2. Inicialize o banco:

```bash
python scripts/init_db.py
```

3. Rode o pipeline completo:

```bash
python scripts/test_pipeline.py
```

O pipeline agora faz `retry` por fonte e, quando uma fonte central falha mas existe artefato local valido, reaproveita o cache local para evitar queda desnecessaria do ciclo.
Tambem registra resumo por ciclo, retencao automatica de historico e reforco controlado de sinais diretos oficiais no risco regional.

O tracking de experimentos do MLflow e armazenado localmente em `models/mlflow.db`.
Os artefatos dos modelos ficam em `models/mlruns`.

4. Suba o dashboard:

No Windows, prefira este comando para evitar o prompt inicial do Streamlit:

```powershell
cmd /c "set STREAMLIT_BROWSER_GATHER_USAGE_STATS=false&& set STREAMLIT_SERVER_HEADLESS=true&& streamlit run services/dashboard/app.py"
```

Em outros ambientes, ou se o prompt inicial nao for um problema:

```bash
streamlit run services/dashboard/app.py
```

## Execucao Rapida

Para executar localmente com um unico comando no Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_local.ps1
```

Opcoes uteis:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_local.ps1 -RunTests
powershell -ExecutionPolicy Bypass -File scripts/run_local.ps1 -SkipPipeline
```

Para rodar ciclos continuos localmente:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_scheduler.ps1 -Cycles 3 -IntervalSeconds 3600
```

## Testes

Suite minima de entrega:

```bash
python -m unittest discover -s tests -v
```

Ela valida:

- volume minimo das tabelas principais;
- historico de snapshots do ultimo ciclo;
- historico de performance e backtest do modelo;
- coerencia das fontes dentro do escopo;
- impacto dos sinais diretos oficiais no monitoramento regional;
- artefato MLflow em `skops`;
- carregamento do dashboard.

## Premissas e Limites

- O `regional_monitoring` continua sendo um composto operacional analitico; as fontes diretas oficiais entram como reforco controlado, nao como telemetria bruta de campo.
- O `INMET Regional Forecast` nao substitui a camada operacional; ele apenas ajusta sensibilidade climatica dos corredores.
- O modelo trabalha com sinais historicos consolidados do pipeline local; sem novos ciclos, as previsoes nao se atualizam.
- O projeto foi fechado para o contexto do Espirito Santo e para operacoes de carga, descarga, armazenagem e movimentacao.

## Fase de Evolucao

Documentos de transicao e planejamento:

- checkpoint da fase encerrada: `docs/EVOLUTION_CHECKPOINT_2026-03-23.md`
- trilha encerrada da fase: `docs/EVOLUTION_TRAIL.md`

Status resumido:

- `Trilha 1` concluida;
- `Trilha 2` concluida;
- `Trilha 3` concluida;
- `Trilha 4` concluida.

A fase de evolucao aberta em `2026-03-23` foi formalmente encerrada em `2026-03-24`.

## Execucao com Docker Desktop

```bash
docker compose up --build
```

Isso executa o pipeline em um container e sobe o dashboard em `http://localhost:8501`.
