# Checkpoint de Evolucao

Data de referencia: `2026-03-23`

## Estado atual

O projeto encerrou a fase de escopo com entrega operacional funcional para o contexto de terceirizacao de mao de obra em carga, descarga, armazenagem e movimentacao no Espirito Santo.

Capacidades concluidas:

- ingestao de demanda operacional real via `MDIC Export Demand`;
- ingestao de sinais macro complementares via `BCB Market Indicators`;
- ingestao de noticias institucionais via `Official Transport News`;
- enriquecimento territorial via `IBGE Localidades ES`;
- monitoramento regional operacional via `Regional Monitoring Derived`;
- enriquecimento climatico complementar via `INMET Regional Forecast`;
- treinamento e previsao de equipe com rastreabilidade em MLflow;
- dashboard executivo-operacional em Streamlit;
- testes smoke de entrega;
- bootstrap local unificado.

## Ultima execucao valida

Pipeline:

- `run_id`: `6caff396-e4b7-4a0c-9fa7-895b2c07300e`
- `status`: `success`
- `started_at`: `2026-03-23T23:28:31.336448`
- `finished_at`: `2026-03-23T23:32:41.776667`

Volumes:

- `operations_daily`: `2805`
- `regional_monitoring`: `935`
- `workforce_forecasts`: `210`
- `alerts_operacionais`: `118`
- `data_quality_checks`: `18`
- `source_registry`: `12`

Modelo:

- `mlflow_run_id`: `67ed62e504f7403c84429725e2d80188`
- `MAE`: `6.7885`
- `MAPE`: `0.0157`
- `R2`: `0.9992`

Qualidade:

- `failed_high`: `0`
- `failed_total`: `0`
- `warned_total`: `0`

## Premissas da fase encerrada

- o monitoramento regional e um composto operacional analitico, nao telemetria direta de campo;
- o INMET atua apenas como ajuste complementar;
- o cockpit foi finalizado para uso local e contexto ES;
- a entrega atual e adequada para operacao assistida e evolucao incremental.

## Riscos residuais aceitos

- parte dos sinais regionais ainda e proxy operacional, nao dado transacional bruto de rodovia/porto;
- o historico de previsoes externas ainda depende de snapshots locais ao longo do tempo;
- a cobertura de fontes oficiais diretas para infraestrutura ainda pode ser aprofundada.

## Criterio de entrada na fase de evolucao

A fase de evolucao comeca a partir deste checkpoint, sem reabrir o escopo base. As proximas entregas devem:

- preservar o cockpit operacional atual;
- aumentar aderencia ao negocio;
- reduzir proxies onde houver fonte oficial melhor;
- manter validacao automatizada e operabilidade local.
