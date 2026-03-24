import sqlite3
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared import config

st.set_page_config(layout="wide", page_title="Cockpit de Operacoes Logisticas")

st.title("Cockpit de Operacoes Logisticas")
st.caption("Previsao de demanda, dimensionamento de equipes, risco operacional e margem para terceirizacao em carga e descarga no ES.")


def _read_table(conn, query):
    try:
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()


conn = sqlite3.connect(config.SQLITE_DB)
operations = _read_table(conn, "SELECT * FROM operations_daily ORDER BY data")
contracts = _read_table(conn, "SELECT * FROM contract_summary ORDER BY margem_total DESC")
forecasts = _read_table(conn, "SELECT * FROM workforce_forecasts ORDER BY data, unidade, cliente")
alerts = _read_table(conn, "SELECT * FROM alerts_operacionais ORDER BY risk_score DESC")
insights = _read_table(conn, "SELECT * FROM executive_insights ORDER BY generated_at DESC")
news = _read_table(conn, "SELECT * FROM news_monitoring ORDER BY data DESC")
regional = _read_table(conn, "SELECT * FROM regional_monitoring ORDER BY data, corredor")
direct_infrastructure_signals = _read_table(conn, "SELECT * FROM direct_infrastructure_signals ORDER BY data DESC, corredor")
regional_history = _read_table(conn, "SELECT * FROM regional_monitoring_history ORDER BY snapshot_captured_at DESC, data, corredor")
forecasts_history = _read_table(conn, "SELECT * FROM workforce_forecasts_history ORDER BY snapshot_captured_at DESC, data, unidade, cliente")
alerts_history = _read_table(conn, "SELECT * FROM alerts_operacionais_history ORDER BY snapshot_captured_at DESC, data DESC")
model_performance_history = _read_table(conn, "SELECT * FROM model_performance_history ORDER BY recorded_at DESC")
model_backtest_folds = _read_table(conn, "SELECT * FROM model_backtest_folds ORDER BY fold_id")
municipality_catalog = _read_table(conn, "SELECT * FROM municipality_catalog ORDER BY municipio")
source_catalog = _read_table(conn, "SELECT * FROM source_catalog ORDER BY priority, source_name")
source_probe = _read_table(conn, "SELECT * FROM source_probe ORDER BY collected_at DESC, source_name")
asset_registry = _read_table(conn, "SELECT * FROM asset_registry ORDER BY fetch_ready DESC, fetch_priority, source_name")
fetched_assets = _read_table(conn, "SELECT * FROM fetched_assets ORDER BY fetched_at DESC, source_name")
asset_fetch_log = _read_table(conn, "SELECT * FROM asset_fetch_log ORDER BY fetched_at DESC, source_name")
official_api_catalog = _read_table(conn, "SELECT * FROM official_api_catalog ORDER BY source_name, resource_count DESC, dataset_title")
official_signal_intelligence = _read_table(conn, "SELECT * FROM official_signal_intelligence ORDER BY signal_strength DESC, source_name")
download_candidates = _read_table(conn, "SELECT * FROM download_candidates ORDER BY priority, source_name, candidate_url")
monitored_entities = _read_table(conn, "SELECT * FROM monitored_entities ORDER BY entity_type, entity_name")
entity_registry = _read_table(conn, "SELECT * FROM entity_registry ORDER BY importance_score DESC, entity_name")
source_registry = _read_table(conn, "SELECT * FROM source_registry ORDER BY priority, source_name")
source_policy = _read_table(conn, "SELECT * FROM source_policy ORDER BY refresh_mode, source_name")
refresh_queue = _read_table(conn, "SELECT * FROM refresh_queue ORDER BY enqueued_at DESC")
ingestion_runs = _read_table(conn, "SELECT * FROM ingestion_runs ORDER BY started_at DESC")
pipeline_run_summaries = _read_table(conn, "SELECT * FROM pipeline_run_summaries ORDER BY recorded_at DESC")
pipeline_state = _read_table(conn, "SELECT * FROM pipeline_state ORDER BY updated_at DESC")
data_quality_checks = _read_table(conn, "SELECT * FROM data_quality_checks ORDER BY checked_at DESC, severity DESC, check_group, table_name")
conn.close()

for frame, date_col in [
    (operations, "data"),
    (forecasts, "data"),
    (news, "data"),
    (regional, "data"),
    (direct_infrastructure_signals, "data"),
    (regional_history, "data"),
    (forecasts_history, "data"),
    (alerts_history, "data"),
    (model_performance_history, "recorded_at"),
    (model_backtest_folds, "recorded_at"),
    (pipeline_run_summaries, "recorded_at"),
]:
    if not frame.empty and date_col in frame.columns:
        frame[date_col] = pd.to_datetime(frame[date_col])

for frame in [regional_history, forecasts_history, alerts_history]:
    if not frame.empty and "snapshot_captured_at" in frame.columns:
        frame["snapshot_captured_at"] = pd.to_datetime(frame["snapshot_captured_at"])

overview_cols = st.columns(5)
if not forecasts.empty:
    overview_cols[0].metric("Pessoas previstas", int(forecasts["trabalhadores_previstos"].sum()))
    overview_cols[1].metric("Gap acumulado", int(forecasts["gap_previsto"].clip(lower=0).sum()))
    overview_cols[2].metric("Margem prevista", f"R$ {forecasts['margem_prevista'].sum():,.0f}")
    overview_cols[3].metric("Risco medio", f"{forecasts['risk_score'].mean():.1f}")
    overview_cols[4].metric("Alertas criticos", int((forecasts["risk_score"] >= 80).sum()))
else:
    for col in overview_cols:
        col.metric("Sem dados", "-")

st.subheader("Leitura Executiva")
if not insights.empty:
    for insight in insights.head(5).itertuples(index=False):
        st.markdown(f"**[{insight.priority.upper()}] {insight.title}**  \n{insight.detail}")
else:
    st.info("Os insights executivos aparecem aqui depois da execucao completa do pipeline.")

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(["Previsao", "Escala", "Clientes", "Alertas", "Sinais", "Regional", "Historico", "Modelo", "Operacao", "Qualidade"])

with tab1:
    st.header("Previsao de Demanda e Capacidade")
    if forecasts.empty:
        st.warning("Nenhuma previsao foi gerada ainda.")
    else:
        daily = forecasts.groupby("data", as_index=False).agg(
            trabalhadores_previstos=("trabalhadores_previstos", "sum"),
            capacidade_atual=("capacidade_atual", "sum"),
            gap_previsto=("gap_previsto", "sum"),
        )
        fig = px.line(daily, x="data", y=["trabalhadores_previstos", "capacidade_atual"], title="Demanda prevista vs capacidade atual")
        st.plotly_chart(fig, width="stretch")

        units = forecasts.groupby("unidade", as_index=False).agg(
            trabalhadores_previstos=("trabalhadores_previstos", "sum"),
            gap_previsto=("gap_previsto", "sum"),
            risk_score=("risk_score", "mean"),
        )
        fig_units = px.bar(units.sort_values("trabalhadores_previstos", ascending=False), x="unidade", y="trabalhadores_previstos", color="risk_score", title="Carga de trabalho prevista por unidade")
        st.plotly_chart(fig_units, width="stretch")
        st.dataframe(forecasts.head(60), width="stretch", hide_index=True)

with tab2:
    st.header("Escala e Dimensionamento")
    if forecasts.empty:
        st.warning("Sem previsoes para dimensionamento.")
    else:
        staffing = forecasts.groupby(["unidade", "turno"], as_index=False).agg(
            trabalhadores_previstos=("trabalhadores_previstos", "sum"),
            capacidade_atual=("capacidade_atual", "sum"),
            gap_previsto=("gap_previsto", "sum"),
            risk_score=("risk_score", "mean"),
        )
        staffing["gap_previsto"] = staffing["gap_previsto"].clip(lower=0)
        fig_staff = px.bar(staffing.sort_values("gap_previsto", ascending=False), x="unidade", y="gap_previsto", color="turno", title="Gap previsto de equipe por unidade e turno", barmode="group")
        st.plotly_chart(fig_staff, width="stretch")
        critical = forecasts[forecasts["gap_previsto"] > 0].sort_values(["gap_previsto", "risk_score"], ascending=[False, False])
        st.dataframe(critical.head(40), width="stretch", hide_index=True)

with tab3:
    st.header("Clientes e Rentabilidade")
    if contracts.empty:
        st.warning("Resumo de contratos indisponivel.")
    else:
        fig_margin = px.bar(contracts.sort_values("margem_total", ascending=False), x="cliente", y="margem_total", color="sla_medio", title="Margem acumulada por cliente")
        st.plotly_chart(fig_margin, width="stretch")
        fig_risk = px.scatter(contracts, x="absenteismo_medio", y="margem_total", size="equipe_media", color="risco_medio", hover_name="cliente", title="Relacao entre absenteismo, margem e risco")
        st.plotly_chart(fig_risk, width="stretch")
        st.dataframe(contracts, width="stretch", hide_index=True)

with tab4:
    st.header("Alertas Operacionais")
    if alerts.empty:
        st.success("Nenhum alerta relevante no horizonte atual.")
    else:
        st.metric("Alertas ativos", len(alerts))
        fig_alerts = px.bar(alerts.head(25), x="unidade", y="risk_score", color="alerta", hover_data=["cliente", "turno", "gap_previsto", "acao_recomendada"], title="Alertas por unidade")
        st.plotly_chart(fig_alerts, width="stretch")
        st.dataframe(alerts[["data", "unidade", "cliente", "turno", "alerta", "gap_previsto", "risk_score", "acao_recomendada"]], width="stretch", hide_index=True)

with tab5:
    st.header("Sinais Externos e Noticias")
    if operations.empty:
        st.warning("Sem dados operacionais historicos.")
    else:
        signals = operations.groupby("data", as_index=False).agg(
            demanda_externa_index=("demanda_externa_index", "mean"),
            pressao_mao_obra_index=("pressao_mao_obra_index", "mean"),
            chuva_mm=("chuva_mm", "mean"),
        )
        fig_signals = px.line(signals.tail(60), x="data", y=["demanda_externa_index", "pressao_mao_obra_index"], title="Sinais que influenciam a necessidade de equipe")
        st.plotly_chart(fig_signals, width="stretch")
        st.dataframe(news.head(20), width="stretch", hide_index=True)

with tab6:
    st.header("Monitoramento Regional")
    st.caption("Cobertura de corredores rodoviarios e portuarios com composto operacional baseado em demanda real, noticias oficiais e clima complementar.")

    if regional.empty:
        st.warning("Sem sinais regionais consolidados.")
    else:
        latest_regional = regional[regional["data"] == regional["data"].max()].copy()
        top_entities = entity_registry.head(12) if not entity_registry.empty else pd.DataFrame()
        regional_cols = st.columns(4)
        regional_cols[0].metric("Corredores monitorados", int(latest_regional["corredor"].nunique()))
        regional_cols[1].metric("Municipios cobertos", int(latest_regional["municipio"].nunique()))
        regional_cols[2].metric("Risco medio regional", f"{latest_regional['infraestrutura_risk_index'].mean():.2f}")
        regional_cols[3].metric("Entidades priorizadas", int(len(entity_registry)))
        if {"infraestrutura_risk_index_base", "impacto_fontes_diretas"}.issubset(latest_regional.columns):
            compare_cols = st.columns(3)
            compare_cols[0].metric("Risco base medio", f"{latest_regional['infraestrutura_risk_index_base'].mean():.2f}")
            compare_cols[1].metric("Reforco medio de fontes diretas", f"{latest_regional['impacto_fontes_diretas'].mean():.3f}")
            compare_cols[2].metric("Suporte direto medio", f"{latest_regional['source_support_index'].mean():.2f}")

        fig_regional = px.bar(
            latest_regional.sort_values("infraestrutura_risk_index", ascending=False),
            x="corredor",
            y="infraestrutura_risk_index",
            color="modal_predominante",
            title="Risco de infraestrutura por corredor monitorado",
        )
        st.plotly_chart(fig_regional, width="stretch")

        fig_multisignal = px.scatter(
            latest_regional,
            x="rodovias_trafego_index",
            y="porto_fila_index",
            size="fiscal_emissao_index",
            color="infraestrutura_risk_index",
            hover_name="corredor",
            title="Corredores: trafego rodoviario, fila portuaria e pulsacao fiscal",
        )
        st.plotly_chart(fig_multisignal, width="stretch")
        if {"infraestrutura_risk_index_base", "infraestrutura_risk_index"}.issubset(latest_regional.columns):
            regional_compare = latest_regional.melt(
                id_vars=["corredor"],
                value_vars=["infraestrutura_risk_index_base", "infraestrutura_risk_index"],
                var_name="serie",
                value_name="valor",
            )
            fig_regional_compare = px.bar(
                regional_compare.sort_values(["corredor", "serie"]),
                x="corredor",
                y="valor",
                color="serie",
                barmode="group",
                title="Risco regional: composto base vs ajustado por fontes diretas",
            )
            st.plotly_chart(fig_regional_compare, width="stretch")
        st.dataframe(latest_regional, width="stretch", hide_index=True)
        if not top_entities.empty:
            st.subheader("Entidades Prioritarias")
            st.dataframe(top_entities, width="stretch", hide_index=True)

    with st.expander("Detalhes tecnicos do monitoramento regional"):
        if not direct_infrastructure_signals.empty:
            st.subheader("Sinais Diretos de Infraestrutura")
            st.dataframe(direct_infrastructure_signals, width="stretch", hide_index=True)
        if not monitored_entities.empty:
            st.subheader("Entidades Monitoradas")
            st.dataframe(monitored_entities, width="stretch", hide_index=True)
        if not source_catalog.empty:
            st.subheader("Fontes Oficiais Priorizadas")
            st.dataframe(source_catalog, width="stretch", hide_index=True)
        if not municipality_catalog.empty:
            st.subheader("Municipios Oficiais do IBGE")
            st.dataframe(municipality_catalog, width="stretch", hide_index=True)
        if not source_probe.empty:
            st.subheader("Probe de Fontes Oficiais")
            st.dataframe(source_probe.head(50), width="stretch", hide_index=True)
        if not asset_registry.empty:
            st.subheader("Registro de Ativos Oficiais")
            st.dataframe(asset_registry.head(50), width="stretch", hide_index=True)
        if not fetched_assets.empty:
            st.subheader("Ativos Oficiais Baixados")
            st.dataframe(fetched_assets.head(50), width="stretch", hide_index=True)
        if not official_api_catalog.empty:
            st.subheader("Catalogo de Datasets via API")
            st.dataframe(official_api_catalog.head(50), width="stretch", hide_index=True)
        if not official_signal_intelligence.empty:
            st.subheader("Inteligencia de Ativos Oficiais")
            st.dataframe(official_signal_intelligence.head(50), width="stretch", hide_index=True)
        if not download_candidates.empty:
            st.subheader("Candidatos de Download Descobertos")
            st.dataframe(download_candidates.head(50), width="stretch", hide_index=True)

with tab7:
    st.header("Historico Entre Ciclos")
    if forecasts_history.empty and regional_history.empty and alerts_history.empty:
        st.info("O historico entre ciclos aparece aqui depois da execucao de snapshots do pipeline.")
    else:
        cycle_rows = []
        history_frames = [
            ("regional_monitoring", regional_history),
            ("workforce_forecasts", forecasts_history),
            ("alerts_operacionais", alerts_history),
        ]
        for metric_name, frame in history_frames:
            if frame.empty or "snapshot_run_id" not in frame.columns:
                continue
            grouped = (
                frame.groupby("snapshot_run_id", as_index=False)
                .agg(snapshot_captured_at=("snapshot_captured_at", "max"), registros=("snapshot_run_id", "size"))
            )
            grouped["metric_name"] = metric_name
            cycle_rows.append(grouped)

        cycle_history = pd.concat(cycle_rows, ignore_index=True) if cycle_rows else pd.DataFrame()
        if cycle_history.empty:
            st.info("Ainda nao ha historico suficiente para comparacao entre ciclos.")
        else:
            latest_cycles = (
                cycle_history[["snapshot_run_id", "snapshot_captured_at"]]
                .drop_duplicates()
                .sort_values("snapshot_captured_at", ascending=False)
                .head(2)
            )
            history_cols = st.columns(3)
            history_cols[0].metric("Ciclos com snapshot", int(cycle_history["snapshot_run_id"].nunique()))

            latest_forecast_count = 0
            latest_alert_count = 0
            delta_forecast_count = None
            delta_alert_count = None

            latest_run_id = latest_cycles.iloc[0]["snapshot_run_id"]
            latest_forecast_count = int(
                cycle_history[
                    (cycle_history["snapshot_run_id"] == latest_run_id)
                    & (cycle_history["metric_name"] == "workforce_forecasts")
                ]["registros"].sum()
            )
            latest_alert_count = int(
                cycle_history[
                    (cycle_history["snapshot_run_id"] == latest_run_id)
                    & (cycle_history["metric_name"] == "alerts_operacionais")
                ]["registros"].sum()
            )

            if len(latest_cycles) > 1:
                previous_run_id = latest_cycles.iloc[1]["snapshot_run_id"]
                previous_forecast_count = int(
                    cycle_history[
                        (cycle_history["snapshot_run_id"] == previous_run_id)
                        & (cycle_history["metric_name"] == "workforce_forecasts")
                    ]["registros"].sum()
                )
                previous_alert_count = int(
                    cycle_history[
                        (cycle_history["snapshot_run_id"] == previous_run_id)
                        & (cycle_history["metric_name"] == "alerts_operacionais")
                    ]["registros"].sum()
                )
                delta_forecast_count = latest_forecast_count - previous_forecast_count
                delta_alert_count = latest_alert_count - previous_alert_count

            history_cols[1].metric("Previsoes no ultimo ciclo", latest_forecast_count, delta=delta_forecast_count)
            history_cols[2].metric("Alertas no ultimo ciclo", latest_alert_count, delta=delta_alert_count)

            fig_cycle_history = px.line(
                cycle_history.sort_values("snapshot_captured_at"),
                x="snapshot_captured_at",
                y="registros",
                color="metric_name",
                markers=True,
                title="Volume persistido por ciclo",
            )
            st.plotly_chart(fig_cycle_history, width="stretch")
            st.dataframe(
                cycle_history.sort_values(["snapshot_captured_at", "metric_name"], ascending=[False, True]),
                width="stretch",
                hide_index=True,
            )

with tab8:
    st.header("Modelo e Backtest")
    if model_performance_history.empty:
        st.info("As metricas historicas do modelo aparecem aqui depois da execucao completa do treino.")
    else:
        latest_model = model_performance_history.iloc[0]
        model_cols = st.columns(5)
        model_cols[0].metric("MAE atual", f"{latest_model['mae']:.2f}")
        model_cols[1].metric("MAPE atual", f"{latest_model['mape']:.4f}")
        model_cols[2].metric("R2 atual", f"{latest_model['r2']:.4f}")
        baseline_delta_mae = None
        if "baseline_mae" in latest_model.index and pd.notna(latest_model["baseline_mae"]):
            baseline_delta_mae = round(float(latest_model["baseline_mae"] - latest_model["mae"]), 2)
        model_cols[3].metric("MAE baseline", f"{latest_model['baseline_mae']:.2f}" if pd.notna(latest_model.get("baseline_mae")) else "-", delta=baseline_delta_mae)
        model_cols[4].metric("Folds de backtest", int(latest_model["backtest_folds"]) if pd.notna(latest_model["backtest_folds"]) else 0)

        if pd.notna(latest_model.get("baseline_mae")):
            if latest_model["mae"] < latest_model["baseline_mae"]:
                st.success(
                    f"O modelo atual superou o baseline no holdout: MAE {latest_model['mae']:.2f} vs {latest_model['baseline_mae']:.2f}."
                )
            else:
                st.warning(
                    f"O modelo atual nao superou o baseline no holdout: MAE {latest_model['mae']:.2f} vs {latest_model['baseline_mae']:.2f}."
                )
        if pd.notna(latest_model.get("backtest_baseline_mae_mean")):
            st.caption(
                "Backtest rolling: "
                f"modelo MAE medio {latest_model['backtest_mae_mean']:.2f} "
                f"vs baseline {latest_model['backtest_baseline_mae_mean']:.2f}."
            )

        perf_long = model_performance_history.melt(
            id_vars=["recorded_at"],
            value_vars=["mae", "baseline_mae"],
            var_name="metrica",
            value_name="valor",
        )
        fig_perf = px.line(
            perf_long.sort_values("recorded_at"),
            x="recorded_at",
            y="valor",
            color="metrica",
            markers=True,
            title="Historico de erro: modelo vs baseline",
        )
        st.plotly_chart(fig_perf, width="stretch")

        stability_long = model_performance_history.melt(
            id_vars=["recorded_at"],
            value_vars=["r2", "baseline_r2"],
            var_name="metrica",
            value_name="valor",
        )
        fig_stability = px.line(
            stability_long.sort_values("recorded_at"),
            x="recorded_at",
            y="valor",
            color="metrica",
            markers=True,
            title="Estabilidade explicativa: modelo vs baseline",
        )
        st.plotly_chart(fig_stability, width="stretch")

        if not model_backtest_folds.empty:
            latest_backtest = model_backtest_folds
            if "run_id" in model_backtest_folds.columns and pd.notna(latest_model.get("run_id")):
                latest_backtest = model_backtest_folds[model_backtest_folds["run_id"] == latest_model["run_id"]].copy()
            backtest_plot = latest_backtest.copy()
            fig_backtest = px.bar(
                backtest_plot,
                x="fold_id",
                y="mae",
                hover_data=["train_end_date", "test_start_date", "test_end_date", "mape", "r2", "baseline_mae", "baseline_mape", "baseline_r2"],
                title="MAE por fold do backtest rolling",
            )
            st.plotly_chart(fig_backtest, width="stretch")
            if {"baseline_mae", "fold_id"}.issubset(backtest_plot.columns):
                backtest_compare = backtest_plot.melt(
                    id_vars=["fold_id"],
                    value_vars=["mae", "baseline_mae"],
                    var_name="serie",
                    value_name="valor",
                )
                fig_backtest_compare = px.line(
                    backtest_compare.sort_values("fold_id"),
                    x="fold_id",
                    y="valor",
                    color="serie",
                    markers=True,
                    title="Backtest rolling: modelo vs baseline",
                )
                st.plotly_chart(fig_backtest_compare, width="stretch")
            st.dataframe(backtest_plot, width="stretch", hide_index=True)
        st.dataframe(model_performance_history, width="stretch", hide_index=True)

with tab9:
    st.header("Saude Operacional do Pipeline")
    if not source_registry.empty:
        st.subheader("Cobertura de APIs")
        api_first = source_registry[source_registry["preferred_ingestion_method"] == "api"].copy() if "preferred_ingestion_method" in source_registry.columns else pd.DataFrame()
        api_healthy = int(len(api_first[api_first["status"] == "success"])) if not api_first.empty else 0
        api_total = int(len(api_first)) if not api_first.empty else 0
        api_cols = st.columns(4)
        api_cols[0].metric("Fontes API-first", api_total)
        api_cols[1].metric("APIs saudaveis", api_healthy)
        api_cols[2].metric("Datasets via API", int(len(official_api_catalog[official_api_catalog["api_status"] == "success"])) if not official_api_catalog.empty and "api_status" in official_api_catalog.columns else 0)
        api_cols[3].metric("Ativos via fetch", int(len(fetched_assets[fetched_assets["fetch_status"] == "success"])) if not fetched_assets.empty and "fetch_status" in fetched_assets.columns else 0)
        if not api_first.empty:
            st.dataframe(
                api_first[
                    [
                        "source_name",
                        "source_type",
                        "preferred_ingestion_method",
                        "fallback_ingestion_method",
                        "last_ingestion_method",
                        "status",
                        "api_url",
                        "last_success_at",
                    ]
                ],
                width="stretch",
                hide_index=True,
            )
    if not source_registry.empty:
        healthy = int((source_registry["status"] == "success").sum())
        total = int(len(source_registry))
        failed = int((source_registry["status"] == "failed").sum())
        stale = int((source_registry["status"] == "stale").sum())
        queued = int(len(refresh_queue[refresh_queue["status"].isin(["queued", "running"])])) if not refresh_queue.empty else 0
        fetched_ok = int((fetched_assets["fetch_status"] == "success").sum()) if not fetched_assets.empty else 0
        op_cols = st.columns(5)
        op_cols[0].metric("Fontes registradas", total)
        op_cols[1].metric("Fontes saudaveis", healthy)
        op_cols[2].metric("Fontes stale", stale)
        op_cols[3].metric("Fontes falhas", failed)
        op_cols[4].metric("Ativos baixados", fetched_ok)
        st.caption(f"Jobs na fila: {queued}")
        cached_fallbacks = 0
        if not pipeline_state.empty and "state_key" in pipeline_state.columns:
            cached_fallbacks = int(pipeline_state["state_key"].astype(str).str.startswith("cached_fallback::").sum())
        st.caption(f"Fallbacks locais reaproveitados: {cached_fallbacks}")
        if not ingestion_runs.empty:
            st.subheader("Execucoes Recentes")
            st.dataframe(ingestion_runs.head(10), width="stretch", hide_index=True)
        if not pipeline_run_summaries.empty:
            latest_cycle = pipeline_run_summaries.iloc[0]
            cycle_cols = st.columns(4)
            cycle_cols[0].metric("Duracao ultimo ciclo", f"{int(latest_cycle['duration_seconds'])}s" if pd.notna(latest_cycle["duration_seconds"]) else "-")
            cycle_cols[1].metric("Fallbacks locais", int(latest_cycle["cached_fallbacks"]) if pd.notna(latest_cycle["cached_fallbacks"]) else 0)
            cycle_cols[2].metric("Falhas criticas de qualidade", int(latest_cycle["critical_quality_failures"]) if pd.notna(latest_cycle["critical_quality_failures"]) else 0)
            cycle_cols[3].metric("Warnings de qualidade", int(latest_cycle["quality_warnings"]) if pd.notna(latest_cycle["quality_warnings"]) else 0)
            st.subheader("Resumo dos Ciclos")
            st.dataframe(pipeline_run_summaries.head(20), width="stretch", hide_index=True)

    with st.expander("Detalhes tecnicos do pipeline"):
        if not source_registry.empty:
            st.subheader("Registro de Fontes")
            st.dataframe(source_registry, width="stretch", hide_index=True)
        if not source_policy.empty:
            st.subheader("Politicas de Refresh")
            st.dataframe(source_policy, width="stretch", hide_index=True)
        if not refresh_queue.empty:
            st.subheader("Fila de Atualizacao")
            st.dataframe(refresh_queue.head(30), width="stretch", hide_index=True)
        if not asset_fetch_log.empty:
            st.subheader("Log de Fetch de Ativos")
            st.dataframe(asset_fetch_log.head(30), width="stretch", hide_index=True)
        if not pipeline_state.empty:
            st.subheader("Estado do Pipeline")
            st.dataframe(pipeline_state, width="stretch", hide_index=True)

with tab10:
    st.header("Qualidade dos Dados")
    if data_quality_checks.empty:
        st.info("Os checks de qualidade aparecem aqui depois da execucao completa do pipeline.")
    else:
        failed_total = int((data_quality_checks["status"] == "fail").sum())
        warned_total = int((data_quality_checks["status"] == "warn").sum())
        passed_total = int((data_quality_checks["status"] == "pass").sum())
        high_failed = int(len(data_quality_checks[(data_quality_checks["status"] == "fail") & (data_quality_checks["severity"] == "high")]))
        q_cols = st.columns(4)
        q_cols[0].metric("Checks executados", len(data_quality_checks))
        q_cols[1].metric("Falhas criticas", high_failed)
        q_cols[2].metric("Falhas totais", failed_total)
        q_cols[3].metric("Warnings", warned_total)

        quality_summary = (
            data_quality_checks.groupby(["status", "severity"], as_index=False)
            .size()
            .rename(columns={"size": "checks"})
        )
        st.caption(f"Checks aprovados: {passed_total}")
        st.dataframe(quality_summary, width="stretch", hide_index=True)
        st.dataframe(data_quality_checks, width="stretch", hide_index=True)
