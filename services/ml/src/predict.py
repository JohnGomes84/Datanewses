import sqlite3
from pathlib import Path
from urllib.parse import unquote, urlparse

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import yaml

from shared import config, get_logger
from services.ml.src.modeling import FEATURE_COLUMNS

logger = get_logger(__name__)


def _uri_to_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"Unsupported model URI scheme: {parsed.scheme}")
    return Path(unquote(parsed.path.lstrip("/")))


def _get_latest_model_path():
    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    model_meta_files = sorted(
        Path(config.MODEL_DIR).glob("**/models/*/meta.yaml"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for meta_file in model_meta_files:
        with meta_file.open("r", encoding="utf-8") as fh:
            meta = yaml.safe_load(fh) or {}
        artifact_location = meta.get("artifact_location")
        if artifact_location:
            model_dir = _uri_to_path(artifact_location)
            if model_dir.exists():
                return model_dir
    return None


def _build_future_frame(operations):
    last_date = operations["data"].max()
    recent = operations[operations["data"] >= last_date - pd.Timedelta(days=42)].copy()
    profiles = (
        recent.groupby(["unidade", "cliente", "tipo_operacao", "turno"], as_index=False)
        .agg(
            volume_toneladas=("volume_toneladas", "mean"),
            cargas_previstas=("cargas_previstas", "mean"),
            demanda_externa_index=("demanda_externa_index", "mean"),
            pressao_mao_obra_index=("pressao_mao_obra_index", "mean"),
            combustivel_index=("combustivel_index", "mean"),
            chuva_mm=("chuva_mm", "mean"),
            news_risk_score=("news_risk_score", "mean"),
            rodovias_trafego_index=("rodovias_trafego_index", "mean"),
            porto_fila_index=("porto_fila_index", "mean"),
            aeroporto_carga_index=("aeroporto_carga_index", "mean"),
            fiscal_emissao_index=("fiscal_emissao_index", "mean"),
            interdicao_prob=("interdicao_prob", "mean"),
            infraestrutura_risk_index=("infraestrutura_risk_index", "mean"),
            absenteismo_pct=("absenteismo_pct", "mean"),
            trabalhadores_planejados=("trabalhadores_planejados", "mean"),
            receita_estimada=("receita_estimada", "mean"),
            custo_operacional=("custo_operacional", "mean"),
            sla_realizado=("sla_realizado", "mean"),
        )
    )

    weekday_multiplier = {0: 1.05, 1: 1.08, 2: 1.1, 3: 1.03, 4: 0.97, 5: 0.74, 6: 0.62}
    rng = np.random.default_rng(99)
    rows = []
    for horizon in range(1, 15):
        future_date = last_date + pd.Timedelta(days=horizon)
        for row in profiles.itertuples(index=False):
            demand_mult = weekday_multiplier[future_date.dayofweek] * (1.07 if future_date.day >= 25 else 1.0) * rng.normal(1.0, 0.04)
            volume = max(40.0, row.volume_toneladas * demand_mult)
            cargas = max(2, int(round(row.cargas_previstas * demand_mult)))
            labor_pressure = float(np.clip(row.pressao_mao_obra_index + rng.normal(0.0, 0.03), 0.15, 0.95))
            news_risk = float(np.clip(row.news_risk_score + rng.normal(0.0, 0.05), 0.12, 0.92))
            chuva_mm = max(0.0, row.chuva_mm + rng.normal(0.0, 2.5))
            absenteismo = float(np.clip(row.absenteismo_pct + labor_pressure * 0.02 + rng.normal(0.0, 0.01), 0.02, 0.22))
            rows.append(
                {
                    "data": future_date,
                    "unidade": row.unidade,
                    "cliente": row.cliente,
                    "tipo_operacao": row.tipo_operacao,
                    "turno": row.turno,
                    "dia_semana_num": future_date.dayofweek,
                    "mes": future_date.month,
                    "is_month_end": int(future_date.is_month_end),
                    "volume_toneladas": round(volume, 2),
                    "cargas_previstas": cargas,
                    "demanda_externa_index": round(float(np.clip(row.demanda_externa_index + rng.normal(0.0, 0.04), 0.2, 1.0)), 4),
                    "pressao_mao_obra_index": round(labor_pressure, 4),
                    "combustivel_index": round(float(np.clip(row.combustivel_index + rng.normal(0.0, 0.03), 0.2, 1.0)), 4),
                    "chuva_mm": round(chuva_mm, 2),
                    "news_risk_score": round(news_risk, 4),
                    "rodovias_trafego_index": round(float(np.clip(row.rodovias_trafego_index + rng.normal(0.0, 0.05), 0.05, 1.0)), 4),
                    "porto_fila_index": round(float(np.clip(row.porto_fila_index + rng.normal(0.0, 0.05), 0.03, 1.0)), 4),
                    "aeroporto_carga_index": round(float(np.clip(row.aeroporto_carga_index + rng.normal(0.0, 0.03), 0.02, 1.0)), 4),
                    "fiscal_emissao_index": round(float(np.clip(row.fiscal_emissao_index + rng.normal(0.0, 0.04), 0.08, 1.0)), 4),
                    "interdicao_prob": round(float(np.clip(row.interdicao_prob + rng.normal(0.0, 0.03), 0.02, 0.95)), 4),
                    "infraestrutura_risk_index": round(float(np.clip(row.infraestrutura_risk_index + rng.normal(0.0, 0.03), 0.05, 1.0)), 4),
                    "absenteismo_pct": round(absenteismo, 4),
                    "capacidade_atual": int(round(row.trabalhadores_planejados)),
                    "receita_base": float(row.receita_estimada),
                    "custo_base": float(row.custo_operacional),
                    "sla_base": float(row.sla_realizado),
                }
            )
    return pd.DataFrame(rows)


def _build_insights(forecasts, alerts):
    insights = []
    total_gap = int(forecasts["gap_previsto"].clip(lower=0).sum())
    hottest_unit = forecasts.groupby("unidade", as_index=False)["gap_previsto"].sum().sort_values("gap_previsto", ascending=False).iloc[0]
    best_client = forecasts.groupby("cliente", as_index=False)["margem_prevista"].sum().sort_values("margem_prevista", ascending=False).iloc[0]
    risk_shift = forecasts.groupby("turno", as_index=False)["risk_score"].mean().sort_values("risk_score", ascending=False).iloc[0]

    insights.append({"insight_type": "staffing", "priority": "alta", "title": "Gap de equipe previsto", "detail": f"O horizonte de 14 dias indica deficit acumulado de {total_gap} colaboradores em operacoes criticas."})
    insights.append({"insight_type": "unit", "priority": "alta" if hottest_unit["gap_previsto"] > 0 else "media", "title": "Unidade mais pressionada", "detail": f"{hottest_unit['unidade']} concentra o maior gap previsto, somando {int(hottest_unit['gap_previsto'])} postos a cobrir."})
    insights.append({"insight_type": "commercial", "priority": "media", "title": "Melhor oportunidade comercial", "detail": f"{best_client['cliente']} lidera a margem prevista no periodo, indicando prioridade de atendimento e retencao."})
    insights.append({"insight_type": "risk", "priority": "alta" if risk_shift["risk_score"] >= 70 else "media", "title": "Turno mais sensivel", "detail": f"O turno {risk_shift['turno']} apresenta risco medio de {risk_shift['risk_score']:.1f}, exigindo reforco de supervisao e contingencia."})
    if not alerts.empty:
        top_alert = alerts.sort_values("risk_score", ascending=False).iloc[0]
        insights.append({"insight_type": "alert", "priority": "alta", "title": "Alerta prioritario", "detail": f"{top_alert['unidade']} / {top_alert['cliente']} no turno {top_alert['turno']} demanda acao imediata: {top_alert['acao_recomendada']}."})
    frame = pd.DataFrame(insights)
    frame["generated_at"] = pd.Timestamp.now()
    return frame


def predict_next():
    model_path = _get_latest_model_path()
    if model_path is None:
        logger.warning("No trained model found")
        return

    model = mlflow.sklearn.load_model(str(model_path))

    conn = sqlite3.connect(config.SQLITE_DB)
    operations = pd.read_sql_query("SELECT * FROM operations_daily ORDER BY data", conn)
    conn.close()
    if operations.empty:
        logger.warning("No operational data available for forecasting")
        return

    operations["data"] = pd.to_datetime(operations["data"])
    future = _build_future_frame(operations)
    future["trabalhadores_previstos"] = np.round(model.predict(future[FEATURE_COLUMNS])).astype(int)
    future["gap_previsto"] = future["trabalhadores_previstos"] - future["capacidade_atual"]
    future["sla_previsto"] = np.clip(future["sla_base"] - future["gap_previsto"].clip(lower=0) * 0.013 - future["absenteismo_pct"] * 0.08, 0.74, 0.995)
    future["receita_prevista"] = (future["receita_base"] / future["volume_toneladas"].clip(lower=1)) * future["volume_toneladas"]
    future["custo_previsto"] = future["custo_base"] * (np.maximum(future["capacidade_atual"], future["trabalhadores_previstos"]) / future["capacidade_atual"].clip(lower=1))
    future["margem_prevista"] = future["receita_prevista"] - future["custo_previsto"]
    future["risk_score"] = np.clip(32 + future["gap_previsto"].clip(lower=0) * 8 + future["absenteismo_pct"] * 100 + future["news_risk_score"] * 22 + future["infraestrutura_risk_index"] * 22 + (1 - future["sla_previsto"]) * 100, 0, 100)
    future["acao_recomendada"] = np.select(
        [future["gap_previsto"] >= 6, future["risk_score"] >= 75, future["gap_previsto"] >= 2],
        ["Acionar banco de reserva e horas extras controladas", "Reforcar lideranca operacional e redistribuir equipes", "Ajustar escala preventiva por turno"],
        default="Operacao sob controle",
    )

    forecasts = future[["data", "unidade", "cliente", "tipo_operacao", "turno", "volume_toneladas", "cargas_previstas", "trabalhadores_previstos", "capacidade_atual", "gap_previsto", "sla_previsto", "receita_prevista", "custo_previsto", "margem_prevista", "risk_score", "acao_recomendada"]].copy()
    alerts = forecasts[(forecasts["risk_score"] >= 72) | (forecasts["gap_previsto"] >= 4)].copy()
    alerts["alerta"] = np.where(alerts["gap_previsto"] >= 4, "Deficit de equipe previsto", "Risco elevado de desempenho operacional")
    alerts = alerts.sort_values(["risk_score", "gap_previsto"], ascending=[False, False]).reset_index(drop=True)
    insights = _build_insights(forecasts, alerts)

    conn = sqlite3.connect(config.SQLITE_DB)
    forecasts.to_sql("workforce_forecasts", conn, if_exists="replace", index=False)
    alerts.to_sql("alerts_operacionais", conn, if_exists="replace", index=False)
    insights.to_sql("executive_insights", conn, if_exists="replace", index=False)
    conn.close()

    logger.info(f"Generated {len(forecasts)} workforce forecasts and {len(alerts)} alerts")


if __name__ == "__main__":
    predict_next()
