import numpy as np
import pandas as pd
import requests

from shared import config, get_logger, save_parquet

logger = get_logger(__name__)

BCB_SERIES = {
    "usd_brl_venda": 1,
    "selic_diaria": 11,
    "selic_meta": 432,
}


def _fetch_sgs_series(series_code, start_date, end_date):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    response = requests.get(
        url,
        params={
            "formato": "json",
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": end_date.strftime("%d/%m/%Y"),
        },
        timeout=45,
        headers={"User-Agent": "nowcasting-ai/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return pd.DataFrame(columns=["data", "valor"])

    frame = pd.DataFrame(payload)
    frame["data"] = pd.to_datetime(frame["data"], format="%d/%m/%Y", errors="coerce")
    frame["valor"] = pd.to_numeric(frame["valor"], errors="coerce")
    frame = frame.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
    return frame


def _normalize(series, min_value, max_value):
    clipped = series.clip(lower=min_value, upper=max_value)
    if max_value == min_value:
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (clipped - min_value) / (max_value - min_value)


def fetch_bcb_series(series_codes=None):
    end_date = pd.Timestamp.today().normalize()
    start_date = end_date - pd.Timedelta(days=260)
    dates = pd.date_range(end=end_date, periods=210, freq="D")

    resolved_codes = dict(BCB_SERIES)
    if isinstance(series_codes, dict):
        resolved_codes.update(series_codes)

    usd = _fetch_sgs_series(resolved_codes["usd_brl_venda"], start_date, end_date).rename(columns={"valor": "usd_brl_venda"})
    selic_daily = _fetch_sgs_series(resolved_codes["selic_diaria"], start_date, end_date).rename(columns={"valor": "selic_diaria"})
    selic_meta = _fetch_sgs_series(resolved_codes["selic_meta"], start_date, end_date).rename(columns={"valor": "selic_meta"})

    merged = pd.DataFrame({"data": dates})
    for frame in [usd, selic_daily, selic_meta]:
        merged = merged.merge(frame, on="data", how="left")

    merged = merged.sort_values("data")
    merged[["usd_brl_venda", "selic_diaria", "selic_meta"]] = merged[["usd_brl_venda", "selic_diaria", "selic_meta"]].ffill().bfill()

    cambio_norm = _normalize(merged["usd_brl_venda"], merged["usd_brl_venda"].quantile(0.05), merged["usd_brl_venda"].quantile(0.95))
    selic_norm = _normalize(merged["selic_meta"], merged["selic_meta"].quantile(0.05), merged["selic_meta"].quantile(0.95))
    cambio_vol = merged["usd_brl_venda"].pct_change().rolling(5, min_periods=2).std().fillna(0.0)
    cambio_vol_norm = _normalize(cambio_vol, cambio_vol.quantile(0.05), max(cambio_vol.quantile(0.95), 1e-6))

    merged["demanda_externa_index"] = np.clip(0.42 + cambio_norm * 0.38 - selic_norm * 0.16, 0.2, 1.0)
    merged["pressao_mao_obra_index"] = np.clip(0.28 + selic_norm * 0.32 + cambio_vol_norm * 0.28 + cambio_norm * 0.12, 0.15, 0.95)
    merged["combustivel_index"] = np.clip(0.24 + cambio_norm * 0.68 + selic_norm * 0.08, 0.2, 1.0)
    merged["chuva_mm"] = 0.0
    merged["origem"] = "bcb_sgs_api"

    output = merged[
        [
            "data",
            "pressao_mao_obra_index",
            "demanda_externa_index",
            "combustivel_index",
            "chuva_mm",
            "usd_brl_venda",
            "selic_diaria",
            "selic_meta",
            "origem",
        ]
    ].copy()
    output["data"] = output["data"].dt.strftime("%Y-%m-%d")

    save_parquet(output, f"{config.DATA_DIR}/bronze/bcb", filename="indicadores_mercado.parquet")
    logger.info(f"Saved {len(output)} real BCB market indicator records")
    return output
