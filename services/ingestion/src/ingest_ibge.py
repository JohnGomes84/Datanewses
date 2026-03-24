import pandas as pd
import requests

from shared import config, get_logger, save_parquet

logger = get_logger(__name__)

IBGE_ES_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/32/municipios"


def fetch_es_localities():
    response = requests.get(IBGE_ES_MUNICIPIOS_URL, timeout=45, headers={"User-Agent": "nowcasting-ai/1.0"})
    response.raise_for_status()
    payload = response.json()

    rows = []
    collected_at = pd.Timestamp.now().isoformat()
    for item in payload:
        microrregiao = item.get("microrregiao") or {}
        mesorregiao = microrregiao.get("mesorregiao") or {}
        regiao_imediata = item.get("regiao-imediata") or {}
        regiao_intermediaria = regiao_imediata.get("regiao-intermediaria") or {}
        rows.append(
            {
                "municipio_id": int(item["id"]),
                "municipio": item["nome"],
                "uf": "ES",
                "microrregiao": microrregiao.get("nome"),
                "mesorregiao": mesorregiao.get("nome"),
                "regiao_imediata": regiao_imediata.get("nome"),
                "regiao_intermediaria": regiao_intermediaria.get("nome"),
                "source_url": IBGE_ES_MUNICIPIOS_URL,
                "ingestion_method": "api",
                "collected_at": collected_at,
            }
        )

    df = pd.DataFrame(rows).sort_values("municipio").reset_index(drop=True)
    save_parquet(df, f"{config.DATA_DIR}/bronze/ibge", filename="municipios_es.parquet")
    logger.info(f"Saved {len(df)} IBGE municipalities for ES")
    return df
