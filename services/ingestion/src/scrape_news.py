import html
import re

import pandas as pd
import requests

from shared import config, get_logger, save_parquet

logger = get_logger(__name__)

NEWS_SOURCES = [
    ("ANAC", "https://www.gov.br/anac/pt-br/noticias/ultimas-noticias-1"),
    ("ANTAQ", "https://www.gov.br/antaq/pt-br/noticias/2026"),
    ("ANTT", "https://www.gov.br/antt/pt-br/assuntos/ultimas-noticias"),
]


def _clean_text(raw):
    text = re.sub(r"<[^>]+>", " ", raw)
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def _classify_theme(title):
    lower = title.lower()
    if any(token in lower for token in ["porto", "portuario", "aquavi", "terminal"]):
        return "porto", 0.58
    if any(token in lower for token in ["aero", "aviação", "aviacao", "cargas", "transporte aéreo", "transporte aereo"]):
        return "aeroporto", 0.52
    if any(token in lower for token in ["rodovia", "rodovi", "ferrovia", "infraestrutura", "leilão", "leilao"]):
        return "infraestrutura", 0.61
    if any(token in lower for token in ["export", "comércio exterior", "comercio exterior", "movimentações", "movimentacoes"]):
        return "demanda", 0.55
    return "regulatorio", 0.34


def _extract_items(source_name, url):
    text = requests.get(url, timeout=45, headers={"User-Agent": "nowcasting-ai/1.0"}).text
    matches = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', text, flags=re.I | re.S)
    rows = []
    seen = set()
    for href, raw_title in matches:
        if source_name == "ANAC":
            if "/pt-br/noticias/2026/" not in href and "/pt-br/noticias/notas-oficiais" not in href:
                continue
        elif source_name == "ANTAQ":
            if "/pt-br/noticias/2026/" not in href:
                continue
        elif source_name == "ANTT":
            if "/pt-br/assuntos/ultimas-noticias/" not in href:
                continue
        title = _clean_text(raw_title)
        if len(title) < 24 or href in seen:
            continue
        seen.add(href)
        article_html = requests.get(href, timeout=45, headers={"User-Agent": "nowcasting-ai/1.0"}).text
        published = re.search(r'"datePublished"\s*:\s*"([^"]+)"', article_html)
        published_at = pd.to_datetime(published.group(1), errors="coerce") if published else pd.Timestamp.today().normalize()
        tema, base_risk = _classify_theme(title)
        rows.append(
            {
                "data": published_at.strftime("%Y-%m-%d"),
                "titulo": title,
                "tema": tema,
                "risk_score": round(base_risk, 2),
                "sentimento": "negativo" if base_risk >= 0.6 else "neutro",
                "origem": href,
                "fonte": source_name,
            }
        )
        if len(rows) >= 10:
            break
    return rows


def scrape_economic_news():
    rows = []
    for source_name, url in NEWS_SOURCES:
        rows.extend(_extract_items(source_name, url))

    df = pd.DataFrame(rows).drop_duplicates(subset=["titulo", "origem"]).sort_values("data", ascending=False).head(24).reset_index(drop=True)
    save_parquet(df, f"{config.DATA_DIR}/bronze/news_raw", filename="noticias_operacionais.parquet")
    logger.info(f"Saved {len(df)} real official operational news records")
    return df
