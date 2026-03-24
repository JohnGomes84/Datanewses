import pandas as pd

from shared import config, get_logger, save_parquet

logger = get_logger(__name__)


API_SOURCE_MAP = {
    "ANTT Rodovias": "https://dados.antt.gov.br/api/3/action/package_search?fq=groups:rodovias",
    "DNIT Dados Abertos": "https://servicos.dnit.gov.br/dadosabertos/api/3/action/package_search?fq=organization:dnit",
}


def build_source_catalog():
    sources = pd.DataFrame(
        [
            {"source_name": "ANTT Rodovias", "category": "rodovia", "scope": "Concessoes, trechos, trafego, eventos e ativos rodoviarios", "provider": "Agencia Nacional de Transportes Terrestres", "url": "https://dados.antt.gov.br/group/rodovias", "format_hint": "CSV/JSON", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "api", "fallback_ingestion_method": "probe_fetch", "api_url": API_SOURCE_MAP.get("ANTT Rodovias")},
            {"source_name": "ANTAQ Estatistica", "category": "porto", "scope": "Movimentacao portuaria, estatistico aquaviario e situacao dos portos", "provider": "Agencia Nacional de Transportes Aquaviarios", "url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica", "format_hint": "Painel/BI", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "probe_fetch", "fallback_ingestion_method": "manual_portal", "api_url": None},
            {"source_name": "ANAC Movimentacao Aeroportuaria", "category": "aeroporto", "scope": "Movimentacao de passageiros, cargas e aeronaves", "provider": "Agencia Nacional de Aviacao Civil", "url": "https://www.anac.gov.br/acesso-a-informacao/dados-abertos/areas-de-atuacao/operador-aeroportuario/dados-de-movimentacao-aeroportuaria", "format_hint": "CSV/JSON", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "probe_fetch", "fallback_ingestion_method": "manual_portal", "api_url": None},
            {"source_name": "SEFAZ-ES Documentos Fiscais", "category": "fiscal", "scope": "CT-e, NFC-e e outras bases abertas da SEFAZ-ES", "provider": "Secretaria da Fazenda do Espirito Santo", "url": "https://sefaz.es.gov.br/GrupodeArquivos/base-de-dados-documentos-fiscais", "format_hint": "XLSX", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "probe_fetch", "fallback_ingestion_method": "manual_portal", "api_url": None},
            {"source_name": "SEFAZ-ES NF-e Estatisticas", "category": "fiscal", "scope": "Estatisticas de NF-e e emitentes no ES", "provider": "Secretaria da Fazenda do Espirito Santo", "url": "https://internet.sefaz.es.gov.br/informacoes/nfe/estatisticas.php", "format_hint": "Pagina institucional", "priority": "media", "status": "validado", "source_type": "official", "preferred_ingestion_method": "probe_fetch", "fallback_ingestion_method": "manual_portal", "api_url": None},
            {"source_name": "DNIT Dados Abertos", "category": "rodovia", "scope": "Infraestrutura, manutencao e trechos rodoviarios federais", "provider": "Departamento Nacional de Infraestrutura de Transportes", "url": "https://servicos.dnit.gov.br/dadosabertos/", "format_hint": "CSV/JSON", "priority": "alta", "status": "catalogado", "source_type": "official", "preferred_ingestion_method": "api", "fallback_ingestion_method": "probe_fetch", "api_url": API_SOURCE_MAP.get("DNIT Dados Abertos")},
            {"source_name": "IBGE Localidades ES", "category": "territorio", "scope": "Municipios do Espirito Santo com codigos e hierarquia territorial oficial", "provider": "Instituto Brasileiro de Geografia e Estatistica", "url": "https://servicodados.ibge.gov.br/api/v1/localidades/estados/32/municipios", "format_hint": "JSON API", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "api", "fallback_ingestion_method": "cached_copy", "api_url": "https://servicodados.ibge.gov.br/api/v1/localidades/estados/32/municipios"},
            {"source_name": "BCB Market Indicators", "category": "macro", "scope": "Cambio e juros oficiais do Banco Central para proxies operacionais de demanda, custo e pressao macro", "provider": "Banco Central do Brasil", "url": "https://dadosabertos.bcb.gov.br/dataset/11-taxa-de-juros---selic", "format_hint": "JSON API", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "api", "fallback_ingestion_method": "cached_copy", "api_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"},
            {"source_name": "MDIC Export Demand", "category": "comex", "scope": "Exportacoes municipais do ES para sinalizar demanda externa operacional", "provider": "Ministerio do Desenvolvimento, Industria, Comercio e Servicos", "url": "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta", "format_hint": "CSV anual", "priority": "alta", "status": "validado", "source_type": "official", "preferred_ingestion_method": "file_download", "fallback_ingestion_method": "cached_copy", "api_url": "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/EXP_{ano}_MUN.csv"},
            {"source_name": "Official Transport News", "category": "news", "scope": "Noticias institucionais de ANTT, ANTAQ e ANAC para risco operacional", "provider": "Agencias setoriais federais", "url": "https://www.gov.br/antt/pt-br/assuntos/ultimas-noticias", "format_hint": "HTML institucional", "priority": "media", "status": "validado", "source_type": "official", "preferred_ingestion_method": "web_fetch", "fallback_ingestion_method": "cached_copy", "api_url": None},
            {"source_name": "Regional Monitoring Derived", "category": "regional", "scope": "Composto operacional de corredores baseado em demanda real, noticias oficiais e ajuste climatico complementar", "provider": "Pipeline analitico interno sobre fontes oficiais", "url": "", "format_hint": "Derived signals", "priority": "alta", "status": "validado", "source_type": "derived", "preferred_ingestion_method": "derived_official_signals", "fallback_ingestion_method": "cached_copy", "api_url": None},
            {"source_name": "INMET Regional Forecast", "category": "clima", "scope": "Previsao oficial municipal do INMET usada como enriquecimento climatico complementar dos corredores do ES", "provider": "Instituto Nacional de Meteorologia", "url": "https://portal.inmet.gov.br", "format_hint": "JSON API", "priority": "media", "status": "validado", "source_type": "official", "preferred_ingestion_method": "api", "fallback_ingestion_method": "cached_copy", "api_url": "https://apiprevmet3.inmet.gov.br/previsao/{geocode}/3"},
        ]
    )

    monitored_entities = pd.DataFrame(
        [
            {"entity_name": "Topcargo", "entity_type": "operador_logistico", "region": "Serra/ES", "focus": "Armazenagem, transporte e estoque", "source_url": "https://topcargo.com.br/sobre/"},
            {"entity_name": "ComLog", "entity_type": "operador_logistico", "region": "ES", "focus": "Armazenagem integrada e conexao multimodal", "source_url": "https://www.comlog.com.br/"},
            {"entity_name": "B4You Log", "entity_type": "operador_logistico", "region": "TIMS / Serra", "focus": "Recebimento, descarga, armazenagem e expedicao", "source_url": "https://b4youlog.com/logistica/"},
            {"entity_name": "TSG Log", "entity_type": "operador_logistico", "region": "Cariacica/ES", "focus": "Armazenagem e distribuicao", "source_url": "https://tsglog.com.br/armazenagem/"},
            {"entity_name": "VOL Logistics", "entity_type": "terminal_operador", "region": "Vila Velha/ES", "focus": "Carga, descarga, armazenagem e distribuicao portuaria", "source_url": "https://vollogistics.com.br/index.php/quemsomos/"},
            {"entity_name": "SA Express", "entity_type": "operador_logistico", "region": "ES", "focus": "Armazenagem, distribuicao e logistica reversa", "source_url": "https://saexpress.com.br/"},
            {"entity_name": "BR-101", "entity_type": "infraestrutura", "region": "ES", "focus": "Corredor rodoviario critico", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "BR-262", "entity_type": "infraestrutura", "region": "ES", "focus": "Acesso logistico a Cariacica, Viana e Vila Velha", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "BR-447", "entity_type": "infraestrutura", "region": "Vila Velha/ES", "focus": "Acesso portuario e industrial", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "Porto de Vitoria", "entity_type": "infraestrutura", "region": "Vitoria/ES", "focus": "Embarque, descarga e acesso portuario", "source_url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica"},
            {"entity_name": "Capuaba", "entity_type": "infraestrutura", "region": "Vila Velha/ES", "focus": "Retroarea e operacao portuaria", "source_url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica"},
            {"entity_name": "Tubarão / Praia Mole", "entity_type": "infraestrutura", "region": "Vitoria/ES", "focus": "Fila portuaria e operacao mineral/siderurgica", "source_url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica"},
            {"entity_name": "Portocel / Barra do Riacho", "entity_type": "infraestrutura", "region": "Aracruz/ES", "focus": "Celulose, exportacao e logistica portuaria", "source_url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica"},
            {"entity_name": "Aeroporto de Vitoria", "entity_type": "infraestrutura", "region": "Vitoria/ES", "focus": "Carga aerea e pressao aeroportuaria", "source_url": "https://www.anac.gov.br/acesso-a-informacao/dados-abertos/areas-de-atuacao/operador-aeroportuario/dados-de-movimentacao-aeroportuaria"},
            {"entity_name": "CT-e", "entity_type": "sinal_fiscal", "region": "ES", "focus": "Pulso de transporte e expedicao", "source_url": "https://sefaz.es.gov.br/GrupodeArquivos/base-de-dados-documentos-fiscais"},
            {"entity_name": "NF-e", "entity_type": "sinal_fiscal", "region": "ES", "focus": "Pulso fiscal de producao e expedicao", "source_url": "https://internet.sefaz.es.gov.br/informacoes/nfe/estatisticas.php"},
            {"entity_name": "NFC-e", "entity_type": "sinal_fiscal", "region": "ES", "focus": "Pulso de varejo e consumo", "source_url": "https://sefaz.es.gov.br/GrupodeArquivos/base-de-dados-documentos-fiscais"},
            {"entity_name": "Serra", "entity_type": "municipio_logistico", "region": "ES", "focus": "Polo industrial, CDs e acessos rodoviarios", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "Cariacica", "entity_type": "municipio_logistico", "region": "ES", "focus": "Entrocamento rodoviario e distribuicao", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "Viana", "entity_type": "municipio_logistico", "region": "ES", "focus": "Retroarea e condominios logisticos", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "Vila Velha", "entity_type": "municipio_logistico", "region": "ES", "focus": "Porto, capuaba e hubs de distribuicao", "source_url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica"},
            {"entity_name": "Vitoria", "entity_type": "municipio_logistico", "region": "ES", "focus": "Porto, aeroporto e conexao urbana", "source_url": "https://www.anac.gov.br/acesso-a-informacao/dados-abertos/areas-de-atuacao/operador-aeroportuario/dados-de-movimentacao-aeroportuaria"},
            {"entity_name": "Aracruz", "entity_type": "municipio_logistico", "region": "ES", "focus": "Porto e polo industrial/exportador", "source_url": "https://www.gov.br/antaq/pt-br/assuntos/estatistica"},
            {"entity_name": "Linhares", "entity_type": "municipio_logistico", "region": "ES", "focus": "Expansao industrial e corredor BR-101", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "Colatina", "entity_type": "municipio_logistico", "region": "ES", "focus": "Interiorizacao e redistribuicao", "source_url": "https://dados.antt.gov.br/group/rodovias"},
            {"entity_name": "Cachoeiro de Itapemirim", "entity_type": "municipio_logistico", "region": "ES", "focus": "Base industrial e ligacao sul", "source_url": "https://dados.antt.gov.br/group/rodovias"},
        ]
    )

    save_parquet(sources, f"{config.DATA_DIR}/bronze/catalog", filename="source_catalog.parquet")
    save_parquet(monitored_entities, f"{config.DATA_DIR}/bronze/catalog", filename="monitored_entities.parquet")
    logger.info(f"Saved {len(sources)} official sources and {len(monitored_entities)} monitored entities")
