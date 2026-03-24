import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


FEATURE_COLUMNS = [
    "unidade",
    "cliente",
    "tipo_operacao",
    "turno",
    "dia_semana_num",
    "mes",
    "is_month_end",
    "volume_toneladas",
    "cargas_previstas",
    "demanda_externa_index",
    "pressao_mao_obra_index",
    "combustivel_index",
    "chuva_mm",
    "news_risk_score",
    "rodovias_trafego_index",
    "porto_fila_index",
    "aeroporto_carga_index",
    "fiscal_emissao_index",
    "interdicao_prob",
    "infraestrutura_risk_index",
    "absenteismo_pct",
]


def prepare_training_frame(df):
    frame = df.copy()
    frame["data"] = pd.to_datetime(frame["data"])
    frame["dia_semana_num"] = frame["data"].dt.dayofweek
    frame["is_month_end"] = frame["data"].dt.is_month_end.astype(int)
    return frame


def build_model_pipeline():
    categorical_features = ["unidade", "cliente", "tipo_operacao", "turno"]
    numeric_features = [col for col in FEATURE_COLUMNS if col not in categorical_features]

    return Pipeline(
        steps=[
            (
                "preprocessor",
                ColumnTransformer(
                    transformers=[
                        ("categorical", OneHotEncoder(handle_unknown="ignore"), categorical_features),
                        ("numeric", "passthrough", numeric_features),
                    ]
                ),
            ),
            ("model", RandomForestRegressor(n_estimators=320, min_samples_leaf=2, random_state=42, n_jobs=1)),
        ]
    )


def predict_group_mean_baseline(train_df, test_df, target_column="trabalhadores_necessarios"):
    group_keys = ["unidade", "cliente", "tipo_operacao", "turno"]
    group_means = train_df.groupby(group_keys, as_index=False)[target_column].mean().rename(
        columns={target_column: "baseline_prediction"}
    )
    fallback_value = float(train_df[target_column].mean())
    merged = test_df[group_keys].merge(group_means, on=group_keys, how="left")
    return merged["baseline_prediction"].fillna(fallback_value).to_numpy()
