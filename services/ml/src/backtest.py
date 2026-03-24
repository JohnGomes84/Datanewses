import sqlite3

import pandas as pd
from sklearn.base import clone
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

from shared import config
from services.ml.src.modeling import FEATURE_COLUMNS, build_model_pipeline, predict_group_mean_baseline, prepare_training_frame


def run_backtest(
    min_train_size=1400,
    step_size=280,
    holdout_size=280,
):
    conn = sqlite3.connect(config.SQLITE_DB)
    operations = pd.read_sql_query("SELECT * FROM operations_daily ORDER BY data", conn)
    conn.close()

    if operations.empty:
        return pd.DataFrame()

    operations = prepare_training_frame(operations)
    results = []
    fold = 1

    for train_end in range(min_train_size, len(operations) - holdout_size + 1, step_size):
        train_df = operations.iloc[:train_end].copy()
        test_df = operations.iloc[train_end : train_end + holdout_size].copy()
        if train_df.empty or test_df.empty:
            continue

        model = clone(build_model_pipeline())
        model.fit(train_df[FEATURE_COLUMNS], train_df["trabalhadores_necessarios"])
        predictions = model.predict(test_df[FEATURE_COLUMNS])
        baseline_predictions = predict_group_mean_baseline(train_df, test_df)

        results.append(
            {
                "fold_id": fold,
                "train_end_date": str(pd.to_datetime(train_df["data"]).max().date()),
                "test_start_date": str(pd.to_datetime(test_df["data"]).min().date()),
                "test_end_date": str(pd.to_datetime(test_df["data"]).max().date()),
                "train_rows": int(len(train_df)),
                "test_rows": int(len(test_df)),
                "mae": float(mean_absolute_error(test_df["trabalhadores_necessarios"], predictions)),
                "mape": float(mean_absolute_percentage_error(test_df["trabalhadores_necessarios"], predictions)),
                "r2": float(r2_score(test_df["trabalhadores_necessarios"], predictions)),
                "baseline_mae": float(mean_absolute_error(test_df["trabalhadores_necessarios"], baseline_predictions)),
                "baseline_mape": float(mean_absolute_percentage_error(test_df["trabalhadores_necessarios"], baseline_predictions)),
                "baseline_r2": float(r2_score(test_df["trabalhadores_necessarios"], baseline_predictions)),
            }
        )
        fold += 1

    return pd.DataFrame(results)
