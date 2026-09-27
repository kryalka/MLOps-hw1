import logging
import os

import pandas as pd
from catboost import CatBoostClassifier, Pool

from model_config import (
    CATEGORICAL_FEATURE_INDICES,
    FEATURE_NAMES,
    MODEL_PATH,
)

logger = logging.getLogger(__name__)

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

THRESHOLD = 0.5

logger.info('Importing pretrained model...')

model = CatBoostClassifier()
model.load_model(MODEL_PATH)

if tuple(model.feature_names_) != FEATURE_NAMES:
    raise ValueError("Feature names in model_meta.json do not match model.cbm")

if tuple(model.get_cat_feature_indices()) != CATEGORICAL_FEATURE_INDICES:
    raise ValueError("Categorical features in model_meta.json do not match model.cbm")

logger.info('Pretrained model imported successfully...')


def _make_prediction_pool(dt):
    ordered_features = dt.loc[:, list(FEATURE_NAMES)]

    return Pool(
        data=ordered_features,
        feature_names=list(FEATURE_NAMES),
        cat_features=list(CATEGORICAL_FEATURE_INDICES),
    )


def make_pred(dt, source_info="kafka"):
    prediction_pool = _make_prediction_pool(dt)

    y_proba = model.predict_proba(prediction_pool)[:, 1]

    submission = pd.DataFrame({
        "score": y_proba,
        "fraud_flag": (y_proba >= THRESHOLD).astype(int),
    })

    logger.info(f'Prediction complete for data from {source_info}')

    return submission, y_proba
