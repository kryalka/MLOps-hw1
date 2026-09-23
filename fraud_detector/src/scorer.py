import json
import logging
import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
MODELS_DIR = Path(__file__).resolve().parent.parent / "models"

MODEL_PATH = MODELS_DIR / "model.cbm"
META_PATH = MODELS_DIR / "model_meta.json"

THRESHOLD = 0.5

logger.info('Importing pretrained model...')

model = CatBoostClassifier()
model.load_model(MODEL_PATH)
with open(META_PATH, "r", encoding="utf-8") as f:
    model_meta = json.load(f)

FEATURE_NAMES = model_meta["feature_names"]

logger.info('Pretrained model imported successfully...')


def make_pred(dt, source_info="kafka"):
    dt = dt[FEATURE_NAMES]

    y_proba = model.predict_proba(dt)[:, 1]

    submission = pd.DataFrame({
        "score": y_proba,
        "fraud_flag": (y_proba >= THRESHOLD).astype(int),
    })

    logger.info(f'Prediction complete for data from {source_info}')

    return submission, y_proba