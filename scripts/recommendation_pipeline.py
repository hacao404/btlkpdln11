import json
import warnings
from datetime import date
from pathlib import Path

import faiss
import implicit
import joblib
import numpy as np
import pandas as pd
import polars as pl
from scipy.sparse import csr_matrix
from xgboost import XGBClassifier

warnings.filterwarnings("ignore")


# ============================================================
# 1. Cấu hình đường dẫn
# ============================================================

DATA_DIR = Path("data/full")
MODEL_DIR = Path("checkpoint/xgboost")

CSV_PATHS = [
    Path("product/2019-Oct.csv"),
    Path("product/2019-Nov.csv"),
]

EVENTS_PARQUET_PATH = DATA_DIR / "events.parquet"
PRODUCT_META_PATH = DATA_DIR / "product_meta.parquet"
RECOMMENDATIONS_OUTPUT_PATH = DATA_DIR / "recommendations_top20.parquet"
RECOMMENDATION_USERS_PATH = DATA_DIR / "recommendation_users.parquet"
MODEL_METRICS_PATH = DATA_DIR / "model_metrics.json"

# Checkpoint đã train sẵn, không train lại và không fit encoder mới.
XGB_MODEL_PATH = MODEL_DIR / "xgb_model_v3_gpu.json"
ENCODER_PATH = MODEL_DIR / "ordinal_encoder_v3_gpu.joblib"


# ============================================================
# 2. Cấu hình pipeline giống notebook
# ============================================================

EVENT_WEIGHT = {
    "purchase": 5.0,
    "cart": 2.0,
    "view": 0.5,
}

ALS_FACTORS = 64
ALS_ITERATIONS = 20
ALS_REGULARIZATION = 0.1
ALS_USE_GPU = False

CANDIDATE_K = 100
ALS_CANDIDATE_K = 100
ANN_CANDIDATE_K = 200
FINAL_TOPN = 20
MAX_PER_CATEGORY = 2

SERVE_CUTOFF = date(2019, 10, 24)
SERVE_ACTIVE_USER_START = date(2019, 10, 18)

# 20 user demo thật. Nếu muốn chạy nhiều hơn, đổi thành None và set SERVE_MAX_USERS.
SERVE_USER_IDS: list[int] | None = [
    512475445,
    542048657,
    561163588,
    537873067,
    512365995,
    512505687,
    513262194,
    543700762,
    513828022,
    537886091,
    517728689,
    545925192,
    513851612,
    513021392,
    522244661,
    516253278,
    521180810,
    519266477,
    535925182,
    536399452,
]
SERVE_MAX_USERS: int | None = None

MODEL_FEATURES = [
    "brand",
    "category_code_level1",
    "category_code_level2",
    "event_weekday",
    "user_total_sessions",
    "user_active_days",
    "product_cart_to_purchase_rate",
    "product_total_purchases",
    "product_unique_viewers",
    "activity_count",
    "session_view_count",
    "session_unique_products",
    "price",
    "price_vs_product_avg",
    "user_product_view_count",
]

CATEGORICAL_FEATURES = [
    "brand",
    "event_weekday",
    "category_code_level1",
    "category_code_level2",
]

NUMERIC_FEATURES = [c for c in MODEL_FEATURES if c not in CATEGORICAL_FEATURES]

EVENT_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


# ============================================================
# 3. Chuẩn bị dữ liệu
# ============================================================

def scan_clean_csv(csv_path: Path) -> pl.LazyFrame:
    return (
        pl.scan_csv(
            csv_path,
            schema_overrides={
                "product_id": pl.Int64,
                "category_id": pl.Int64,
                "user_id": pl.Int64,
                "price": pl.Float32,
                "event_time": pl.Utf8,
            },
            ignore_errors=True,
        )
        .select(EVENT_COLUMNS)
        .filter(
            pl.col("user_id").is_not_null()
            & pl.col("product_id").is_not_null()
            & pl.col("user_session").is_not_null()
        )
        .with_columns(
            [
                pl.col("brand").fill_null("unknown").alias("brand"),
                pl.col("category_code").fill_null("unknown").alias("category_code"),
                pl.col("event_time")
                .str.replace(" UTC", "")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                .alias("event_dt"),
            ]
        )
        .filter(pl.col("event_dt").is_not_null())
        .with_columns(pl.col("event_dt").dt.date().alias("event_date"))
    )


def prepare_data() -> None:
    print("Đang convert CSV sang parquet...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    scans = [scan_clean_csv(path) for path in CSV_PATHS]
    events_lf = pl.concat(scans, how="vertical_relaxed")
    events_lf.sink_parquet(EVENTS_PARQUET_PATH, compression="zstd", compression_level=3)

    print("Đang tạo product metadata...")
    product_meta = (
        pl.scan_parquet(EVENTS_PARQUET_PATH)
        .sort("event_dt")
        .group_by("product_id")
        .agg(
            [
                pl.col("brand").last().alias("brand"),
                pl.col("category_code").last().alias("category_code"),
                pl.col("price").last().alias("price"),
            ]
        )
        .with_columns(
            [
                pl.col("category_code").str.split(".").list.get(0).fill_null("unknown").alias("category_code_level1"),
                pl.col("category_code")
                .str.split(".")
                .list.get(1, null_on_oob=True)
                .fill_null("unknown")
                .alias("category_code_level2"),
            ]
        )
        .collect(streaming=True)
    )
    product_meta.write_parquet(PRODUCT_META_PATH, compression="zstd")


def load_events() -> pl.DataFrame:
    print("Đang đọc events parquet...")
    return (
        pl.scan_parquet(EVENTS_PARQUET_PATH)
        .filter(pl.col("event_date") <= pl.lit(SERVE_CUTOFF))
        .with_columns(
            [
                pl.col("user_id").cast(pl.Int64),
                pl.col("product_id").cast(pl.Int64),
                pl.col("price").cast(pl.Float32),
                pl.col("brand").fill_null("unknown").alias("brand"),
                pl.col("category_code").fill_null("unknown").alias("category_code"),
            ]
        )
        .sort(["user_id", "event_dt"])
        .collect(streaming=True)
    )


# ============================================================
# 4. ALS + FAISS state
# ============================================================

def build_interaction_matrix(history_df: pl.DataFrame):
    weighted = (
        history_df
        .filter(pl.col("event_type").is_in(list(EVENT_WEIGHT.keys())))
        .with_columns(
            pl.when(pl.col("event_type") == "purchase")
            .then(pl.lit(EVENT_WEIGHT["purchase"]))
            .when(pl.col("event_type") == "cart")
            .then(pl.lit(EVENT_WEIGHT["cart"]))
            .when(pl.col("event_type") == "view")
            .then(pl.lit(EVENT_WEIGHT["view"]))
            .otherwise(pl.lit(0.0))
            .cast(pl.Float32)
            .alias("weight")
        )
        .group_by(["user_id", "product_id"])
        .agg(pl.col("weight").sum().alias("weight"))
    )

    user_ids = weighted.select("user_id").unique().sort("user_id").get_column("user_id").to_list()
    item_ids = weighted.select("product_id").unique().sort("product_id").get_column("product_id").to_list()

    user_map_df = pl.DataFrame({"user_id": user_ids, "user_idx": np.arange(len(user_ids), dtype=np.int32)})
    item_map_df = pl.DataFrame({"product_id": item_ids, "item_idx": np.arange(len(item_ids), dtype=np.int32)})

    matrix_df = (
        weighted
        .join(user_map_df, on="user_id", how="inner")
        .join(item_map_df, on="product_id", how="inner")
        .select(["user_idx", "item_idx", "weight"])
    )

    rows = matrix_df.get_column("user_idx").to_numpy()
    cols = matrix_df.get_column("item_idx").to_numpy()
    vals = matrix_df.get_column("weight").to_numpy().astype(np.float32)

    interaction_csr = csr_matrix((vals, (rows, cols)), shape=(len(user_ids), len(item_ids)), dtype=np.float32)
    user_to_idx = dict(zip(user_ids, range(len(user_ids))))
    item_to_idx = dict(zip(item_ids, range(len(item_ids))))
    idx_to_item = np.array(item_ids, dtype=np.int64)

    return interaction_csr, user_map_df, item_map_df, user_to_idx, item_to_idx, idx_to_item


def train_als(interaction_csr: csr_matrix):
    print("Đang train ALS...")
    als = implicit.als.AlternatingLeastSquares(
        factors=ALS_FACTORS,
        regularization=ALS_REGULARIZATION,
        iterations=ALS_ITERATIONS,
        use_gpu=ALS_USE_GPU,
        random_state=42,
    )
    als.fit(interaction_csr)
    return als


def build_faiss_index(item_factors: np.ndarray):
    vectors = item_factors.astype(np.float32).copy()
    faiss.normalize_L2(vectors)
    index = faiss.IndexFlatIP(vectors.shape[1])
    index.add(vectors)
    return index


def build_feature_tables(history_df: pl.DataFrame):
    print("Đang tạo feature tables...")
    base = history_df.sort("event_dt")

    user_features = (
        base
        .group_by("user_id")
        .agg(
            [
                pl.col("user_session").n_unique().alias("user_total_sessions"),
                pl.col("event_date").n_unique().alias("user_active_days"),
            ]
        )
    )

    product_features = (
        base
        .group_by("product_id")
        .agg(
            [
                (pl.col("event_type") == "purchase").sum().alias("product_total_purchases"),
                (pl.col("event_type") == "cart").sum().alias("product_total_carts"),
                pl.col("user_id").filter(pl.col("event_type") == "view").n_unique().alias("product_unique_viewers"),
                pl.col("price").mean().alias("product_avg_price"),
            ]
        )
        .with_columns(
            (
                pl.col("product_total_purchases")
                / pl.col("product_total_carts").clip(lower_bound=1)
            ).alias("product_cart_to_purchase_rate")
        )
    )

    session_features = (
        base
        .group_by(["user_id", "user_session"])
        .agg(
            [
                pl.max("event_dt").alias("session_last_dt"),
                pl.len().alias("activity_count"),
                (pl.col("event_type") == "view").sum().alias("session_view_count"),
                pl.col("product_id").n_unique().alias("session_unique_products"),
            ]
        )
    )

    user_recent_session_features = (
        session_features
        .sort(["user_id", "session_last_dt"])
        .group_by("user_id")
        .agg(
            [
                pl.col("activity_count").last().alias("activity_count"),
                pl.col("session_view_count").last().alias("session_view_count"),
                pl.col("session_unique_products").last().alias("session_unique_products"),
            ]
        )
    )

    user_product_features = (
        base
        .filter(pl.col("event_type").is_in(["view", "purchase"]))
        .group_by(["user_id", "product_id"])
        .agg(
            [
                (pl.col("event_type") == "view").sum().alias("user_product_view_count"),
                ((pl.col("event_type") == "purchase").sum() > 0)
                .cast(pl.Int8)
                .alias("user_ever_purchased_product"),
            ]
        )
    )

    product_meta = (
        base
        .group_by("product_id")
        .agg(
            [
                pl.col("brand").last().alias("brand"),
                pl.col("category_code").last().alias("category_code"),
                pl.col("price").last().alias("price"),
            ]
        )
        .with_columns(
            [
                pl.col("category_code").str.split(".").list.get(0).fill_null("unknown").alias("category_code_level1"),
                pl.col("category_code")
                .str.split(".")
                .list.get(1, null_on_oob=True)
                .fill_null("unknown")
                .alias("category_code_level2"),
            ]
        )
    )

    popularity = (
        base
        .filter(pl.col("event_type").is_in(list(EVENT_WEIGHT.keys())))
        .with_columns(
            pl.when(pl.col("event_type") == "purchase")
            .then(pl.lit(EVENT_WEIGHT["purchase"]))
            .when(pl.col("event_type") == "cart")
            .then(pl.lit(EVENT_WEIGHT["cart"]))
            .when(pl.col("event_type") == "view")
            .then(pl.lit(EVENT_WEIGHT["view"]))
            .otherwise(pl.lit(0.0))
            .cast(pl.Float32)
            .alias("weight")
        )
        .group_by("product_id")
        .agg(pl.col("weight").sum().alias("pop_score"))
        .sort("pop_score", descending=True)
    )

    user_seen_map = {
        row["user_id"]: set(row["seen_items"])
        for row in base.group_by("user_id").agg(pl.col("product_id").unique().alias("seen_items")).iter_rows(named=True)
    }

    user_bought_map = {
        row["user_id"]: set(row["bought_items"])
        for row in (
            base
            .filter(pl.col("event_type") == "purchase")
            .group_by("user_id")
            .agg(pl.col("product_id").unique().alias("bought_items"))
            .iter_rows(named=True)
        )
    }

    return {
        "user_features": user_features,
        "product_features": product_features,
        "user_recent_session_features": user_recent_session_features,
        "user_product_features": user_product_features,
        "product_meta": product_meta,
        "popular_items": popularity.get_column("product_id").to_list(),
        "user_seen_map": user_seen_map,
        "user_bought_map": user_bought_map,
    }


def build_state_asof(events_df: pl.DataFrame, cutoff_date: date):
    print(f"Đang build state tại cutoff={cutoff_date}...")
    history_df = events_df.filter(pl.col("event_date") <= pl.lit(cutoff_date))

    interaction_csr, user_map_df, item_map_df, user_to_idx, item_to_idx, idx_to_item = build_interaction_matrix(history_df)
    als_model = train_als(interaction_csr)

    user_factors = als_model.user_factors.astype(np.float32)
    item_factors = als_model.item_factors.astype(np.float32)
    ann_index = build_faiss_index(item_factors)

    state = {
        "cutoff_date": cutoff_date,
        "history_df": history_df,
        "interaction_csr": interaction_csr,
        "user_map_df": user_map_df,
        "item_map_df": item_map_df,
        "user_to_idx": user_to_idx,
        "item_to_idx": item_to_idx,
        "idx_to_item": idx_to_item,
        "als_model": als_model,
        "user_factors": user_factors,
        "item_factors": item_factors,
        "ann_index": ann_index,
    }
    state.update(build_feature_tables(history_df))
    return state


# ============================================================
# 5. Candidate retrieval
# ============================================================

def retrieve_candidates_for_users(user_ids, state):
    records = []

    valid_users = [u for u in user_ids if u in state["user_to_idx"]]
    if not valid_users:
        return pl.DataFrame(
            schema={
                "user_id": pl.Int64,
                "product_id": pl.Int64,
                "retrieval_score": pl.Float64,
                "als_score": pl.Float64,
                "ann_score": pl.Float64,
                "source": pl.Utf8,
            }
        )

    user_indices = np.array([state["user_to_idx"][u] for u in valid_users], dtype=np.int32)

    als_items_batch, als_scores_batch = state["als_model"].recommend(
        user_indices,
        state["interaction_csr"][user_indices],
        N=ALS_CANDIDATE_K,
        filter_already_liked_items=True,
        recalculate_user=False,
    )

    queries = state["user_factors"][user_indices].astype(np.float32).copy()
    faiss.normalize_L2(queries)
    ann_scores_batch, ann_items_batch = state["ann_index"].search(queries, ANN_CANDIDATE_K)

    for i, user_id in enumerate(valid_users):
        seen_items = state["user_seen_map"].get(user_id, set())
        blend_score = {}
        als_raw_score = {}
        ann_raw_score = {}
        source_map = {}

        for rank, (item_idx, score) in enumerate(zip(als_items_batch[i], als_scores_batch[i]), start=1):
            if item_idx < 0:
                continue
            product_id = int(state["idx_to_item"][item_idx])
            if product_id in seen_items:
                continue
            blend_score[product_id] = blend_score.get(product_id, 0.0) + (1.0 / (rank + 1.0))
            als_raw_score[product_id] = float(score)
            source_map.setdefault(product_id, set()).add("als")

        for rank, (item_idx, score) in enumerate(zip(ann_items_batch[i], ann_scores_batch[i]), start=1):
            if item_idx < 0:
                continue
            product_id = int(state["idx_to_item"][item_idx])
            if product_id in seen_items:
                continue
            blend_score[product_id] = blend_score.get(product_id, 0.0) + (1.0 / (rank + 1.0))
            ann_raw_score[product_id] = float(score)
            source_map.setdefault(product_id, set()).add("ann")

        if len(blend_score) < CANDIDATE_K:
            for rank, product_id in enumerate(state["popular_items"], start=1):
                if product_id in seen_items or product_id in blend_score:
                    continue
                blend_score[product_id] = 1.0 / (1000.0 + rank)
                source_map.setdefault(product_id, set()).add("popular")
                if len(blend_score) >= CANDIDATE_K:
                    break

        top_items = sorted(blend_score.items(), key=lambda x: x[1], reverse=True)[:CANDIDATE_K]
        for product_id, retrieval_score in top_items:
            records.append(
                {
                    "user_id": int(user_id),
                    "product_id": int(product_id),
                    "retrieval_score": float(retrieval_score),
                    "als_score": float(als_raw_score.get(product_id, 0.0)),
                    "ann_score": float(ann_raw_score.get(product_id, 0.0)),
                    "source": "|".join(sorted(source_map.get(product_id, {"popular"}))),
                }
            )

    return pl.DataFrame(records)


# ============================================================
# 6. Scoring bằng checkpoint XGBoost
# ============================================================

def load_checkpoints():
    print("Đang load checkpoint XGBoost và OrdinalEncoder...")
    if not XGB_MODEL_PATH.exists():
        raise FileNotFoundError(f"Không tìm thấy checkpoint XGBoost: {XGB_MODEL_PATH}")
    if not ENCODER_PATH.exists():
        raise FileNotFoundError(f"Không tìm thấy OrdinalEncoder: {ENCODER_PATH}")

    xgb_model = XGBClassifier()
    xgb_model.load_model(XGB_MODEL_PATH)
    encoder = joblib.load(ENCODER_PATH)
    return xgb_model, encoder


def score_recommendations(state, model, encoder, serve_users):
    candidate_df = retrieve_candidates_for_users(serve_users, state)
    if candidate_df.height == 0:
        return pd.DataFrame(columns=["user_id", "rank", "product_id", "score", "retrieval_score", "source"])

    user_context = (
        state["history_df"]
        .filter(pl.col("user_id").is_in(serve_users))
        .group_by("user_id")
        .agg(pl.max("event_dt").alias("context_ts"))
        .with_columns(pl.col("context_ts").dt.weekday().cast(pl.Int64).alias("event_weekday"))
    )

    features = (
        candidate_df
        .join(user_context, on="user_id", how="left")
        .join(state["product_meta"], on="product_id", how="left")
        .join(state["user_features"], on="user_id", how="left")
        .join(state["product_features"], on="product_id", how="left")
        .join(state["user_recent_session_features"], on="user_id", how="left")
        .join(state["user_product_features"], on=["user_id", "product_id"], how="left")
        .with_columns((pl.col("price") / pl.col("product_avg_price").fill_null(1.0)).alias("price_vs_product_avg"))
    )

    for col in ["brand", "category_code_level1", "category_code_level2"]:
        features = features.with_columns(pl.col(col).fill_null("unknown").alias(col))

    for col in NUMERIC_FEATURES:
        features = features.with_columns(pl.col(col).fill_null(0).alias(col))

    features = features.with_columns(
        pl.col("event_weekday").fill_null(SERVE_CUTOFF.weekday()).cast(pl.Int64).alias("event_weekday")
    )

    pdf = features.select(["user_id", "product_id", "retrieval_score", "source"] + MODEL_FEATURES).to_pandas()

    for col in CATEGORICAL_FEATURES:
        pdf[col] = pdf[col].astype(str).fillna("unknown")
    for col in NUMERIC_FEATURES + ["retrieval_score"]:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0.0)

    X = pdf[MODEL_FEATURES].copy()
    X[CATEGORICAL_FEATURES] = encoder.transform(X[CATEGORICAL_FEATURES])
    pdf["score"] = model.predict_proba(X)[:, 1]

    return rerank_balanced(pdf, state["user_bought_map"])


def rerank_balanced(pdf: pd.DataFrame, user_bought_map: dict):
    rows = []
    sorted_pdf = pdf.sort_values(["user_id", "score", "retrieval_score"], ascending=[True, False, False])

    for user_id, group in sorted_pdf.groupby("user_id", sort=False):
        bought = user_bought_map.get(int(user_id), set())
        cat_counter = {}
        rank = 1

        for _, row in group.iterrows():
            product_id = int(row["product_id"])
            if product_id in bought:
                continue

            cat = row.get("category_code_level1", "unknown")
            cat = "unknown" if pd.isna(cat) else str(cat)
            if cat_counter.get(cat, 0) >= MAX_PER_CATEGORY:
                continue

            rows.append(
                {
                    "user_id": int(user_id),
                    "rank": rank,
                    "product_id": product_id,
                    "score": float(row["score"]),
                    "retrieval_score": float(row["retrieval_score"]),
                    "source": str(row["source"]),
                }
            )
            cat_counter[cat] = cat_counter.get(cat, 0) + 1
            rank += 1

            if rank > FINAL_TOPN:
                break

    return pd.DataFrame(rows, columns=["user_id", "rank", "product_id", "score", "retrieval_score", "source"])


def write_model_metrics(model, serve_users, recommendation_count):
    # Ghi metadata đơn giản cho UI, không train/evaluate lại model.
    gain = model.get_booster().get_score(importance_type="gain")
    feature_importance = [
        {"name": name, "value": float(gain.get(name, 0.0))}
        for name in MODEL_FEATURES
    ]
    feature_importance = sorted(feature_importance, key=lambda x: x["value"], reverse=True)

    metrics = {
        "training_mode": f"Offline inference bằng checkpoint {XGB_MODEL_PATH} và encoder {ENCODER_PATH}.",
        "leakage_note": "Serving artifact chỉ dùng history <= 2019-10-24. Notebook gốc dùng time-based split để tránh label leakage.",
        "splits": {
            "train_history": "2019-10-01 to 2019-10-17",
            "train_label": "2019-10-18 to 2019-10-24",
            "validation_label": "2019-10-25 to 2019-10-28",
            "test_label": "2019-10-29 to 2019-10-31",
            "serve_history": f"history <= {SERVE_CUTOFF}",
            "serve_active_user_start": str(SERVE_ACTIVE_USER_START),
        },
        "metrics": [],
        "dataset": {
            "serve_users": len(serve_users),
            "recommendation_rows": int(recommendation_count),
            "candidate_k": CANDIDATE_K,
            "final_topn": FINAL_TOPN,
        },
        "feature_importance": feature_importance,
        "pipeline_steps": [
            {"title": "Raw CSV behavior logs", "description": "Đọc 2019-Oct.csv và 2019-Nov.csv."},
            {"title": "Clean parquet", "description": "Clean null, parse event_time, ghi events.parquet."},
            {"title": "Weighted matrix", "description": "purchase=5.0, cart=2.0, view=0.5."},
            {"title": "ALS retrieval", "description": "Tìm candidate bằng collaborative filtering."},
            {"title": "FAISS ANN", "description": "Tìm candidate gần user vector trong latent space."},
            {"title": "XGBoost rerank", "description": "Load checkpoint và OrdinalEncoder đã train sẵn để score."},
            {"title": "Top-20 artifact", "description": "Ghi recommendations_top20.parquet cho FastAPI serve."},
        ],
    }
    MODEL_METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


# ============================================================
# 7. Chạy pipeline demo và ghi artifact cuối
# ============================================================

def get_serve_users(state):
    if SERVE_USER_IDS is not None:
        return [u for u in SERVE_USER_IDS if u in state["user_to_idx"]]

    user_ids = (
        state["history_df"]
        .filter(pl.col("event_date") >= pl.lit(SERVE_ACTIVE_USER_START))
        .select("user_id")
        .unique()
        .get_column("user_id")
        .to_list()
    )

    if SERVE_MAX_USERS is not None:
        rng = np.random.default_rng(42)
        user_ids = rng.choice(np.array(user_ids), size=min(SERVE_MAX_USERS, len(user_ids)), replace=False).tolist()

    return user_ids


def run_recommendation_pipeline():
    raw_events = load_events()
    state = build_state_asof(raw_events, SERVE_CUTOFF)
    xgb_model, encoder = load_checkpoints()
    serve_users = get_serve_users(state)

    print(f"Số user cần gợi ý: {len(serve_users)}")

    print("Đang tạo candidates và score bằng checkpoint XGBoost...")
    final_recs_pdf = score_recommendations(state, xgb_model, encoder, serve_users)

    if len(final_recs_pdf) > 0:
        final_recs = pl.from_pandas(final_recs_pdf)
    else:
        final_recs = pl.DataFrame(
            schema={
                "user_id": pl.Int64,
                "rank": pl.Int64,
                "product_id": pl.Int64,
                "score": pl.Float64,
                "retrieval_score": pl.Float64,
                "source": pl.Utf8,
            }
        )

    print("Đang ghi artifact cuối...")
    final_recs.write_parquet(RECOMMENDATIONS_OUTPUT_PATH)
    final_recs.select("user_id").unique().sort("user_id").write_parquet(RECOMMENDATION_USERS_PATH)
    write_model_metrics(xgb_model, serve_users, final_recs.height)

    print(f"Đã lưu recommendations: {RECOMMENDATIONS_OUTPUT_PATH}")
    print(f"Đã lưu danh sách user: {RECOMMENDATION_USERS_PATH}")
    print(f"Đã lưu thông tin pipeline: {MODEL_METRICS_PATH}")


def main():
    # Chỉ cần chạy prepare_data() một lần nếu chưa có events.parquet.
    if not EVENTS_PARQUET_PATH.exists():
        prepare_data()

    run_recommendation_pipeline()


if __name__ == "__main__":
    main()
