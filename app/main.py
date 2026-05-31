from pathlib import Path
import json

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


# ============================================================
# 1. Cấu hình đơn giản
# ============================================================

DATA_DIR = Path("data/full")
FRONTEND_DIR = Path("app/static")
TOP_K_DEFAULT = 20

PRODUCT_META_PATH = DATA_DIR / "product_meta.parquet"
RECOMMENDATIONS_PATH = DATA_DIR / "recommendations_top20.parquet"
RECOMMENDATION_USERS_PATH = DATA_DIR / "recommendation_users.parquet"
MODEL_METRICS_PATH = DATA_DIR / "model_metrics.json"


# ============================================================
# 2. Biến global để backend đọc artifact một lần lúc start
# ============================================================

product_meta_df = pd.DataFrame()
recommendations_df = pd.DataFrame()
users_df = pd.DataFrame()
model_metrics = {}
data_loaded = False


app = FastAPI(
    title="Real Recommendation Demo",
    description="FastAPI demo chỉ serve recommendation artifact đã sinh offline.",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# ============================================================
# 3. Load dữ liệu
# ============================================================

def load_all_data():
    global product_meta_df, recommendations_df, users_df, model_metrics, data_loaded

    print("Đang load product metadata...")
    product_meta_df = read_product_meta()

    print("Đang load recommendation artifact...")
    recommendations_df = read_recommendations()

    print("Đang load danh sách user có recommendation...")
    users_df = read_recommendation_users(recommendations_df)

    print("Đang load thông tin pipeline/model...")
    model_metrics = read_model_metrics()

    data_loaded = True
    print("Backend đã sẵn sàng.")


def read_product_meta():
    if not PRODUCT_META_PATH.exists():
        raise FileNotFoundError(f"Thiếu file: {PRODUCT_META_PATH}")

    df = pd.read_parquet(PRODUCT_META_PATH)
    need_cols = ["product_id", "brand", "category_code", "price"]
    df = df[need_cols].copy()
    df["product_id"] = df["product_id"].astype(str)
    df["brand"] = df["brand"].fillna("unknown").astype(str)
    df["category_code"] = df["category_code"].fillna("unknown").astype(str)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    return df.drop_duplicates("product_id").reset_index(drop=True)


def read_recommendations():
    if not RECOMMENDATIONS_PATH.exists():
        raise FileNotFoundError(f"Thiếu file: {RECOMMENDATIONS_PATH}")

    df = pd.read_parquet(RECOMMENDATIONS_PATH)
    need_cols = ["user_id", "rank", "product_id", "score", "retrieval_score", "source"]
    df = df[need_cols].copy()
    df["user_id"] = df["user_id"].astype(str)
    df["product_id"] = df["product_id"].astype(str)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce").fillna(0).astype(int)
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)
    df["retrieval_score"] = pd.to_numeric(df["retrieval_score"], errors="coerce").fillna(0.0)
    df["source"] = df["source"].fillna("unknown").astype(str)
    return df.sort_values(["user_id", "rank"]).reset_index(drop=True)


def read_recommendation_users(recs_df):
    if RECOMMENDATION_USERS_PATH.exists():
        df = pd.read_parquet(RECOMMENDATION_USERS_PATH)
        df["user_id"] = df["user_id"].astype(str)
        return df[["user_id"]].drop_duplicates().sort_values("user_id").reset_index(drop=True)

    return recs_df[["user_id"]].drop_duplicates().sort_values("user_id").reset_index(drop=True)


def read_model_metrics():
    if MODEL_METRICS_PATH.exists():
        return json.loads(MODEL_METRICS_PATH.read_text(encoding="utf-8"))

    return {
        "training_mode": "Offline inference demo",
        "leakage_note": "Recommendation artifacts được sinh offline bằng time-based split.",
        "splits": {
            "train_history": "2019-10-01 -> 2019-10-17",
            "train_label": "2019-10-18 -> 2019-10-24",
            "validation_label": "2019-10-25 -> 2019-10-28",
            "test_label": "2019-10-29 -> 2019-10-31",
            "serve_history": "history <= configured SERVE_CUTOFF",
        },
        "metrics": [],
        "feature_importance": [],
        "pipeline_steps": [
            {"title": "Raw CSV logs", "description": "Đọc 2019-Oct.csv và 2019-Nov.csv bằng Polars."},
            {"title": "Clean parquet", "description": "Clean null, parse event_time, ghi events.parquet."},
            {"title": "ALS retrieval", "description": "Tạo weighted user-item matrix và lấy candidate."},
            {"title": "FAISS ANN", "description": "Search item embedding gần user preference vector."},
            {"title": "XGBoost rerank", "description": "Load checkpoint đã train sẵn để score candidates."},
            {"title": "Top-20", "description": "Ghi recommendations_top20.parquet cho backend serve."},
        ],
    }


# ============================================================
# 4. Logic trả recommendation
# ============================================================

def build_explanation(source, score):
    value = str(source).lower()
    texts = []

    if "als" in value:
        texts.append("ALS tìm item này từ pattern collaborative filtering giữa user và product.")
    if "ann" in value:
        texts.append("FAISS lấy item này vì embedding của item gần với vector sở thích của user.")
    if "popular" in value:
        texts.append("Popular fallback được dùng khi candidate từ ALS/ANN chưa đủ.")
    if float(score) >= 0.7:
        texts.append("XGBoost gán purchase probability cao dựa trên user, product, session và price features.")

    if not texts:
        texts.append("Item này nằm trong offline recommendation artifact đã sinh trước.")
    return " ".join(texts)


def get_product_meta(product_id):
    rows = product_meta_df.loc[product_meta_df["product_id"] == str(product_id)]
    if rows.empty:
        return {
            "product_id": str(product_id),
            "brand": "unknown",
            "category_code": "unknown",
            "price": 0.0,
        }
    return rows.iloc[0].to_dict()


def get_user_recommendations(user_id, top_k):
    user_recs = recommendations_df.loc[recommendations_df["user_id"] == str(user_id)]
    if user_recs.empty:
        return []

    user_recs = user_recs.head(top_k).merge(product_meta_df, on="product_id", how="left")
    user_recs["brand"] = user_recs["brand"].fillna("unknown")
    user_recs["category_code"] = user_recs["category_code"].fillna("unknown")
    user_recs["price"] = pd.to_numeric(user_recs["price"], errors="coerce").fillna(0.0)

    items = []
    for row in user_recs.to_dict(orient="records"):
        items.append(
            {
                "user_id": str(row["user_id"]),
                "rank": int(row["rank"]),
                "product_id": str(row["product_id"]),
                "brand": str(row["brand"]),
                "category_code": str(row["category_code"]),
                "price": float(row["price"]),
                "score": float(row["score"]),
                "retrieval_score": float(row["retrieval_score"]),
                "source": str(row["source"]),
                "explanation": build_explanation(row["source"], row["score"]),
            }
        )
    return items


# ============================================================
# 5. API routes
# ============================================================

@app.on_event("startup")
def startup():
    load_all_data()


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/styles.css", include_in_schema=False)
def root_styles():
    return FileResponse(FRONTEND_DIR / "styles.css")


@app.get("/app.js", include_in_schema=False)
def root_script():
    return FileResponse(FRONTEND_DIR / "app.js")


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "data_loaded": data_loaded,
        "recommendation_rows": int(len(recommendations_df)),
        "recommendation_users": int(len(users_df)),
    }


@app.get("/api/users")
def users(limit: int = Query(100, ge=1, le=500), query: str | None = None):
    df = users_df.copy()
    if query:
        df = df[df["user_id"].str.contains(str(query), case=False, na=False)]
    return df["user_id"].head(limit).tolist()


@app.get("/api/users/{user_id}/recommendations")
def recommendations(user_id: str, top_k: int = Query(TOP_K_DEFAULT, ge=1, le=100)):
    items = get_user_recommendations(user_id, top_k)
    if not items:
        raise HTTPException(
            status_code=404,
            detail=(
                "User này chưa có recommendation artifact. "
                "Hãy thêm user_id vào SERVE_USER_IDS trong scripts/student_recommendation_pipeline.py "
                "hoặc build full recommendations rồi chạy lại backend."
            ),
        )
    return items


@app.get("/api/products/{product_id}")
def product(product_id: str):
    return get_product_meta(product_id)


@app.get("/api/model/metrics")
def model_metrics_api():
    return model_metrics
