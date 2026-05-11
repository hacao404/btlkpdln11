# ============================================================
# KAGGLE NOTEBOOK 01 — Purchase Prediction (Lazy Pattern)
# Features được tính ở session-level → nhỏ hơn raw data rất nhiều
# ============================================================

# ── Cell 1: Imports ──────────────────────────────────────────
import os, time, warnings, pickle
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import polars as pl
import lightgbm as lgb
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn import metrics
from sklearn.preprocessing import OrdinalEncoder

plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")
plt.rcParams.update({"figure.dpi": 110, "font.size": 11, "axes.titlesize": 13})
print(" Imports OK")

# ── Cell 2: Paths & Files ─────────────────────────────────────
OUT       = "/kaggle/working/output"
MODEL_DIR = f"{OUT}/models"
PLOT_DIR  = f"{OUT}/plots"
DATA_DIR  = f"{OUT}/data"
for d in [MODEL_DIR, PLOT_DIR, DATA_DIR]:
    os.makedirs(d, exist_ok=True)

csv_files = []
for root, dirs, files in os.walk("/kaggle/input"):
    for f in sorted(files):
        if f.endswith(".csv"):
            csv_files.append(os.path.join(root, f))

if not csv_files:
    raise FileNotFoundError("No CSV found!")

total_gb = sum(os.path.getsize(f)/1024**3 for f in csv_files)
print(f" {len(csv_files)} files ({total_gb:.1f} GB)")
for f in csv_files:
    print(f"    {os.path.basename(f):40s}  {os.path.getsize(f)/1024**3:.2f} GB")

# ── Cell 3: Tạo LazyFrame ─────────────────────────────────────
SCHEMA = {"product_id": pl.Int64, "category_id": pl.Int64,
          "user_id": pl.Int64, "price": pl.Float64, "event_time": pl.Utf8}

raw = (
    pl.concat([pl.scan_csv(f, schema_overrides=SCHEMA) for f in csv_files])
    .filter(
        pl.col("user_session").is_not_null() &
        pl.col("user_id").is_not_null() &
        pl.col("product_id").is_not_null() &
        pl.col("price").is_not_null() &
        (pl.col("price") > 0)
    )
    .with_columns([
        pl.col(["brand", "category_code"]).fill_null("unknown"),
        pl.col("event_time").str.slice(0, 19)
          .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).alias("event_ts"),
    ])
    .filter(pl.col("event_ts").is_not_null())
    .with_columns([
        pl.col("event_ts").dt.date().alias("event_date"),
        pl.col("event_ts").dt.hour().alias("event_hour"),
        pl.col("event_ts").dt.weekday().alias("event_weekday"),
        pl.col("category_code").str.split(".").list.get(0, null_on_oob=True)
          .fill_null("unknown").alias("category_level1"),
        pl.col("category_code").str.split(".").list.get(1, null_on_oob=True)
          .fill_null("unknown").alias("category_level2"),
    ])
)

print(" LazyFrame ready")

# ── Cell 4: Session features — Hash Partition Streaming ────────
# File-by-file vẫn OOM vì Nov 2019 có ~68M rows → group_by hash table quá lớn
# Fix: hash partition sessions → mỗi pass chỉ giữ 25% sessions trong RAM
#   Pass 1: sessions hash%4==0 → ~12.5M sessions → hash table ~1 GB 
#   Pass 2: sessions hash%4==1 → ~12.5M sessions → hash table ~1 GB 
#   ...
#   Scan data 4 lần nhưng mỗi lần streaming + nhỏ → không OOM
CKPT_SESSIONS = f"{DATA_DIR}/sessions.parquet"
N_PARTS = 4   # tăng lên 8 nếu vẫn OOM

if os.path.exists(CKPT_SESSIONS):
    print("  ⚡ Loading sessions from checkpoint...")
    t0 = time.time()
    sessions = pl.read_parquet(CKPT_SESSIONS)
    print(f" Sessions loaded: {len(sessions):,} rows ({time.time()-t0:.1f}s)")
else:
    import gc
    NEEDED = ["user_id", "user_session", "event_type", "product_id",
              "category_code", "brand", "price", "event_time"]
    all_lazy = pl.concat([pl.scan_csv(f, schema_overrides=SCHEMA) for f in csv_files])

    print(f"\n🔧 Building sessions via hash-partition streaming ({N_PARTS} passes)...")
    t0 = time.time()
    parts = []

    for p in range(N_PARTS):
        print(f"  Pass {p+1}/{N_PARTS}...", end=" ", flush=True)
        tp = time.time()
        part = (
            all_lazy
            .select(NEEDED)
            .filter(
                pl.col("user_session").is_not_null() &
                pl.col("user_id").is_not_null() &
                pl.col("product_id").is_not_null() &
                pl.col("price").is_not_null() & (pl.col("price") > 0) &
                # Chỉ giữ 1/N_PARTS sessions theo hash
                ((pl.col("user_session").hash(seed=42) % N_PARTS) == p)
            )
            .with_columns([
                pl.col("category_code").fill_null("unknown")
                  .str.split(".").list.get(0, null_on_oob=True)
                  .fill_null("unknown").alias("cat1"),
                pl.col("brand").fill_null("unknown").alias("brand_clean"),
            ])
            .group_by(["user_id", "user_session"]).agg([
                (pl.col("event_type") != "purchase").sum().alias("n_events"),
                (pl.col("event_type") == "view").sum().alias("n_views"),
                (pl.col("event_type") == "cart").sum().alias("n_carts"),
                (pl.col("event_type") == "purchase").sum().alias("n_purchases"),
                (pl.col("event_type") == "remove_from_cart").sum().alias("n_removes"),
                pl.col("price").mean().alias("avg_price"),
                pl.col("price").max().alias("max_price"),
                pl.col("price").min().alias("min_price"),
                pl.col("product_id").approx_n_unique().alias("n_unique_products"),
                pl.col("brand_clean").approx_n_unique().alias("n_unique_brands"),
                pl.col("cat1").approx_n_unique().alias("n_unique_cats"),
                pl.col("event_time").str.slice(11, 2)
                  .cast(pl.Int8, strict=False).first().alias("start_hour"),
                pl.col("event_time").str.slice(0, 10).first().alias("event_date_str"),
                pl.col("cat1").first().alias("top_category"),
                pl.col("brand_clean").first().alias("top_brand"),
            ])
            .collect(streaming=True)   # streaming + chỉ 25% sessions → ~1-2 GB RAM
        )
        parts.append(part)
        print(f"{len(part):,} sessions ({time.time()-tp:.0f}s)")
        del part; gc.collect()

    sessions = pl.concat(parts)
    del parts; gc.collect()

    # Tính weekday sau khi concat (data nhỏ, nhanh)
    sessions = (
        sessions
        .with_columns(
            pl.col("event_date_str").str.to_date("%Y-%m-%d", strict=False).alias("event_date")
        )
        .with_columns(pl.col("event_date").dt.weekday().alias("weekday"))
        .drop("event_date_str")
        .with_columns([
            (pl.col("n_purchases") > 0).cast(pl.Int8).alias("label"),
            (pl.col("n_carts") / pl.col("n_events").clip(lower_bound=1)).alias("cart_rate"),
            (pl.col("n_views") / pl.col("n_events").clip(lower_bound=1)).alias("view_rate"),
            (pl.col("n_removes") / pl.col("n_carts").clip(lower_bound=1)).alias("remove_rate"),
            pl.col("avg_price  ").log1p().alias("avg_price_log"),
            pl.when(pl.col("start_hour").is_between(6, 12)).then(pl.lit("morning"))
              .when(pl.col("start_hour").is_between(12, 18)).then(pl.lit("afternoon"))
              .when(pl.col("start_hour").is_between(18, 22)).then(pl.lit("evening"))
              .otherwise(pl.lit("night")).alias("time_of_day"),
            pl.when(pl.col("weekday") >= 5).then(1).otherwise(0).alias("is_weekend"),
        ])
    )
    sessions.write_parquet(CKPT_SESSIONS)
    print(f" Checkpoint saved! ({time.time()-t0:.0f}s total)")
    print(f" {len(sessions):,} sessions")

print(f"   Purchases: {sessions['label'].sum():,} ({sessions['label'].mean()*100:.2f}%)")
print(f"   RAM: ~{sessions.estimated_size('mb'):.0f} MB")

# ── Cell 5: Train/Test split (temporal) ───────────────────────
sessions_pd = sessions.to_pandas()
sessions_pd["event_date"] = pd.to_datetime(sessions_pd["event_date"])
sessions_pd = sessions_pd.sort_values("event_date")

split_idx = int(len(sessions_pd) * 0.80)
train_df = sessions_pd.iloc[:split_idx]
test_df  = sessions_pd.iloc[split_idx:]

CAT_COLS  = ["top_category", "top_brand", "time_of_day"]
NUM_COLS  = ["n_events", "n_views", "n_carts", "n_removes",
             # n_purchases bị loại — trực tiếp = label → data leakage!
             "avg_price", "max_price", "min_price", "avg_price_log",
             "n_unique_products", "n_unique_brands", "n_unique_cats",
             "start_hour", "weekday", "cart_rate", "view_rate",
             "remove_rate", "is_weekend"]
FEAT_COLS = NUM_COLS + CAT_COLS

# Encode categoricals
enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
train_df[CAT_COLS] = enc.fit_transform(train_df[CAT_COLS])
test_df[CAT_COLS]  = enc.transform(test_df[CAT_COLS])

X_train = train_df[FEAT_COLS]; y_train = train_df["label"]
X_test  = test_df[FEAT_COLS];  y_test  = test_df["label"]

print(f"\n   Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"   Purchase rate — Train: {y_train.mean():.3f} | Test: {y_test.mean():.3f}")

# ── Cell 6: LightGBM Training ────────────────────────────────
lgb_train = lgb.Dataset(X_train, label=y_train,
                         categorical_feature=CAT_COLS, free_raw_data=False)
lgb_valid = lgb.Dataset(X_test, label=y_test, reference=lgb_train)

params = {
    "objective": "binary",
    "metric": ["binary_logloss", "auc"],
    "learning_rate": 0.05,
    "num_leaves": 127,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "verbose": -1,
    "seed": 42,
}
evals = {}
t0 = time.time()
model = lgb.train(
    params, lgb_train, num_boost_round=2000,
    valid_sets=[lgb_train, lgb_valid],
    valid_names=["train", "valid"],
    callbacks=[
        lgb.early_stopping(100),
        lgb.log_evaluation(200),
        lgb.record_evaluation(evals),
    ]
)
print(f"\n⏱️  Train: {time.time()-t0:.1f}s | Best iter: {model.best_iteration}")

# ── Cell 7: Evaluation ────────────────────────────────────────
y_prob = model.predict(X_test)
y_pred = (y_prob >= 0.5).astype(int)
auc    = metrics.roc_auc_score(y_test, y_prob)
f1     = metrics.f1_score(y_test, y_pred)

print(f"\n{'='*50}")
print(f"   Purchase Prediction Results")
print(f"{'='*50}")
print(f"  {'Accuracy':<20}: {metrics.accuracy_score(y_test, y_pred):.4f}")
print(f"  {'Precision':<20}: {metrics.precision_score(y_test, y_pred):.4f}")
print(f"  {'Recall':<20}: {metrics.recall_score(y_test, y_pred):.4f}")
print(f"  {'F1-Score':<20}: {f1:.4f}")
print(f"  {'ROC-AUC ':<20}: {auc:.4f}")
print(metrics.classification_report(y_test, y_pred,
      target_names=["No Purchase", "Purchase"], digits=4))

# ── Cell 8: Plots ─────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(20, 6))

# ROC
fpr, tpr, _ = metrics.roc_curve(y_test, y_prob)
axes[0].plot(fpr, tpr, color="#3498db", linewidth=2.5, label=f"AUC={auc:.4f}")
axes[0].plot([0,1],[0,1],"k--",alpha=0.4)
axes[0].fill_between(fpr, tpr, alpha=0.08, color="#3498db")
axes[0].set_title("📈 ROC Curve"); axes[0].legend(); axes[0].grid(True,alpha=0.3)
axes[0].set_xlabel("FPR"); axes[0].set_ylabel("TPR")

# Loss curve
axes[1].plot(evals["train"]["binary_logloss"], color="#3498db", label="Train", linewidth=2)
axes[1].plot(evals["valid"]["binary_logloss"], color="#e74c3c", label="Valid", linewidth=2)
axes[1].axvline(model.best_iteration, color="gray", linestyle="--", alpha=0.6)
axes[1].set_title("📉 Loss Curve"); axes[1].legend(); axes[1].grid(True,alpha=0.3)

# Feature Importance
fi = pd.DataFrame({"feature": FEAT_COLS,
                   "gain": model.feature_importance("gain")})
fi = fi.sort_values("gain", ascending=True).tail(15)
axes[2].barh(fi["feature"], fi["gain"], color=sns.color_palette("viridis",15))
axes[2].set_title("⭐ Feature Importance (Gain)")
axes[2].grid(True, alpha=0.3, axis="x")

plt.suptitle("🛒 Purchase Prediction — LightGBM", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig(f"{PLOT_DIR}/purchase_prediction.png", dpi=150, bbox_inches="tight")
plt.show()

# ── Cell 9: Save ─────────────────────────────────────────────
model.save_model(f"{MODEL_DIR}/lgbm_purchase.txt")
with open(f"{MODEL_DIR}/purchase_encoder.pkl", "wb") as f:
    pickle.dump(enc, f)
with open(f"{MODEL_DIR}/purchase_feature_cols.pkl", "wb") as f:
    pickle.dump({"features": FEAT_COLS, "cat_cols": CAT_COLS, "num_cols": NUM_COLS}, f)

sessions.write_parquet(f"{DATA_DIR}/sessions.parquet")

print(f"\n💾 Saved:")
print(f"   {MODEL_DIR}/lgbm_purchase.txt")
print(f"   {MODEL_DIR}/purchase_encoder.pkl")
print(f"   {DATA_DIR}/sessions.parquet")
print(f"\n✅ Purchase Prediction HOÀN THÀNH!")
print(f"   AUC={auc:.4f} | F1={f1:.4f}")
