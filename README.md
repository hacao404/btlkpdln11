  Ecommerce Purchase Prediction Pipeline

 Dự đoán hành vi mua hàng trong thương mại điện tử sử dụng PySpark & LightGBM

---

##  Giới thiệu bài toán

Trong thương mại điện tử hiện đại, việc **dự đoán người dùng có mua hàng hay không** trong một phiên duyệt web là bài toán có giá trị kinh doanh cực kỳ lớn. Hệ thống dự đoán chính xác giúp:

- **Cá nhân hóa trải nghiệm**: Đề xuất sản phẩm phù hợp đúng lúc
- **Tối ưu hóa quảng cáo**: Tập trung ngân sách vào khách hàng có khả năng mua cao
- **Tăng tỷ lệ chuyển đổi**: Kích hoạt ưu đãi cho khách hàng đang do dự
- **Phân tích hành vi**: Hiểu sâu hơn về hành trình mua hàng của khách

### Quy mô dữ liệu

| Chỉ số | Giá trị |
|--------|---------|
| Tổng số sự kiện | **~42.4 triệu** bản ghi |
| Số cột | 9 |
| Số người dùng độc lập | **~3 triệu** |
| Phạm vi thời gian | Tháng 10/2019 |

---

## 🗂️ Cấu trúc dữ liệu

### Schema nguồn (eCommerce Events)

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `event_time` | String | Thời điểm xảy ra sự kiện (UTC) |
| `event_type` | String | Loại sự kiện: `view`, `cart`, `purchase` |
| `product_id` | Int64 | ID sản phẩm |
| `category_id` | Int64 | ID danh mục |
| `category_code` | String | Mã danh mục (vd: `electronics.smartphone`) |
| `brand` | String | Thương hiệu sản phẩm |
| `price` | Float32 | Giá sản phẩm |
| `user_id` | Int64 | ID người dùng |
| `user_session` | String | UUID phiên làm việc |

### Ví dụ dữ liệu

```
event_time              event_type  product_id  category_code              brand    price    user_id     user_session
2019-10-01 00:00:00    view        44600062    unknown                    shiseido  35.79   541312140   72d76fde-...
2019-10-01 00:00:01    view        1307067     computers.notebook         lenovo    251.74  550050854   7c90fc70-...
2019-10-01 00:00:04    view        1004237     electronics.smartphone     apple    1081.98  535871217   c6bd7419-...
```

---

## 🏗️ Kiến trúc Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ECOMMERCE PREDICTION PIPELINE                    │
└─────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐
  │   Nguồn DL   │   CSV / Parquet (42M+ rows)
  │  (Raw Data)  │   /kaggle/input/ecommerce-events/
  └──────┬───────┘
         │
         ▼
  ┌──────────────────────────────────┐
  │       1. DATA INGESTION          │
  │  • Đọc CSV với schema cố định    │
  │  • Chuyển đổi sang Parquet       │
  │  • Nén zstd (tiết kiệm ~60% disk)│
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │       2. DATA CLEANING           │
  │  • Lọc null: user_session,       │
  │    user_id, product_id           │
  │  • Fill null brand → "unknown"   │
  │  • Fill null category → "unknown"│
  │  • Parse event_time → datetime   │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │     3. FEATURE ENGINEERING       │
  │                                  │
  │  [User Features]                 │
  │  • user_total_sessions           │
  │  • user_active_days              │
  │                                  │
  │  [Product Features]              │
  │  • product_unique_viewers        │
  │  • product_total_purchases       │
  │  • product_total_carts           │
  │  • product_avg_price             │
  │  • product_price_std             │
  │  • product_cart_to_purchase_rate │
  │                                  │
  │  [Session Features]              │
  │  • session_view_count            │
  │  • session_unique_products       │
  │                                  │
  │  [User-Product Interaction]      │
  │  • user_product_views            │
  │  • user_product_purchased        │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │      4. TARGET LABELING          │
  │  • is_purchased = 1 nếu user     │
  │    đã mua product trong session  │
  │  • is_purchased = 0 nếu chưa    │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │      5. DOWNSAMPLING             │
  │  • Imbalanced: ~1% purchase      │
  │  • Giữ toàn bộ positive (mua)   │
  │  • Sample negative (không mua)   │
  │  • Target: ~300K samples total   │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │    6. MODEL TRAINING             │
  │  • Algorithm: LightGBM           │
  │  • Task: Binary Classification   │
  │  • Hyperparameters:              │
  │    - num_leaves = 127            │
  │    - learning_rate = 0.05        │
  │    - n_estimators = 500          │
  │  • Validation: ROC-AUC           │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │    7. EVALUATION & EXPORT        │
  │  • Metrics: AUC, Accuracy, F1    │
  │  • Feature Importance plot       │
  │  • Model export (pickle/joblib)  │
  └──────────────────────────────────┘
```

---

##  Feature Engineering Chi Tiết

###  User-Level Features

| Feature | Mô tả | Cách tính |
|---------|-------|-----------|
| `user_total_sessions` | Tổng số phiên của user | COUNT DISTINCT session theo user_id |
| `user_active_days` | Số ngày hoạt động | COUNT DISTINCT date theo user_id |

###  Product-Level Features

| Feature | Mô tả | Cách tính |
|---------|-------|-----------|
| `product_unique_viewers` | Số người xem duy nhất | COUNT DISTINCT user_id WHERE event=view |
| `product_total_purchases` | Tổng lượt mua | COUNT WHERE event=purchase |
| `product_total_carts` | Tổng lượt thêm giỏ | COUNT WHERE event=cart |
| `product_avg_price` | Giá trung bình | AVG(price) |
| `product_price_std` | Độ lệch chuẩn giá | STDDEV(price) |
| `product_cart_to_purchase_rate` | Tỷ lệ mua sau khi thêm giỏ | total_purchases / total_carts |

###  Session-Level Features

| Feature | Mô tả | Cách tính |
|---------|-------|-----------|
| `session_view_count` | Số lần xem trong phiên | COUNT events trong session |
| `session_unique_products` | Số sản phẩm khác nhau xem | COUNT DISTINCT product_id trong session |

###  User-Product Interaction Features

| Feature | Mô tả | Cách tính |
|---------|-------|-----------|
| `user_product_views` | Số lần user xem sản phẩm này | COUNT WHERE event=view GROUP BY (user, product) |
| `user_product_purchased` | User đã từng mua sản phẩm này | MAX(1 IF event=purchase ELSE 0) |

---

##  Mô hình Machine Learning

### Lý do chọn LightGBM

```
LightGBM được chọn vì:
 Tốc độ training nhanh (Gradient-based One-Side Sampling)
 Hiệu quả bộ nhớ cao (Histogram-based algorithm)
 Xử lý tốt dữ liệu tabular lớn
 Hỗ trợ categorical features tốt
 Ít cần tuning so với XGBoost
```

### Hyperparameters

```python
params = {
    'objective': 'binary',
    'metric': 'auc',
    'num_leaves': 127,
    'learning_rate': 0.05,
    'n_estimators': 500,
    'boosting_type': 'gbdt'
}
```

### Xử lý mất cân bằng dữ liệu (Class Imbalance)

```
Phân phối gốc:
  • event = 'view'     : ~85%
  • event = 'cart'     : ~10%
  • event = 'purchase' : ~5%

Sau khi tạo nhãn is_purchased:
  • is_purchased = 0   : ~99%
  • is_purchased = 1   : ~1%

Giải pháp: Downsampling
  • Giữ toàn bộ positive samples (mua)
  • Random sample negative samples
  • Tỷ lệ cuối cùng: ~300K tổng mẫu
```

---

##  Khám phá Dữ liệu (EDA)

### Xu hướng lượt truy cập hàng ngày

Dữ liệu cho thấy lượng truy cập hàng ngày trong tháng 10/2019, với khoảng **3 triệu người dùng** duy nhất truy cập mỗi ngày (trong khoảng 500K-1M unique visitors mỗi ngày).

### Phân tích giá sản phẩm

- Giá sản phẩm biến động theo thời gian, đặc biệt rõ với các mặt hàng điện tử
- Mặt hàng notebook (Lenovo, product_id=1307067) có mức giá ~251.74 USD

### Phân phối danh mục sản phẩm

Các danh mục phổ biến nhất:
- `electronics.smartphone`
- `computers.notebook`  
- `computers.desktop`
- `appliances.environment.water_heater`
- `furniture.living_room.sofa`

---

##  Công nghệ sử dụng

| Công nghệ | Phiên bản | Mục đích |
|-----------|-----------|---------|
| **Apache Spark (PySpark)** | 3.x | Xử lý dữ liệu phân tán |
| **LightGBM** | Latest | Mô hình gradient boosting |
| **Polars** | 1.x | EDA và tiền xử lý nhanh |
| **Scikit-learn** | Latest | Metrics, preprocessing |
| **Matplotlib** | Latest | Visualization |
| **Squarify** | Latest | Treemap visualization |
| **NumPy** | Latest | Tính toán số học |

---

##  Cấu trúc dự án

```
.
├── README.md                       # Tài liệu dự án (file này)
├── process_data_3.ipynb        #  Pipeline chính: PySpark + LightGBM
├── process_data.ipynb              #  EDA: Khám phá dữ liệu ban đầu
└── process_data_v2.ipynb           # EDA nâng cao: Polars + Visualization
```

### Mô tả các notebook

> Notebook triển khai toàn bộ pipeline ML từ đầu đến cuối với PySpark và LightGBM.

**Nội dung:**
- Khởi tạo Spark Session với cấu hình tối ưu
- Định nghĩa schema dữ liệu
- Load và clean dữ liệu
- Feature engineering phân tán
- Training LightGBM
- Đánh giá mô hình (AUC, Classification Report)

#### `process_data.ipynb` — EDA Cơ bản
> Notebook khám phá dữ liệu ban đầu sử dụng Polars.

**Nội dung:**
- Load dữ liệu CSV/Parquet
- Thống kê cơ bản (shape, schema, null values)
- Xu hướng lượt truy cập hàng ngày
- Phân tích giá sản phẩm theo thời gian

#### `process_data_v2.ipynb` — EDA Nâng cao
> Notebook phân tích chuyên sâu với visualization đầy đủ.

**Nội dung:**
- Chuyển đổi CSV → Parquet (nén zstd)
- Phân tích ~42.4M bản ghi với Polars streaming
- Daily Visitor Trend chart
- Price Daily Trend chart cho sản phẩm cụ thể
- Phân tích phân phối danh mục với treemap (squarify)

---

##  Hướng dẫn chạy

### Yêu cầu hệ thống

```
- Python 3.8+
- Java 8+ (cho PySpark)
- RAM: tối thiểu 16GB (khuyến nghị 32GB+)
- Disk: 20GB+ trống
```

### Cài đặt dependencies

```bash
pip install pyspark lightgbm scikit-learn matplotlib polars squarify numpy
```

### Cấu hình đường dẫn dữ liệu

Trước khi chạy, cập nhật đường dẫn CSV trong notebook:

```python
# Trong notebookdef7110166.ipynb
CSV_PATH = "/path/to/your/2019-Oct.csv"

# Trong process_data_v2.ipynb  
CSV_PATH = "/path/to/your/2019-Oct.csv"
```

>  **Lưu ý**: Mặc định cấu hình cho môi trường Kaggle (`/kaggle/input/...`). Cần thay đổi path khi chạy local.

### Cấu hình Spark (tuỳ chỉnh theo RAM)

```python
spark = SparkSession.builder \
    .appName("EcommercePrediction") \
    .config("spark.driver.memory", "8g")    # Điều chỉnh theo RAM \
    .config("spark.executor.memory", "8g")  # Điều chỉnh theo RAM \
    .getOrCreate()
```


## 📈 Kết quả mong đợi

| Metric | Target |
|--------|--------|
| ROC-AUC | > 0.85 |
| Precision | > 0.70 |
| Recall | > 0.65 |

> **Ghi chú**: Kết quả thực tế phụ thuộc vào hyperparameter tuning và chất lượng feature engineering.

---

## 🔄 Luồng dữ liệu chi tiết

```
CSV File (2019-Oct.csv)
        │
        ▼ [Polars: scan_csv → sink_parquet]
Parquet File (zstd compressed)
        │
        ▼ [PySpark: spark.read.parquet]
Spark DataFrame (42M+ rows)
        │
        ▼ [Filter nulls]
Clean DataFrame
        │
        ▼ [Window functions + GroupBy aggregations]
Feature Matrix (user × product level)
        │
        ▼ [Label: is_purchased]
Labeled Dataset
        │
        ▼ [Downsampling ~300K rows]
Balanced Training Set
        │
        ▼ [Train/Val split 80/20]
    ┌───┴───┐
  Train   Val
    │       │
    ▼       ▼
  LightGBM Training
        │
        ▼
    Trained Model
        │
        ▼
  Evaluation (AUC, F1, etc.)
```

---

## 🔍 Insights từ EDA

### 1. Hành vi người dùng
- Phần lớn người dùng chỉ xem sản phẩm mà không mua
- Tỷ lệ chuyển đổi từ xem → mua rất thấp (~1-5%)
- Session ngắn (1-3 sản phẩm) chiếm đa số

### 2. Phân tích sản phẩm
- Điện tử (smartphone, laptop) có giá cao nhưng lượt xem lớn
- Giá sản phẩm tương đối ổn định qua thời gian
- Brand "unknown" chiếm tỷ lệ đáng kể → cần xử lý cẩn thận

### 3. Xu hướng thời gian
- Lượt truy cập tăng vào cuối tháng 10/2019
- Có thể liên quan đến các sự kiện khuyến mại (Halloween sales)

---

##  Cải tiến tiếp theo

- [ ] **Hyperparameter Tuning**: Sử dụng Optuna hoặc Ray Tune
- [ ] **Feature Selection**: SHAP values để chọn feature quan trọng
- [ ] **Cross-validation**: K-fold để đánh giá tốt hơn
- [ ] **Real-time Serving**: Triển khai model với FastAPI/Flask
- [ ] **Monitoring**: Drift detection cho production
- [ ] **More Features**: Time-based features (giờ trong ngày, ngày trong tuần)

---

##  Dataset

Dữ liệu từ: [eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop) trên Kaggle.

---

