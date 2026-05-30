# Phân Tích Hành Vi Thương Mại Điện Tử

Dự án phân tích dữ liệu hành vi người dùng trên nền tảng thương mại điện tử, sử dụng dataset **eCommerce behavior data from multi-category store**.
Mục tiêu chính là xây dựng pipeline xử lý dữ liệu, phân tích hành vi người dùng, trích xuất đặc trưng và huấn luyện mô hình dự đoán khả năng mua hàng.

---

## 1. Tổng quan dự án

Dữ liệu gồm các hành vi của người dùng như:

* `view`: xem sản phẩm
* `cart`: thêm sản phẩm vào giỏ hàng
* `remove_from_cart`: xóa sản phẩm khỏi giỏ hàng
* `purchase`: mua hàng

Dự án tập trung vào các bài toán chính:

* Làm sạch và xử lý dữ liệu hành vi người dùng.
* Phân tích khám phá dữ liệu thương mại điện tử.
* Tạo đặc trưng theo phiên truy cập của người dùng.
* Dự đoán khả năng một phiên truy cập có phát sinh mua hàng hay không.
* Xây dựng pipeline gợi ý sản phẩm dựa trên lịch sử tương tác.

---


## 2. Pipeline xử lý

Pipeline của dự án được xây dựng theo luồng xử lý từ dữ liệu thô đến mô hình dự đoán và hệ thống gợi ý sản phẩm.

```text
Raw Data
   ↓
Data Cleaning & Preprocessing
   ↓
Exploratory Data Analysis
   ↓
Feature Engineering
   ↓
Model Training / Recommendation
   ↓
Evaluation & Saved Results
```

### Bước 1: Data Cleaning & Preprocessing

Mục tiêu của bước này là làm sạch dữ liệu gốc và chuẩn hóa dữ liệu để phục vụ cho các bước phân tích sau.

Các công việc chính:

* Đọc dữ liệu hành vi người dùng từ file CSV.
* Loại bỏ các bản ghi bị thiếu thông tin quan trọng như `user_id`, `user_session`, `product_id`, `price`.
* Lọc bỏ các sản phẩm có giá không hợp lệ, ví dụ `price <= 0`.
* Chuẩn hóa cột thời gian `event_time` về đúng định dạng datetime.
* Tạo thêm các trường thời gian như giờ, ngày, thứ trong tuần để phục vụ phân tích.
* Xử lý giá trị thiếu trong các cột như `brand`, `category_code`.
* Lưu dữ liệu đã xử lý thành checkpoint để tránh phải xử lý lại từ đầu.

Đầu ra của bước này là dữ liệu đã được làm sạch và sẵn sàng cho phân tích.

---

### Bước 2: Exploratory Data Analysis

Mục tiêu của bước này là khám phá dữ liệu để hiểu rõ hơn về hành vi người dùng trên nền tảng thương mại điện tử.

Các phân tích chính:

* Thống kê số lượng từng loại hành vi: `view`, `cart`, `remove_from_cart`, `purchase`.
* Phân tích tỷ lệ chuyển đổi từ xem sản phẩm sang thêm vào giỏ hàng và mua hàng.
* Phân tích các sản phẩm, danh mục và thương hiệu được quan tâm nhiều nhất.
* Phân tích hành vi người dùng theo thời gian như giờ trong ngày hoặc ngày trong tuần.
* Quan sát sự phân bố giá sản phẩm và mối liên hệ giữa giá với hành vi mua hàng.

Đầu ra của bước này là các bảng thống kê và biểu đồ giúp hiểu dữ liệu trước khi xây dựng mô hình.

---

### Bước 3: Feature Engineering

Mục tiêu của bước này là biến dữ liệu sự kiện rời rạc thành các đặc trưng có thể dùng cho mô hình Machine Learning.

Dữ liệu được tổng hợp theo từng phiên truy cập `user_session`.
Mỗi phiên truy cập sẽ được biểu diễn bằng một tập đặc trưng như:

* Tổng số sự kiện trong phiên.
* Số lượt xem sản phẩm.
* Số lượt thêm vào giỏ hàng.
* Số sản phẩm khác nhau đã tương tác.
* Số thương hiệu và danh mục khác nhau.
* Giá trung bình, giá lớn nhất, giá nhỏ nhất trong phiên.
* Thời điểm bắt đầu phiên.
* Phiên có xảy ra vào cuối tuần hay không.

Nhãn dự đoán được tạo như sau:

```text
label = 1 nếu phiên có hành vi purchase
label = 0 nếu phiên không có hành vi purchase
```

Đầu ra của bước này là bảng dữ liệu dạng session-level, trong đó mỗi dòng tương ứng với một phiên truy cập của người dùng.

---

### Bước 4: Model Training và Recommendation

Ở bước này, dự án thực hiện hai hướng xử lý chính.

Thứ nhất là bài toán dự đoán khả năng mua hàng:

* Sử dụng các đặc trưng đã tạo ở bước Feature Engineering.
* Chia dữ liệu thành tập train và test.
* Huấn luyện mô hình LightGBM để dự đoán phiên truy cập có phát sinh mua hàng hay không.
* Đánh giá mô hình bằng các chỉ số Accuracy, Precision, Recall, F1-score và ROC-AUC.
* Lưu mô hình đã huấn luyện để có thể sử dụng lại.

Thứ hai là pipeline gợi ý sản phẩm:

* Xây dựng lịch sử tương tác giữa người dùng và sản phẩm.
* Gán trọng số cho từng loại hành vi, ví dụ `purchase` quan trọng hơn `cart`, `cart` quan trọng hơn `view`.
* Tạo dữ liệu tương tác user-product.
* Đưa ra danh sách sản phẩm gợi ý dựa trên hành vi trước đó của người dùng.

Đầu ra của bước này gồm model dự đoán mua hàng, kết quả đánh giá và pipeline gợi ý sản phẩm.

---

### Bước 5: Evaluation & Saved Results

Sau khi huấn luyện và đánh giá, các kết quả quan trọng được lưu lại để phục vụ báo cáo và tái sử dụng.

Các kết quả đầu ra có thể gồm:

```text
output/models/
output/plots/
output/data/
```

Trong đó:

* `output/models/`: lưu mô hình đã huấn luyện.
* `output/plots/`: lưu biểu đồ đánh giá mô hình.
* `output/data/`: lưu dữ liệu đã xử lý hoặc dữ liệu đặc trưng.


## 3. Cấu trúc file

```text
btlkpdln11/
├── README.md
├── kaggle_01_purchase_prediction.py
├── process_data.ipynb
├── process_data_v2.ipynb
├── process_data_v4.ipynb.ipynb
└── recommendation_pipeline_v1.ipynb
```

Mô tả nhanh:

| File                               | Mô tả                                       |
| ---------------------------------- | ------------------------------------------- |
| `process_data.ipynb`               | Notebook xử lý dữ liệu ban đầu              |
| `process_data_v2.ipynb`            | Notebook làm sạch dữ liệu và tạo checkpoint |
| `process_data_v4.ipynb.ipynb`      | Phiên bản xử lý dữ liệu nâng cấp |
| `recommendation_pipeline_v1.ipynb` | Pipeline gợi ý sản phẩm                     |
| `kaggle_01_purchase_prediction.py` | Script huấn luyện mô hình dự đoán mua hàng  |

---

## 4. Quick Start

Clone repository:

```bash
git clone https://github.com/hacao404/btlkpdln11.git
cd btlkpdln11
```

Tạo môi trường ảo:

```bash
python -m venv .venv
.venv\Scripts\activate
```

Cài thư viện cần thiết:

```bash
pip install pandas numpy polars scikit-learn lightgbm matplotlib seaborn jupyter pyarrow
```

Chạy notebook xử lý dữ liệu:

```bash
jupyter notebook
```

Sau đó mở và chạy:

```text
process_data_v2.ipynb
```

Chạy mô hình dự đoán mua hàng:

```bash
python kaggle_01_purchase_prediction.py
```

Chạy pipeline gợi ý sản phẩm bằng notebook:

```text
recommendation_pipeline_v1.ipynb
```

Sau khi chạy xong, kết quả có thể bao gồm:

```text
output/models/
output/plots/
output/data/
```

Trong đó:

* `output/models/`: lưu mô hình đã huấn luyện.
* `output/plots/`: lưu biểu đồ đánh giá.
* `output/data/`: lưu dữ liệu đã xử lý hoặc dữ liệu tổng hợp.
