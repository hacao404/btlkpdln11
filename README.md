# Phân Tích Hành Vi Thương Mại Điện Tử (eCommerce Behavior Analysis) 

**Bài tập lớn môn Khai phá dữ liệu (Data Mining) - Nhóm 11**

Kho lưu trữ này chứa mã nguồn phục vụ cho việc tiền xử lý, phân tích và khai phá tập dữ liệu hành vi người dùng trên nền tảng thương mại điện tử đa ngành hàng (eCommerce behavior data from multi-category store).

###  Mục lục

1. [Mô tả dự án (Description & Motivation)](#1-mô-tả-dự-án)
2. [Tập dữ liệu (Dataset)](#2-tập-dữ-liệu)
3. [Mô tả tệp tin (File Descriptions)](#3-mô-tả-các-tệp-tin)
4. [Khởi động nhanh (Quick Start với Checkpoint)](#4-khởi-động-nhanh-với-checkpoint)

---

## <a name="1-mô-tả-dự-án"></a> 1. Mô tả dự án

Mục tiêu chính của dự án này là thực hiện toàn bộ đường ống (pipeline) làm việc với dữ liệu lớn, thông qua đó khám phá và trả lời các câu hỏi kinh doanh thực tế:
Quá trình bao gồm các bước: **Làm sạch dữ liệu (Data Cleaning)**, **Phân tích khám phá (EDA)**, và **Trích xuất đặc trưng (Feature Engineering)** để chuẩn bị cho các mô hình Máy học (Machine Learning) ở giai đoạn sau.

---

## <a name="2-tập-dữ-liệu"></a> 2. Tập dữ liệu

* **Nguồn dữ liệu:** [REES46 Marketing Platform & Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
* **Các đặc trưng chính (Features):**
  * `event_time`: Thời gian diễn ra sự kiện (chuẩn UTC).
  * `event_type`: Loại tương tác của người dùng (**view**, **cart**, **remove_from_cart**, **purchase**).
  * `product_id`, `category_id`, `category_code`: Thông tin định danh và phân loại sản phẩm.
  * `brand`: Tên thương hiệu.
  * `price`: Giá trị sản phẩm.
  * `user_id`, `user_session`: Mã định danh khách hàng và phiên hoạt động.


---



## <a name="4-khởi-động-nhanh-với-checkpoint"></a> 3. Khởi động nhanh với Checkpoint

Việc đọc và xử lý lại toàn bộ file `2019-Nov.csv` mỗi lần mở máy sẽ tiêu tốn rất nhiều thời gian và RAM. Vì vậy, nhóm đã thiết lập cơ chế **Checkpoint**. 

Sau khi chạy xong notebook `process_data_v2.ipynb`, một bản sao của dữ liệu đã được làm sạch và rút gọn sẽ được lưu lại. Bạn có thể sử dụng đoạn mã sau để load nhanh dữ liệu và bỏ qua bước tiền xử lý:

```python
import pandas as pd

CHECKPOINT_PATH = 'data/processed/nov_2019_cleaned_checkpoint.csv'

try:
    df = pd.read_csv(CHECKPOINT_PATH)
    print("Dữ liệu sẵn sàng để phân tích.")
    display(df.head())
except FileNotFoundError:
    print("Vui lòng chạy file process_data_v2.ipynb trước để tạo checkpoint.")
