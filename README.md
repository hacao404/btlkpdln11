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

* **Câu hỏi 1:** Xu hướng và lưu lượng tương tác hàng ngày của nền tảng trong tháng 11/2019 diễn ra như thế nào?
* **Câu hỏi 2:** Những danh mục sản phẩm (`category`) và thương hiệu (`brand`) nào thu hút được nhiều sự chú ý và mang lại doanh thu cao nhất?
* **Câu hỏi 3:** Hành trình của khách hàng từ việc xem (`view`), thêm vào giỏ (`cart`) cho đến khi quyết định mua hàng (`purchase`) có tỷ lệ chuyển đổi ra sao?

Quá trình bao gồm các bước: **Làm sạch dữ liệu (Data Cleaning)**, **Phân tích khám phá (EDA)**, và **Trích xuất đặc trưng (Feature Engineering)** để chuẩn bị cho các mô hình Máy học (Machine Learning) ở giai đoạn sau.

---

## <a name="2-tập-dữ-liệu"></a> 2. Tập dữ liệu

* **Nguồn dữ liệu:** [REES46 Marketing Platform & Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
* **File sử dụng:** `2019-Nov.csv` (Chứa dữ liệu hành vi của tháng 11 năm 2019).
* **Các đặc trưng chính (Features):**
  * `event_time`: Thời gian diễn ra sự kiện (chuẩn UTC).
  * `event_type`: Loại tương tác của người dùng (**view**, **cart**, **remove_from_cart**, **purchase**).
  * `product_id`, `category_id`, `category_code`: Thông tin định danh và phân loại sản phẩm.
  * `brand`: Tên thương hiệu.
  * `price`: Giá trị sản phẩm.
  * `user_id`, `user_session`: Mã định danh khách hàng và phiên hoạt động.


---

## <a name="3-mô-tả-các-tệp-tin"></a> 3. Mô tả các tệp tin

Cấu trúc hiện tại của Repository bao gồm các tệp phân tích chính:

* **`process_data.ipynb`**: Notebook sơ khởi. Chứa các bước load dữ liệu cơ bản, kiểm tra cấu trúc tập dữ liệu và xử lý các giá trị bị thiếu (missing values) ở mức độ ban đầu.
* **`process_data_v2.ipynb`**: Notebook phiên bản hoàn thiện hơn. Bao gồm các kỹ thuật lọc dữ liệu nhiễu, chuyển đổi kiểu dữ liệu (đặc biệt là xử lý chuỗi thời gian datetime), trích xuất đặc trưng mới và lưu trữ dữ liệu sau xử lý xuống dạng **checkpoint** để tối ưu hóa hiệu năng.

---

## <a name="4-khởi-động-nhanh-với-checkpoint"></a> 4. Khởi động nhanh với Checkpoint

Việc đọc và xử lý lại toàn bộ file `2019-Nov.csv` mỗi lần mở máy sẽ tiêu tốn rất nhiều thời gian và RAM. Vì vậy, nhóm đã thiết lập cơ chế **Checkpoint**. 

Sau khi chạy xong notebook `process_data_v2.ipynb`, một bản sao của dữ liệu đã được làm sạch và rút gọn sẽ được lưu lại. Bạn có thể sử dụng đoạn mã sau để load nhanh dữ liệu và bỏ qua bước tiền xử lý:

```python
import pandas as pd

# Đường dẫn tới file dữ liệu đã qua xử lý 
CHECKPOINT_PATH = 'data/processed/nov_2019_cleaned_checkpoint.csv'

try:
    # Tải dữ liệu checkpoint
    df = pd.read_csv(CHECKPOINT_PATH)
    print("Dữ liệu sẵn sàng để phân tích.")
    display(df.head())
except FileNotFoundError:
    print("Vui lòng chạy file process_data_v2.ipynb trước để tạo checkpoint.")
