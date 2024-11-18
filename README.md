# Đồ án "Phân tích dữ liệu sân bay" gồm:
- _File báo cáo:_ Report_KyThuatXuLyDuLieuLon.pdf
- _Slide báo cáo:_ Slide_KyThuatXuLyDuLieuLon.pdf
- _File code Cào dữ liệu 50 sân bay đông đúc nhất thế giới:_ rawdata.py
- _File code Phân tích dữ liệu sân bay ở Việt Nam:_ prediction.py
- _File data 50 sân bay đông đúc nhất thế giới từ năm 2017 đến 2023:_ airport_data_2017_2023.csv
- _File data các sân bay ở Việt Nam từ năm 2012 đến 2023:_ merged_airports_data_Vietnam.csv

# Web - App: 🛫 Top 50 sân bay đông đúc nhất

Dự án này hiển thị thông tin về top 50 sân bay đông đúc nhất dựa trên dữ liệu hành khách từ năm 2017 đến 2023. Ứng dụng được xây dựng bằng Streamlit, cung cấp giao diện tương tác để lọc dữ liệu theo quốc gia và thời gian, đồng thời hiển thị biểu đồ trực quan về lưu lượng hành khách.

---

## **1. Tính Năng**

- Lọc dữ liệu theo **năm** và **quốc gia**.
- Hiển thị biểu đồ tương tác về số lượng hành khách theo thời gian.
- So sánh số lượng hành khách giữa các quốc gia.
- Biểu diễn dữ liệu trực quan, dễ sử dụng.

---

## **2. Công Nghệ Sử Dụng**

- **Python**: Ngôn ngữ lập trình chính.
- **Streamlit**: Tạo ứng dụng web nhanh chóng và dễ dàng.
- **Pandas**: Xử lý và phân tích dữ liệu.
- **Matplotlib / Plotly**: Vẽ biểu đồ trực quan (nếu cần).

---

## **3. Yêu Cầu Hệ Thống**

- Python >= 3.8
- Các thư viện Python:
  - `streamlit`
  - `pandas`

---

## **4. Hướng Dẫn Cài Đặt**

### Bước 1: Clone Dự Án
```bash
git clone https://github.com/<username>/<repository-name>.git
cd <repository-name>
```

### Bước 2: Cài Đặt Virtual Environment
```bash
python -m venv venv
source venv/bin/activate      # Trên Linux/macOS
venv\Scripts\activate         # Trên Windows
```

### Bước 3: Cài Đặt Thư Viện Yêu Cầu
```bash
pip install -r requirements.txt
```

### Bước 4: Chạy Ứng Dụng
```bash
streamlit run app.py
```

---

## **7. File Dữ Liệu**
File: `airport_data_2017_2023.csv`
- Thông tin: Tên sân bay, quốc gia, số hành khách (2017-2023).

---

## **8. Tác Giả**
Dự án được phát triển bởi Vũ Thi Thi, Nguyễn Lê Thành Phước, Lê Thị Mai Thảo.


---

## **10. Giấy Phép**

Dự án này được phân phối theo giấy phép **MIT License**. Xem file `LICENSE` để biết thêm chi tiết.
