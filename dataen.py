
from bs4 import BeautifulSoup
import requests
import pandas as pd
import pyodbc
import re

# Hàm làm sạch dữ liệu cho cột 'Rank' để loại bỏ các ký tự không phải số
def clean_rank(rank_value):
    cleaned_rank = re.sub(r'[^\d]', '', rank_value)
    return int(cleaned_rank) if cleaned_rank.isdigit() else None

# Hàm làm sạch dữ liệu cho cột 'Total passengers' để loại bỏ dấu phẩy
def clean_total_passengers(passengers_value):
    cleaned_passengers = passengers_value.replace(',', '')
    return int(cleaned_passengers) if cleaned_passengers.isdigit() else None

# Hàm làm sạch dữ liệu cho cột '% change'
def clean_percent_change(percent_value):
    cleaned_percent = percent_value.replace('%', '').strip()
    try:
        return float(cleaned_percent)
    except ValueError:
        return None
# Hàm làm sạch dữ liệu cho cột Rank change
def clean_rank_change(rank_change_value):
    if rank_change_value == '?' or rank_change_value == '':
        return None  # Trả về None nếu là '?' hoặc chuỗi rỗng
    return int(rank_change_value)  # Chuyển đổi thành int nếu không phải là giá trị không hợp lệ

# URL của trang Wikipedia
url = 'https://en.wikipedia.org/wiki/List_of_busiest_airports_by_passenger_traffic'

# Gửi yêu cầu GET tới URL
page = requests.get(url)

# Phân tích HTML với BeautifulSoup
soup = BeautifulSoup(page.text, 'html.parser')

# Tìm bảng dữ liệu (ở đây bảng thứ 2 chứa thông tin mong muốn)
table = soup.find_all('table')[1]

# Đặt tên các cột cho DataFrame
col_table_titles = ['Rank', 'Airport', 'Location', 'Country', 'Code(IATA/ICAO)', 'Total_passengers', 'Rank_change', 'Percent_change']
df = pd.DataFrame(columns=col_table_titles)

# Lấy tất cả các hàng từ bảng
column_data = table.find_all('tr')

# Vòng lặp để lấy dữ liệu từ các hàng
for row in column_data[1:]:
    row_data = row.find_all('td')
    individual_row_data = [data.text.strip() for data in row_data]
     # Làm sạch dữ liệu cột Rank
    individual_row_data[0] = clean_rank(individual_row_data[0])
     # Làm sạch dữ liệu cột 'Total passengers'
    individual_row_data[5] = clean_total_passengers(individual_row_data[5])
     # Làm sạch dữ liệu cột '% change'
    individual_row_data[7] = clean_percent_change(individual_row_data[7])
     # Làm sạch dữ liệu cột 'Rank change'
    individual_row_data[6] = clean_rank_change(individual_row_data[6]) 
    length = len(df)
    df.loc[length] = individual_row_data

# Tách cột 'Code(IATA/ICAO)' thành 2 cột riêng biệt
df[['IATA', 'ICAO']] = df['Code(IATA/ICAO)'].str.split('/', expand=True)

# Xóa cột gốc 'Code(IATA/ICAO)'
df = df.drop('Code(IATA/ICAO)', axis=1)
# Xuất DataFrame ra file CSV
df.to_csv(r'C:\Users\THIS PC\OneDrive\Desktop\data_engineerairports1.csv', index=False)

# In kết quả để kiểm tra
conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=LAPTOP-BVTT8G0U\SQLEXPRESS;"
    "Database=troll;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()
# Xóa dữ liệu hiện có trong bảng airport_auto_new3
# cursor.execute("DELETE FROM airport_auto_new4")
# Tạo bảng SQL
cursor.execute(
    """
    CREATE TABLE demo (
    Rank int,
    Airport nvarchar(100) primary key,
    Location nvarchar(50),
    Country nvarchar(50),
    Total_passengers int,
    Rank_change int,
    Percent_change decimal,
    IATA nvarchar(50),
    ICAO  nvarchar(50)
    );
    """
)

# Chèn dữ liệu từ DataFrame vào bảng SQL
for row in df.itertuples():
    cursor.execute(
        """
        INSERT INTO demo (Rank, Airport, Location, Country, Total_passengers, Rank_change, Percent_change, IATA, ICAO)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        row.Rank,
        row.Airport,
        row.Location,
        row.Country,
        row.Total_passengers,
        row.Rank_change,
        row.Percent_change,
        row.IATA,
        row.ICAO
    )

# Lưu thay đổi vào cơ sở dữ liệu
conn.commit()

# Đóng kết nối
cursor.close()
conn.close()

print(df.head())
