import requests
from bs4 import BeautifulSoup
import pandas as pd

# ====== 參數設定區 ======
TARGET_URL = "https://www.taifex.com.tw/cht/3/optContractsDate"  # 目標網址
CSV_FILE = 'txop_data.csv'  # 儲存資料的 CSV 檔案名稱

# 定義表格的標題名稱
HEADERS = [
    "日期", "商品名稱", "身份別", "多方交易口數", "多方交易契約金額(千元)",
    "空方交易口數", "空方交易契約金額(千元)", "多空交易口數淨額",
    "多空交易契約金額淨額(千元)", "多方未平倉口數", "多方未平倉契約金額(千元)",
    "空方未平倉口數", "空方未平倉契約金額(千元)", "多空未平倉口數淨額",
    "多空未平倉契約金額淨額(千元)"
]

# ====== 函數定義區 ======
def fetch_page(url):
    """抓取目標網址的網頁內容"""
    response = requests.get(url)
    response.encoding = 'utf-8'  # 設定編碼以正確解析中文
    return response.text


def extract_date(soup):
    """從網頁中提取日期資訊"""
    DATE_CLASS = 'right'  # 日期資訊所在的 <span> class 名稱
    date_span = soup.find('span', class_=DATE_CLASS)
    if date_span:
        date_text = date_span.get_text(strip=True)
        return date_text.replace("日期", "").strip()
    else:
        print("無法找到日期資訊，請檢查網頁結構。")
        return "未知日期"


def extract_table_data(soup, num_rows):
    """提取目標表格中的數據"""
    SECTION_CLASS = 'section'  # 目標 <div> 的 class 名稱
    TABLE_CLASS = 'table_f table-sticky-3 w-1000'  # 目標表格的 class 名稱

    sections = soup.find_all('div', class_=SECTION_CLASS)
    if len(sections) >= 3:
        target_section = sections[2]  # 取得第三個 <div class="section">
        table = target_section.find('table', {'class': TABLE_CLASS})
        if table:
            tbody = table.find('tbody')
            rows = []
            count = 0
            for row in tbody.find_all('tr'):
                if count < num_rows:
                    columns = row.find_all('td')
                    if len(columns) > 2:
                        if count == 0:
                            identity = columns[2].get_text(strip=True)
                            row_data = [col.get_text(strip=True) for col in columns[3:]]
                        else:
                            identity = columns[0].get_text(strip=True)
                            row_data = [col.get_text(strip=True) for col in columns[1:]]
                        row_with_metadata = [date, "臺股期貨", identity] + row_data
                        rows.append(row_with_metadata)
                    count += 1
                else:
                    break
            return rows
        else:
            print("無法找到目標表格，請檢查網頁結構。")
            return []
    else:
        print("無法找到指定的 <div class='section'> 結構，請檢查網頁結構。")
        return []


def save_to_csv(new_data, file_name):
    """將新的資料合併至 CSV 並保存"""
    try:
        existing_df = pd.read_csv(file_name, encoding='utf-8-sig')
    except UnicodeDecodeError:
        existing_df = pd.read_csv(file_name, encoding='ISO-8859-1')

    merged_df = pd.concat([existing_df, new_data]).drop_duplicates(
        subset=["日期", "商品名稱", "身份別"], keep='last'
    ).sort_values(by=["日期", "商品名稱", "身份別"])

    merged_df.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"資料已更新並儲存至 {file_name}")


# ====== 主程式執行區 ======
# 抓取網頁內容
page_content = fetch_page(TARGET_URL)
soup = BeautifulSoup(page_content, 'html.parser')

# 提取日期資訊
date = extract_date(soup)

# 提取表格資料
NUM_ROWS_TO_EXTRACT = 3  # 要提取的行數
table_data = extract_table_data(soup, NUM_ROWS_TO_EXTRACT)

if table_data:
    # 將資料轉換為 DataFrame 格式
    df = pd.DataFrame(table_data, columns=HEADERS)
    print(df)

    # 保存資料至 CSV 檔案
    save_to_csv(df, CSV_FILE)
else:
    print("無法提取表格數據。")
