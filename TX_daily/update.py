import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import logging

# 設置 logging 參數
log_file = "update.log"
logging.basicConfig(
    filename=log_file,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    encoding='utf-8'
)

# 定義儲存檔案路徑
fund_data_file_path = "tx_fund_data.csv"
twse_data_file_path = "tx_closed_data.csv"
txf_data_file_path = "txf_data.csv"
txop_data_file_path = "txop_data.csv"

# 定義表格的標題名稱（期貨與選擇權資料）
HEADERS = [
    "日期", "商品名稱", "身份別", "多方交易口數", "多方交易契約金額(千元)",
    "空方交易口數", "空方交易契約金額(千元)", "多空交易口數淨額",
    "多空交易契約金額淨額(千元)", "多方未平倉口數", "多方未平倉契約金額(千元)",
    "空方未平倉口數", "空方未平倉契約金額(千元)", "多空未平倉口數淨額",
    "多空未平倉契約金額淨額(千元)"
]
TARGET_URL_TFX = "https://www.taifex.com.tw/cht/3/futContractsDate"  # 期貨目標網址
TARGET_URL_TXOP = "https://www.taifex.com.tw/cht/3/optContractsDate"  # 選擇權目標網址

# ====== 取得三大法人資料的函數 ======
def fetch_fund_data_for_today():
    current_date = datetime.now().strftime("%Y%m%d")
    # current_date = datetime(2024, 10, 25).strftime("%Y%m%d")
    api_url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={current_date}&response=json"
    
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data["stat"] == "OK":
            records = []
            for record in data["data"]:
                unit_name = record[0]
                buy_amount = record[1]
                sell_amount = record[2]
                diff = record[3]
                records.append([current_date, unit_name, buy_amount, sell_amount, diff])
            df = pd.DataFrame(records, columns=["日期", "單位名稱", "買進金額", "賣出金額", "買賣差額"])
            df["日期"] = pd.to_datetime(df["日期"], format="%Y%m%d")
            logging.info(f"Successfully retrieved institutional investor data for {current_date}.")
            return df
        else:
            logging.warning(f"Failed to retrieve institutional investor data for {current_date}. API Response: {data['stat']}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error : institutional investor data : {e}")
        return None

# ====== 取得 TWSE 成交資料的函數 ======
def fetch_latest_twse_data():
    current_year = datetime.now().year
    current_month = datetime.now().month
    formatted_month = f"{current_month:02d}"
    date_param = f"{current_year}{formatted_month}01"
    api_url = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={date_param}&response=json"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        if data["stat"] == "OK":
            records = []
            for record in data["data"]:
                date = str(int(record[0].split("/")[0]) + 1911) + "/" + record[0].split("/")[1] + "/" + record[0].split("/")[2]
                date = datetime.strptime(date, "%Y/%m/%d")
                amount = record[2]
                index = record[4]
                change = record[5]
                records.append([date, amount, index, change])
            df = pd.DataFrame(records, columns=["日期", "成交金額", "加權指數", "漲跌點數"])
            logging.info(f"Successfully retrieved TWSE trading data for {date_param}.")
            return df
        else:
            logging.warning(f"Failed to retrieve TWSE trading data for {date_param}. API Response: {data['stat']}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error : TWSE closed trading data : {e}")
        return None

# ====== 取得期貨或選擇權資料的共用函數 ======
def fetch_page(url):
    try:
        response = requests.get(url)
        response.encoding = 'utf-8'
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error : TXF or TXOP webpage : {e}")
        return None

def extract_date(soup):
    date_class = 'right'
    date_span = soup.find('span', class_=date_class)
    if date_span:
        date_text = date_span.get_text(strip=True)
        return date_text.replace("日期", "").strip()
    else:
        logging.warning("Failed to find date information. Please check the webpage structure.")
        return "Unknown Date"

def extract_table_data(soup, date, num_rows, product_name):
    section_class = 'section'
    table_class = 'table_f table-sticky-3 w-1000'
    sections = soup.find_all('div', class_=section_class)
    if len(sections) >= 3:
        target_section = sections[2]
        table = target_section.find('table', {'class': table_class})
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
                        row_with_metadata = [date, product_name, identity] + row_data
                        rows.append(row_with_metadata)
                    count += 1
                else:
                    break
            logging.info(f"Successfully extracted {product_name} table data. Total rows: {len(rows)}.")
            return rows
        else:
            logging.warning(f"Failed to find the target table for {product_name}. Please check the webpage structure.")
            return []
    else:
        logging.warning("Failed to find the required <div class='section'> structure. Please check the webpage structure.")
        return []

def fetch_txf_data():
    page_content = fetch_page(TARGET_URL_TFX)
    soup = BeautifulSoup(page_content, 'html.parser')
    date = extract_date(soup)
    NUM_ROWS_TO_EXTRACT = 3
    table_data = extract_table_data(soup, date, NUM_ROWS_TO_EXTRACT, "臺股期貨")
    if table_data:
        df = pd.DataFrame(table_data, columns=HEADERS)
        return df
    else:
        return None

def fetch_txop_data():
    page_content = fetch_page(TARGET_URL_TXOP)
    soup = BeautifulSoup(page_content, 'html.parser')
    date = extract_date(soup)
    NUM_ROWS_TO_EXTRACT = 3
    table_data = extract_table_data(soup, date, NUM_ROWS_TO_EXTRACT, "選擇權")
    if table_data:
        df = pd.DataFrame(table_data, columns=HEADERS)
        return df
    else:
        return None

# 定義單位名稱的排序順序
order = {
    "自營商(自行買賣)": 0,
    "自營商(避險)": 1,
    "投信": 2,
    "外資及陸資(不含外資自營商)": 3,
    "外資自營商": 4,
    "合計": 5
}

# ====== 資料更新函數 ======
def update_data(file_path, new_data, date_column="日期", unique_columns=None):
    """
    更新 CSV 檔案，將新資料合併進現有的資料中。
    
    參數:
        file_path (str): CSV 檔案路徑
        new_data (DataFrame): 新取得的資料
        date_column (str): 日期欄位名稱（預設為 "日期"）
        unique_columns (list): 用於辨識資料唯一性的欄位名稱列表（若無指定則以 "日期" 為主）
    """
    if unique_columns is None:
        unique_columns = [date_column]  # 預設以日期欄位作為唯一性檢查欄位

    # 根據檔案名稱或資料欄位自動選擇 unique_columns
    if '商品名稱' in new_data.columns and '身份別' in new_data.columns:
        unique_columns = [date_column, "商品名稱", "身份別"]  # 期貨、選擇權資料用
    elif '單位名稱' in new_data.columns:
        unique_columns = [date_column, "單位名稱"]  # 三大法人資料用

    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            existing_df[date_column] = pd.to_datetime(existing_df[date_column])
            new_data[date_column] = pd.to_datetime(new_data[date_column])

            # 找出新資料中尚未更新的日期
            new_dates = set(new_data[date_column]) - set(existing_df[date_column])
            df_new_filtered = new_data[new_data[date_column].isin(new_dates)]

            # 若有新資料，則合併並儲存
            if not df_new_filtered.empty:
                combined_df = pd.concat([existing_df, df_new_filtered], ignore_index=True).drop_duplicates(subset=unique_columns).sort_values(date_column)
                combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
                logging.info(f"新增了 {len(df_new_filtered)} 筆資料並更新至 {file_path}")
            else:
                logging.info(f"無新增資料，{file_path} 檔案未變更。")
        except Exception as e:
            logging.error(f"讀取或更新 {file_path} CSV 檔案時發生錯誤：{e}")
    else:
        # 如果檔案不存在，則直接儲存新的資料
        new_data.to_csv(file_path, index=False, encoding="utf-8-sig")
        logging.info(f"檔案不存在，已創建新檔案並儲存 {len(new_data)} 筆資料至 {file_path}")

# ====== 主程式 ======
def main():
    date = datetime.now().strftime("%Y%m%d")
    logging.info("Start update {date}...".format(date=date))

    # 取得三大法人資料並更新
    fund_data = fetch_fund_data_for_today()
    if fund_data is not None:
        update_data(fund_data_file_path, fund_data)

    # 取得 TWSE 成交資料並更新
    twse_data = fetch_latest_twse_data()
    if twse_data is not None:
        update_data(twse_data_file_path, twse_data)

    # 取得臺股期貨資料並更新
    txf_data = fetch_txf_data()
    if txf_data is not None:
        update_data(txf_data_file_path, txf_data)

    # 取得選擇權資料並更新
    txop_data = fetch_txop_data()
    if txop_data is not None:
        update_data(txop_data_file_path, txop_data)

    logging.info("End...")

if __name__ == "__main__":
    main()
