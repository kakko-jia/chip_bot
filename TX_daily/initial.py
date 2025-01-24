import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random

# 定義常數與全域變數
FILE_PATH_FUND = "tx_fund_data.csv"  # 三大法人資料儲存的 CSV 檔案路徑
FILE_PATH_TWSE = "tx_closed_data.csv"  # 台灣證券交易所資料儲存的 CSV 檔案路徑
MIN_DELAY = 3  # 每次請求的最小延遲時間（秒）
MAX_DELAY = 5  # 每次請求的最大延遲時間（秒）
MAX_RETRIES = 3  # 最大重試次數

# ---- 三大法人資料抓取與儲存 ----
def fetch_fund_data(date):
    """
    從 API 取得指定日期的三大法人買賣金額資料。

    參數:
        date (datetime): 要取得資料的日期（datetime 格式）

    回傳:
        DataFrame: 包含指定日期的三大法人買賣金額資料
        None: 若取得資料失敗或無資料時
    """
    formatted_date = date.strftime("%Y%m%d")  # 格式化日期為 YYYYMMDD
    api_url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={formatted_date}&response=json"

    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            response = requests.get(api_url, timeout=10)  # 設置請求超時時間為 10 秒
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data["stat"] == "OK":
                        records = []
                        for record in data["data"]:
                            unit_name = record[0]
                            buy_amount = record[1]
                            sell_amount = record[2]
                            diff = record[3]
                            records.append([date, unit_name, buy_amount, sell_amount, diff])

                        df = pd.DataFrame(records, columns=["日期", "單位名稱", "買進金額", "賣出金額", "買賣差額"])
                        print(f"成功取得 {date.strftime('%Y/%m/%d')} 的資料。")
                        return df
                    else:
                        print(f"{date.strftime('%Y/%m/%d')} 無法取得資料或無符合條件的資料。")
                        return None
                except ValueError as e:
                    print(f"{date.strftime('%Y/%m/%d')} 回傳內容無法解析為 JSON，錯誤：{e}")
            else:
                print(f"{date.strftime('%Y/%m/%d')} 伺服器回應失敗，狀態碼：{response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"請求 {date.strftime('%Y/%m/%d')} 資料時發生錯誤，錯誤原因：{e}")

        retry_count += 1
        print(f"重試 {retry_count}/{MAX_RETRIES} 次...")
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))  # 隨機延遲一段時間再進行重試

    print(f"超過最大重試次數，跳過 {date.strftime('%Y/%m/%d')} 的資料。")
    return None

def fetch_and_save_fund_data(start_date, end_date, file_path):
    """
    抓取指定日期範圍內的所有三大法人買賣金額資料，並儲存至指定的 CSV 檔案中。

    參數:
        start_date (datetime): 要開始抓取的日期
        end_date (datetime): 要結束抓取的日期
        file_path (str): 儲存資料的 CSV 檔案路徑
    """
    all_data = pd.DataFrame(columns=["日期", "單位名稱", "買進金額", "賣出金額", "買賣差額"])

    current_date = start_date
    while current_date <= end_date:
        daily_data = fetch_fund_data(current_date)
        if daily_data is not None:
            all_data = pd.concat([all_data, daily_data], ignore_index=True)
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))  # 隨機延遲一段時間

        current_date += timedelta(days=1)

    # 儲存所有資料至 CSV
    all_data.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"三大法人資料已成功儲存至 {file_path} 中。")

# ---- 台灣證券交易所資料抓取與儲存 ----
def fetch_twse_data(start_month, end_month, current_year, file_path):
    """
    抓取當年起始月份至今的台灣證券交易所成交資料，並儲存至指定的 CSV 檔案中。

    參數:
        start_month (int): 起始月份
        end_month (int): 結束月份
        current_year (int): 當前年份
        file_path (str): 儲存資料的 CSV 檔案路徑
    """
    all_data = pd.DataFrame(columns=["日期", "成交金額", "加權指數", "漲跌點數"])

    for month in range(start_month, end_month + 1):
        formatted_month = f"{month:02d}"
        date_param = f"{current_year}{formatted_month}01"
        api_url = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={date_param}&response=json"

        response = requests.get(api_url)
        data = response.json()

        if data["stat"] == "OK":
            records = []
            for record in data["data"]:
                date = record[0]
                date = str(int(date.split("/")[0]) + 1911) + "/" + date.split("/")[1] + "/" + date.split("/")[2]
                date = datetime.strptime(date, "%Y/%m/%d")
                amount = record[2]
                index = record[4]
                change = record[5]
                records.append([date, amount, index, change])

            df_month = pd.DataFrame(records, columns=["日期", "成交金額", "加權指數", "漲跌點數"])
            all_data = pd.concat([all_data, df_month], ignore_index=True)
            print(f"成功取得 {current_year} 年 {month} 月的資料，並已合併至總資料中。")
        else:
            print(f"無法取得 {current_year} 年 {month} 月的資料，請檢查 API 狀態。")

    all_data = all_data.sort_values("日期")
    all_data.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"台灣證券交易所資料已成功儲存至 {file_path} 中。")

def main():
    """主程式執行區，負責設定抓取的日期範圍並執行抓取作業。"""
    # 取得當前年與起始日期
    current_year = datetime.now().year
    start_date = datetime(current_year, 1, 1)
    end_date = datetime.now()

    # 執行三大法人資料抓取並儲存
    fetch_and_save_fund_data(start_date, end_date, FILE_PATH_FUND)

    # 執行台灣證券交易所資料抓取並儲存
    fetch_twse_data(start_month=1, end_month=datetime.now().month, current_year=current_year, file_path=FILE_PATH_TWSE)


# 執行主程式
if __name__ == "__main__":
    main()
