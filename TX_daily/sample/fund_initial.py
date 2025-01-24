import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random

# 定義常數與全域變數
FILE_PATH = "fund_data.csv"  # 資料儲存的 CSV 檔案路徑
MIN_DELAY = 3  # 每次請求的最小延遲時間（秒）
MAX_DELAY = 5  # 每次請求的最大延遲時間（秒）
MAX_RETRIES = 3  # 最大重試次數

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
            # 發送請求並取得資料
            response = requests.get(api_url, timeout=10)  # 設置請求超時時間為 10 秒
            if response.status_code == 200:
                try:
                    data = response.json()  # 嘗試解析 JSON
                    if data["stat"] == "OK":
                        # 篩選出需要的欄位：日期、單位名稱、買進金額、賣出金額、買賣差額
                        records = []
                        for record in data["data"]:
                            unit_name = record[0]
                            buy_amount = record[1]
                            sell_amount = record[2]
                            diff = record[3]
                            records.append([date, unit_name, buy_amount, sell_amount, diff])

                        # 建立 DataFrame
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
    # 建立一個空的 DataFrame 來儲存所有日資料
    all_data = pd.DataFrame(columns=["日期", "單位名稱", "買進金額", "賣出金額", "買賣差額"])

    current_date = start_date
    while current_date <= end_date:
        # 取得指定日期的資料
        daily_data = fetch_fund_data(current_date)
        if daily_data is not None:
            # 合併每日資料到總資料中
            all_data = pd.concat([all_data, daily_data], ignore_index=True)

        # 每次請求間的延遲，避免過於頻繁的請求導致被封鎖 IP
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))  # 隨機延遲一段時間（例如 3 到 10 秒）

        # 日期增加一天
        current_date += timedelta(days=1)

    # 儲存所有資料至 CSV
    all_data.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"所有資料已成功儲存至 {file_path} 中。")


def main():
    """主程式執行區，負責設定抓取的日期範圍並執行抓取作業。"""
    # 取得當前年和起始日期（1月1日）
    current_year = datetime.now().year
    start_date = datetime(current_year, 10, 1)  # 設定起始日期為當年1月1日
    end_date = datetime.now()  # 設定結束日期為今天

    # 執行抓取並儲存資料
    fetch_and_save_fund_data(start_date, end_date, FILE_PATH)


# 執行主程式
if __name__ == "__main__":
    main()
