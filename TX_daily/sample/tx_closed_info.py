import requests
import pandas as pd
from datetime import datetime

# 定義資料來源 API
api_url = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date=20240930&response=json"

# 發送請求並取得資料
response = requests.get(api_url)
data = response.json()

# 檢查是否成功取得資料
if data["stat"] == "OK":
    # 篩選出需要的欄位：日期、成交金額、加權指數、漲跌點數
    records = []
    for record in data["data"]:
        date = record[0]
        # 轉換民國年為西元年
        date = str(int(date.split("/")[0]) + 1911) + "/" + date.split("/")[1] + "/" + date.split("/")[2]
        date = datetime.strptime(date, "%Y/%m/%d")  # 轉換為 datetime 格式
        amount = record[1]
        index = record[4]
        change = record[5]
        records.append([date, amount, index, change])
    
    # 建立 DataFrame
    df = pd.DataFrame(records, columns=["日期", "成交金額", "加權指數", "漲跌點數"])
    
    # 排序日期
    df = df.sort_values("日期")
    
    # 讀取已有的 CSV 檔案，若無則建立一個新檔案
    file_path = "twse_data_filtered.csv"
    try:
        existing_df = pd.read_csv(file_path)
        # 將日期欄位轉換為 datetime 格式，確保格式一致
        existing_df["日期"] = pd.to_datetime(existing_df["日期"])
        # 合併新的資料，去除重複的部分（根據日期）
        combined_df = pd.concat([existing_df, df]).drop_duplicates(subset="日期", keep="last")
    except FileNotFoundError:
        # 若檔案不存在，使用新的資料
        combined_df = df

    # 儲存資料至 CSV
    combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"資料已成功更新並儲存至 {file_path}")
else:
    print("無法從 API 中取得資料，請檢查 API 狀態。")
