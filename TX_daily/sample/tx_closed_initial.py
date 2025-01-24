import requests
import pandas as pd
from datetime import datetime

# 定義要抓取資料的起始年份和月份
current_year = datetime.now().year
start_month = 1

# 定義儲存檔案路徑
file_path = "tx_closed_data.csv"

# 建立一個空的 DataFrame 來儲存所有月份的資料
all_data = pd.DataFrame(columns=["日期", "成交金額", "加權指數", "漲跌點數"])

# 依月份逐月抓取資料
for month in range(start_month, datetime.now().month + 1):
    # 格式化月份為兩位數
    formatted_month = f"{month:02d}"
    
    # 組合 API 查詢 URL（查詢格式為：YYYYMM）
    date_param = f"{current_year}{formatted_month}01"
    api_url = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={date_param}&response=json"
    
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
        
        # 將當月的資料建立為 DataFrame
        df_month = pd.DataFrame(records, columns=["日期", "成交金額", "加權指數", "漲跌點數"])
        
        # 合併當月資料到總資料中
        all_data = pd.concat([all_data, df_month], ignore_index=True)
        print(f"成功取得 {current_year} 年 {month} 月的資料，並已合併至總資料中。")
    else:
        print(f"無法取得 {current_year} 年 {month} 月的資料，請檢查 API 狀態。")

# 排序所有資料
all_data = all_data.sort_values("日期")

# 儲存資料至 CSV
all_data.to_csv(file_path, index=False, encoding="utf-8-sig")
print(f"所有資料已成功儲存至 {file_path} 中。")
