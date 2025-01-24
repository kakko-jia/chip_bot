import requests
import pandas as pd
from datetime import datetime

# 定義儲存檔案路徑
file_path = "twse_data_filtered.csv"

# 定義取得資料的函數
def fetch_latest_twse_data():
    """取得當前月份的成交資料"""
    # 取得當前年和月份
    current_year = datetime.now().year
    current_month = datetime.now().month
    # 格式化月份為兩位數
    formatted_month = f"{current_month:02d}"
    # 組合 API 查詢 URL（查詢格式為：YYYYMM）
    date_param = f"{current_year}{formatted_month}01"
    api_url = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={date_param}&response=json"
    
    response = requests.get(api_url)
    data = response.json()
    
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
        return df
    else:
        print(f"無法取得最新月份的資料，API 回應：{data['stat']}")
        return None

# 取得最新月份的資料
latest_data = fetch_latest_twse_data()
if latest_data is not None:
    # 讀取已存在的 CSV 檔案，若檔案不存在則創建新的檔案
    try:
        # 如果 CSV 檔案已經存在，則讀取現有的 CSV 檔案
        existing_df = pd.read_csv(file_path)
        # 將日期欄位轉換為 datetime 格式
        existing_df["日期"] = pd.to_datetime(existing_df["日期"])
        
        # 找出新的資料（根據日期），過濾掉已存在的日期
        latest_data["日期"] = pd.to_datetime(latest_data["日期"])
        new_dates = set(latest_data["日期"]) - set(existing_df["日期"])
        df_new_filtered = latest_data[latest_data["日期"].isin(new_dates)]
        
        # 若有新資料，則合併並儲存
        if not df_new_filtered.empty:
            combined_df = pd.concat([existing_df, df_new_filtered]).drop_duplicates(subset="日期", keep="last").sort_values("日期")
            combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
            print(f"新增了 {len(df_new_filtered)} 筆資料並更新至 {file_path}")
        else:
            print("無新增資料，CSV 檔案未變更。")
            
    except FileNotFoundError:
        # 若檔案不存在，則直接儲存新的資料
        latest_data.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"檔案不存在，已創建新檔案並儲存 {len(latest_data)} 筆資料至 {file_path}")
else:
    print("無法取得最新月份的資料，請檢查 API 狀態。")
