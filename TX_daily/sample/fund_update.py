import requests
import pandas as pd
from datetime import datetime
import os

# 定義資料儲存檔案路徑
file_path = "fund_data.csv"

def fetch_fund_data_for_today():
    """
    取得最新一天的三大法人買賣金額資料。
    
    回傳:
        DataFrame: 當日三大法人買賣金額資料（若成功取得）
        None: 若取得資料失敗
    """
    # 取得今天的日期（或測試日期）
    current_date = datetime.now().strftime("%Y%m%d")
    api_url = f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={current_date}&response=json"
    
    try:
        # 發送請求並檢查狀態碼
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # 如果狀態碼不是 200，會引發 HTTPError

        # 解析 JSON 資料
        data = response.json()
        if data["stat"] == "OK":
            # 篩選出需要的欄位：日期、單位名稱、買進金額、賣出金額、買賣差額
            records = []
            for record in data["data"]:
                unit_name = record[0]
                buy_amount = record[1]
                sell_amount = record[2]
                diff = record[3]
                records.append([current_date, unit_name, buy_amount, sell_amount, diff])
            
            # 建立 DataFrame 並回傳
            df = pd.DataFrame(records, columns=["日期", "單位名稱", "買進金額", "賣出金額", "買賣差額"])
            df["日期"] = pd.to_datetime(df["日期"], format="%Y%m%d")  # 統一日期格式
            return df
        else:
            print(f"無法取得 {current_date} 的資料，API 回應：{data['stat']}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"取得資料時發生錯誤：{e}")
        return None
    except ValueError:
        print(f"回應資料無法解析為 JSON 格式，可能是 API 回應異常。")
        return None

def update_fund_data(file_path, new_data):
    """
    更新 CSV 檔案，將新資料合併進現有的資料中。
    
    參數:
        file_path (str): CSV 檔案路徑
        new_data (DataFrame): 新取得的三大法人買賣金額資料
    """
    # 檢查 CSV 檔案是否存在
    if os.path.exists(file_path):
        try:
            # 讀取現有的 CSV 資料
            existing_df = pd.read_csv(file_path)
            existing_df["日期"] = pd.to_datetime(existing_df["日期"], format="%Y-%m-%d")  # 修改格式為 %Y-%m-%d

            # 將新資料的日期轉換為 datetime 格式
            new_data["日期"] = pd.to_datetime(new_data["日期"], format="%Y-%m-%d")  # 確保格式一致

            # 找出新資料中尚未更新的日期，過濾掉已存在的日期資料
            new_dates = set(new_data["日期"]) - set(existing_df["日期"])
            df_new_filtered = new_data[new_data["日期"].isin(new_dates)]

            # 若有新資料，則合併並儲存
            if not df_new_filtered.empty:
                combined_df = pd.concat([existing_df, df_new_filtered], ignore_index=True).drop_duplicates(subset=["日期", "單位名稱"]).sort_values("日期")
                combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
                print(f"新增了 {len(df_new_filtered)} 筆資料並更新至 {file_path}")
            else:
                print("無新增資料，CSV 檔案未變更。")
        except Exception as e:
            print(f"讀取或更新 CSV 檔案時發生錯誤：{e}")
    else:
        # 如果檔案不存在，則直接儲存新的資料
        new_data.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"檔案不存在，已創建新檔案並儲存 {len(new_data)} 筆資料至 {file_path}")

def main():
    """主程式執行區，負責取得資料並更新 CSV 檔案"""
    # 取得最新一天的三大法人買賣金額資料
    latest_data = fetch_fund_data_for_today()
    if latest_data is not None:
        # 更新資料至 CSV 檔案
        update_fund_data(file_path, latest_data)
    else:
        print("無法取得最新的資料，請檢查 API 狀態或網路連線。")

# 執行主程式
if __name__ == "__main__":
    main()
