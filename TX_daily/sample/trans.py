import pandas as pd
import chardet

# 定義原始檔案路徑與新檔案儲存路徑
# original_file = 'txf_data.csv'
# utf8_file = 'txf_data_utf8.csv'
original_file = 'txop_data.csv'
utf8_file = 'txop_data_utf8.csv'


# 自動檢測原檔案編碼
with open(original_file, 'rb') as f:
    result = chardet.detect(f.read())  # 檢測編碼
original_encoding = result['encoding']
print(f"檢測到的原始檔案編碼：{original_encoding}")

# 讀取原始 CSV 檔案並轉換為 UTF-8 編碼
try:
    # 使用檢測到的編碼讀取原始檔案
    df = pd.read_csv(original_file, encoding=original_encoding)
    
    # 將「身份別」欄位中的「外資及陸資」替換為「外資」
    if "身份別" in df.columns:
        df["身份別"] = df["身份別"].replace("外資及陸資", "外資")
        print("已將「身份別」欄位中的「外資及陸資」替換為「外資」")
    
    # 以 UTF-8 編碼儲存新檔案
    df.to_csv(utf8_file, index=False, encoding='utf-8-sig')
    print(f"成功將檔案轉換為 UTF-8 編碼並儲存至 {utf8_file}")
except Exception as e:
    print(f"轉換檔案時發生錯誤：{e}")
