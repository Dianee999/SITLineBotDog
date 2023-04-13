# 匯入套件
# =========================================================
import os
import pandas as pd
# =========================================================

# 前處理
# =========================================================
# 指定讀取資料夾路徑
folder_path = 'D:/DataSets/股價日成交資訊/raw'

# =========================================================

# 處理
# =========================================================
# 讀取資料夾內所有CSV檔案
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        # 讀取CSV檔案
        df = pd.read_csv(file_path)
        # 在這裡對df進行你需要的操作
        # 刪除不必要的欄位
        try:
            df = df.drop(['stock_id','Trading_Volume',\
                'Trading_money','spread',\
                'Trading_turnover'], axis = 1)
            df = df.reset_index()
            df = df.drop(['index','Unnamed: 0'], axis = 1)
            df = df.rename(columns = {'open':'開盤價',\
                                 'max':'最高價',\
                                 'min':'最低價',\
                                 'close':'收盤價'})
            # 重新輸出csv
            df.to_csv(f'D:/DataSets/股價日成交資訊/{filename}',\
                      encoding = 'cp950')
        except:
            print(filename)
            continue
        
# =========================================================