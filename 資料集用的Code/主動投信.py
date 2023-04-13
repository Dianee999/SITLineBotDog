# 匯入套件
# =========================================================
import os
import pandas as pd
# =========================================================

# 前處理
# =========================================================
# 指定讀取資料夾路徑
folder_path = 'D:/Datasets/投信/2022投信'

# =========================================================

# 處理
# =========================================================
# 讀取資料夾內所有CSV檔案
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        # 讀取CSV檔案
        df = pd.read_csv(file_path, encoding = 'cp950')
        # 在這裡對df進行你需要的操作
        
        # 找出所有符合台灣境內ETF的行列
        templs = df.index[df["是否為台灣境內ETF"] == "是"].tolist()
        
        # 利用For迴圈，刪除特定的行
        for i in range(len(templs)):
            df.drop( templs[i], inplace=True)
        # 刪除整個欄位
        df.drop(['是否為台灣境內ETF'], axis = 1, inplace = True)
        
        # 設定要輸出的csv
        output_name = filename.replace('投信.csv', '')
        df.to_csv(f'D:/DataSets/主動投信/2022/{output_name}主動投信.csv',\
                  encoding = 'cp950', index = False)
        
# =========================================================