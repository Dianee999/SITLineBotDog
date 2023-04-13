# 匯入套件
# =========================================================
from datetime import datetime
import os
import pandas as pd
# =========================================================

# 前處理
# =========================================================
# 指定讀取資料夾路徑
folder_path = 'D:/Project/資料集/2017/ETF'
testls = []

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
        
        # 先建立欄位
        df['開盤價'] = None
        df['最高價'] = None
        df['最低價'] = None
        df['收盤價'] = None
        
        # 建立要找的日期
        dt = filename.replace('投信與境內ETF.csv', '')
        dt = datetime.strptime(dt,'%Y%m%d')
        dt = dt.strftime('%Y-%m-%d')
        # 建立證券代號list
        numls = list(df['證券代號'].values)
        
        for i in range(0, len(numls)):
            tempname = str(numls[i]).replace('="','')
            tempname = tempname.replace('"','')
            filename2 = tempname + '.csv'
            file_path2 = os.path.join('D:/DataSets/股價日成交資訊/',\
                            filename2)
        
        
            try:
                numdf = pd.read_csv(file_path2, encoding = 'cp950')
            #print(numdf.head())
                a = float(numdf.loc[numdf['date'] == dt,'開盤價'])
                b = float(numdf.loc[numdf['date'] == dt,'最高價'])
                c = float(numdf.loc[numdf['date'] == dt,'最低價'])
                d = float(numdf.loc[numdf['date'] == dt,'收盤價'])
                df.iloc[i, 6] = a
                df.iloc[i, 7] = b
                df.iloc[i, 8] = c
                df.iloc[i, 9] = d
            except:
                testls.append(filename2)
                continue
            #print(df.iloc[i, 6] )
        
        # 設定要輸出的csv
        output_name = filename.replace('投信與境內ETF.csv', '')
        df.to_csv(f'D:/DataSets/投信/2017/{output_name}投信.csv', encoding = 'cp950', index = False)
        
print(testls)