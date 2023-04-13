# 匯入套件
# =========================================================
import os
import pandas as pd
# =========================================================

# 前處理
# =========================================================
# 指定讀取資料夾路徑
folder_path = 'D:/Datasets/投信/2023'

# =========================================================

# 處理
# =========================================================
# 讀取資料夾內所有CSV檔案
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        # 讀取CSV檔案
        df = pd.read_csv(file_path,dtype={'證券代號': str},encoding = 'cp950')
        # 在這裡對df進行你需要的操作
        
        # 更動欄位所有名稱
        df = df.rename(columns = {
                             '證券代號':'stock_id',\
                             '證券名稱':'stock_name',\
                             '買進股數':'Bought_volume',\
                             '賣出股數':'Sold_volume',\
                             '買賣超股數':'Overbought or oversold',\
                             '開盤價':'Open',\
                             '最高價':'High',\
                             '最低價':'Low',\
                             '收盤價':'Close',\
                            '台灣境內ETF(Y/N)':'IsETF(Y/N)'})
        
        # 更改所有欄位的格式
        df['stock_id'] = df['stock_id'].astype(str)
        df['stock_name'] = df['stock_name'].astype(str)
        for i in range(len(df)):
            df.iloc[i,2] = df.iloc[i,2].replace(',', '')
            df.iloc[i,3] = df.iloc[i,3].replace(',', '')
            df.iloc[i,4] = df.iloc[i,4].replace(',', '')
        df['Bought_volume'] = df['Bought_volume'].astype(int)
        df['Sold_volume'] = df['Sold_volume'].astype(int)
        df['Overbought or oversold'] = df['Overbought or oversold'].astype(int)
        
        
        # 刪除市值超過100百億之Record
        # 先做一個空的list 
        numls_100 = []
        folder_path2 = 'D:/Datasets/市值報表前600名/2022'
        for filename2 in os.listdir(folder_path2):
            if filename2.endswith('.csv'):
                file_path2 = os.path.join(folder_path2, filename2)
                # 讀取CSV檔案
                df2 = pd.read_csv(file_path2, encoding = 'utf-8')
                for i in range(len(df2)):
                    df2.iloc[i,5] = str(df2.iloc[i,5]).replace(',', '')
                    df2.iloc[i,5] = float(df2.iloc[i,5])
                    if df2.iloc[i,5] > 100:
                        numls_100.append(df2.iloc[i,1])
                
        # 處理list的格式
        for i in range(len(numls_100)):
            if '=' in numls_100[i]:
                numls_100[i] = numls_100[i].replace('="','')
                numls_100[i] = numls_100[i].replace('"','')
                
        # 刪除符合特定的列
        templs = []
        # 找出所有符合的行列
        for i in range(len(df)):
            if df.iloc[i,0] in numls_100:
                templs.append(i)
        
        # 利用For迴圈，刪除特定的行
        for i in range(len(templs)):
            df.drop( templs[i], inplace=True)
        
        # 刪除買賣超低於200的紀錄
        #df.drop(df[df['Overbought or oversold'] < 200].index, inplace=True)
        
        # 設定要輸出的csv
        output_name = filename.replace('投信.csv', '')
        df.to_csv(f'D:/Datasets/投信/2023/{output_name}投信.csv',\
                  encoding = 'ANSI', index = False)
            
        # 找出所有符合台灣境內ETF的行列
        templs = df.index[df["IsETF(Y/N)"] == "Y"].tolist()
        
        # 利用For迴圈，刪除特定的行
        for i in range(len(templs)):
            df.drop( templs[i], inplace=True)
        # 刪除整個欄位
        df.drop(['IsETF(Y/N)'], axis = 1, inplace = True)
        
        # 設定要輸出的csv
        output_name = filename.replace('投信.csv', '')
        df.to_csv(f'D:/Datasets/主動投信/2023/{output_name}主動投信.csv',\
                  encoding = 'ANSI', index = False)
        
# =========================================================