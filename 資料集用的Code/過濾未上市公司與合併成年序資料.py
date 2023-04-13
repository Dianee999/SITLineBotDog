import pandas as pd
import os
import datetime

# 前處理
# =========================================================
# 指定讀取資料夾路徑
folder_path = 'D:/Datasets/投信/2023'

stock_id = []
# 讀取資料夾內所有CSV檔案
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        # 讀取CSV檔案
        df = pd.read_csv(file_path,dtype={'stock_id': str},encoding = 'ANSI')
        # 在這裡對df進行你需要的操作
        
        for i in  range(len(df)):
            if df.iloc[i,0] not in stock_id:
                stock_id.append(df.iloc[i,0])
# 處理掉為ETF的代號
for i in stock_id:
    if i[:2] == '00':
        stock_id.remove(i)

notstock = []
templs = []
# 指定讀取資料夾路徑
folder_path2 = 'D:/Datasets/上市和上櫃名單'
# 讀取資料夾內所有CSV檔案
for filename2 in os.listdir(folder_path2):
    if filename2.endswith('.csv'):
        file_path2 = os.path.join(folder_path2, filename2)
        # 讀取CSV檔案
        df2 = pd.read_csv(file_path2, encoding = 'utf-8')
        # 在這裡對df進行你需要的操作
        
        for i in range(len(df2)):
            templs.append(str(df2.iloc[i,1]))
            
# 找出未上市的公司
for i in stock_id:
    if i not in templs:
        notstock.append(i)
        
# 指定讀取資料夾路徑
folder_path = 'D:/Datasets/投信/2023'

# 前至一個空的df，等下輸出
year_df = pd.DataFrame(columns=['Date', 'stock_id', 'stock_name', 'Bought_volume', \
                                'Sold_volume','Overbought or oversold','Open',\
                                'High','Low','Close','IsETF(Y/N)'])
# 讀取資料夾內所有CSV檔案
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        # 讀取CSV檔案
        df = pd.read_csv(file_path,dtype={'stock_id': str},encoding = 'ANSI')
        
        date_str = filename.replace('投信.csv','')
        date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
        formatted_date_str = date_obj.strftime("%Y/%m/%d")
        
        year_df = year_df.merge(df, on=['stock_id', 'stock_name', 'Bought_volume', \
                                        'Sold_volume','Overbought or oversold','Open',\
                                        'High','Low','Close','IsETF(Y/N)'], how='outer')
        # 將None值轉為空白(雖然顯示還是為NaN)
        year_df.fillna(formatted_date_str, inplace = True)
        

templs = []
for i in range(len(notstock)):
    templs = year_df.index[year_df['stock_id'] == notstock[i]].tolist()
    # 利用For迴圈，刪除特定的行
    for j in range(len(templs)):
        year_df.drop(templs[j], inplace=True)
        
df_sortedbyid = year_df.sort_values('stock_id')
df_sortedbyid.to_csv('D:/Datasets/測試/2023投信(代號排序).csv', encoding = 'ANSI', index  = False)

df_sortedbydate = year_df.sort_values('Date')
df_sortedbydate.to_csv('D:/Datasets/測試/2023投信(時間排序).csv', encoding = 'ANSI', index  = False)

 # 找出所有符合台灣境內ETF的行列
templs = year_df.index[year_df["IsETF(Y/N)"] == "Y"].tolist()
 
 # 利用For迴圈，刪除特定的行
for i in range(len(templs)):
    year_df.drop( templs[i], inplace=True)
 # 刪除整個欄位
year_df.drop(['IsETF(Y/N)'], axis = 1, inplace = True)

df_sortedbyid = year_df.sort_values('stock_id')
df_sortedbyid.to_csv('D:/Datasets/測試/2023主動投信(代號排序).csv', encoding = 'ANSI', index  = False)

df_sortedbydate = year_df.sort_values('Date')
df_sortedbydate.to_csv('D:/Datasets/測試/2023主動投信(時間排序).csv', encoding = 'ANSI', index  = False)

        
        
            