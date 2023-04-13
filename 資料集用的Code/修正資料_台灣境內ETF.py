# 匯入套件
# =========================================================
import os
import pandas as pd
# =========================================================

# 前處理
# =========================================================
# 指定讀取資料夾路徑
folder_path = 'D:/Datasets/投信/2022投信'

# 讀取台股總覽檔案
stockdf = pd.read_csv('D:/Datasets/台股總覽.csv', encoding = 'cp950')
# 台灣境內ETF的證券代號
ETFlist = []

templs = []
# 找出所有符合ETF的行列
for i in range(3001): # 3001是資料總筆數
    templs = stockdf.index[stockdf["產業別"] == 'ETF'].to_list()
#print(templs)

for i in range(len(templs)):
    ETFlist.append(stockdf.iloc[templs[i],1])    
#print(ETFlist)

for i in range(len(ETFlist)):
    ETFlist[i] = '="'+ETFlist[i]+'"'

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
        
        # 先刪除整個欄位
        df.drop(['是否為台灣境內ETF'], axis = 1, inplace = True)
        # 再重新增加'是否為台灣境內ETF'欄位
        df['台灣境內ETF(Y/N)'] = None
        
        # 空list，等下要存符合行列的索引值
        templs = []
        # 找出所有符合台灣境內ETF的行列
        for i in range(len(ETFlist)):
            templs.append(df.index[df["證券代號"] == ETFlist[i]])
        
        for j in range(len(templs)):
            df.loc[templs[j],'台灣境內ETF(Y/N)'] = 'Y'
        # 將None值轉為空白(雖然顯示還是為NaN)
        df.fillna('N', inplace = True)
        
        # 設定要輸出的csv
        output_name = filename.replace('投信.csv', '')
        df.to_csv(f'D:/DataSets/投信/修正後/2022/{output_name}投信.csv',\
                  encoding = 'cp950', index = False)
        
# =========================================================