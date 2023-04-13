# 匯入套件
# =========================================================
import boto3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
# =========================================================

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    # TODO implement
   # =========================================================
       # 0. 前處理 ===============================================
    # =========================================================
    # 確定今天日期 =============================================
    today = datetime.now().strftime('%Y%m%d')  #西元年(yyyymmdd)
    # 建立1個證券代號空list，用來今日個股 (先等今天總彙表抓完才能確定)
    numls = []
    
    # 1. 爬取 投信買賣超彙總表 ==================================
    # =========================================================
    # 資料來源網址 -> http://www.twse.com.tw/fund/TWT44U?response=csv&date={today}&selectType=ALLBUT0999
    # 擷取網址資料
    TWSE_URL = 'https://www.twse.com.tw/rwd/zh/fund/TWT44U?date={today}&response=csv&type=ALLBUT0999'
    response = requests.get(TWSE_URL)
    #print(response.text)
    #aa：創建一個字典，內容為用 ' ' 包住資料
    aa = {ord(c): None for c in ' '}
    #bb：將導進來的資料用 '\n' 元素換行
    bb = response.text.split('\n')
    #cc：在每一筆資料中，保留 ' ", '分隔的項目個數等於7，及第一個值不等於'='，並利用translate呈現aa裡剩餘的資料
    cc = [i.translate(aa)
        for i in bb
        if len(i.split('",')) == 7 and i[0] != '=']
    #dd：將每一筆資料用換行符號"\n"連在一起形成一個字串
    dd = "\n".join(cc)
    #ee：將字串放入內存StringIO裡，StringIO只讀字串型別的資料
    ee = StringIO(dd)
    # 將字串轉為df
    df = pd.read_csv(ee, header=0)
    # 刪除不必要的欄位
    df = df.drop(['Unnamed: 0', 'Unnamed: 6'], axis = 1)
    
    # =========================================================
    
    
    # 2. 處理 投信買賣超彙總表的格式 ===========================
    # =========================================================
    # 讀取台股總覽檔案
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockInfo",
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMy0xNSAxNzo1NzowNyIsInVzZXJfaWQiOiJyeXVraWJhMDAiLCJpcCI6IjU5LjEyNi4yLjEzMiJ9.gAi43-6wK7Gt23gvMP1Y0jt3TWPLKDb1orAQeEe_9u8", # 參考登入，獲取金鑰
    }
    resp = requests.get(url, params=parameter)
    data = resp.json()
    stockdf = pd.DataFrame(data["data"])
    # 台灣境內ETF的證券代號
    ETFlist = []
    
    templs = []
    # 找出所有符合ETF的行列
    for i in range(3001): # 3001是資料總筆數
        templs = stockdf.index[stockdf["industry_category"] == 'ETF'].to_list()
    #print(templs)
    
    for i in range(len(templs)):
        ETFlist.append(stockdf.iloc[templs[i],1])    
    #print(ETFlist)
    
    for i in range(len(ETFlist)):
        ETFlist[i] = '="'+ETFlist[i]+'"'
    
    # 增加需要的欄位
    df['開盤價'] = None
    df['最高價'] = None
    df['最低價'] = None
    df['收盤價'] = None
    df['台灣境內ETF(Y/N)'] = None
    
    # 找尋今天的每日股價
    # 先確定今天所有的證券代號 =================================
    numls = list(df['證券代號'])
    # 處理格式問題
    for i in range(len(numls)):
        if '=' in numls[i]:
            numls[i] = numls[i].replace('="', '')
            numls[i] = numls[i].replace('"', '')
    
    # 將今天變為指定格式
    today = datetime.now().strftime('%Y-%m-%d')  #西元年(yyyy-mm-dd)
    
    # 使用 FinMind api ========================================
    url = "https://api.finmindtrade.com/api/v4/data"
    
    for i in range(0, len(numls)):
        parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": numls[i],
        "start_date": today,
        "end_date": today,
        "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMy0xNSAxNzo1NzowNyIsInVzZXJfaWQiOiJyeXVraWJhMDAiLCJpcCI6IjU5LjEyNi4yLjEzMiJ9.gAi43-6wK7Gt23gvMP1Y0jt3TWPLKDb1orAQeEe_9u8", # 參考登入，獲取金鑰
        }
        
        resp = requests.get(url, params=parameter)
        data = resp.json()
        data = pd.DataFrame(data["data"])
        
        df.iloc[i, 5] = data.iloc[0, 4]
        df.iloc[i, 6] = data.iloc[0, 5]
        df.iloc[i, 7] = data.iloc[0, 6]
        df.iloc[i, 8] = data.iloc[0, 7]
        
    
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
    today = datetime.now().strftime('%Y%m%d')  #西元年(yyyymmdd)
    df.to_csv(f'/tmp/{today}投信.csv',\
              encoding = 'utf-8', index = False)
        
    # =========================================================
    
    # 3. 過濾 主動投信 ===========================
    # =========================================================
    # 找出所有符合台灣境內ETF的行列
    templs = df.index[df["台灣境內ETF(Y/N)"] == "Y"].tolist()
    
    # 利用For迴圈，刪除特定的行
    for i in range(len(templs)):
        df.drop( templs[i], inplace=True)
    # 刪除整個欄位
    df.drop(['台灣境內ETF(Y/N)'], axis = 1, inplace = True)
    
    # 設定要輸出的csv
    df.to_csv(f'/tmp/{today}主動投信.csv',\
              encoding = 'utf-8', index = False)
    
    s3.Bucket('lab-385515').upload_file(f'/tmp/{today}投信.csv', f'{today}投信.csv')
    s3.Bucket('lab-385515').upload_file(f'/tmp/{today}主動投信.csv', f'{today}主動投信.csv')
    
    return None