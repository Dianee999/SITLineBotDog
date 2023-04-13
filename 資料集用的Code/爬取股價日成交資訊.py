# 匯入套件
# =========================================================
import requests
import time
import pandas as pd
# =========================================================

# 先讀取證券所有代號
# =========================================================
#df = pd.read_csv('D:/所有證券代號.csv')
numls = ['008201']

#print(numls)
# =========================================================

# 爬取資料
# =========================================================
url = "https://api.finmindtrade.com/api/v4/data"

for i in range(0, len(numls)):
    parameter = {
    "dataset": "TaiwanStockPrice",
    "data_id": numls[i],
    "start_date": "2013-01-01",
    "end_date": "2022-12-31",
    "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMy0xNSAxNzo1NzowNyIsInVzZXJfaWQiOiJyeXVraWJhMDAiLCJpcCI6IjU5LjEyNi4yLjEzMiJ9.gAi43-6wK7Gt23gvMP1Y0jt3TWPLKDb1orAQeEe_9u8", # 參考登入，獲取金鑰
    }
    
    resp = requests.get(url, params=parameter)
    data = resp.json()
    data = pd.DataFrame(data["data"])
    
    data.to_csv(f'D:/DataSets/股價日成交資訊/raw/{numls[i]}.csv')
    
    if i == 599:
        time.sleep(3600)
# =========================================================