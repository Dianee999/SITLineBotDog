import requests, traceback, logging, boto3, json, sys, os, datetime
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from typing import List, Dict, Union
from traceback import print_exc
import pandas as pd

# 指定API跟金鑰
url = "https://api.finmindtrade.com/api/v4/data"
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wNC0wNiAxMzowMjowMiIsInVzZXJfaWQiOiJyeXVraWJhMDAiLCJpcCI6IjIxMS4yMC4xNDkuMTUyIn0.0HZkVN99oI-QjrEskL-PppwovfCLkHAaVDr6VQKRSSw"

# 指定日期
today = datetime.date(2021, 1, 11)
today_str = today.strftime('%Y-%m-%d')

# 追蹤現有股票設定
current_date = pd.to_datetime(today)
start_date = pd.to_datetime(today)
end_date = pd.to_datetime(current_date + pd.Timedelta(days=1))
ten_date = pd.to_datetime(current_date - pd.Timedelta(days=9))
quarterly_date = pd.to_datetime(current_date - pd.Timedelta(days=100))
tracking_interval = datetime.timedelta(days=1)  #每次追蹤股票的間隔(每天)

# 交易設定
daily_trade_limit = 100000  #每日交易上限
cash = 1000000  #剛開始總資產=cash=100萬
total_value = cash
daily_total_value = [total_value]  #紀錄初始資產

# 定義資料結構
now_stocks: List[Dict[str, Union[str, int, float]]] = [{
    'stock_id': 'cash',
    'last_date': current_date,
    'shares': cash,
    'close': 1
}]
all_transactions: List[Dict[str, Union[str, int, float, bool]]] = []
buy_list: List[Dict[str, Union[str]]] = []
sell_list: List[Dict[str, Union[str]]] = []
test_data={"stock_id": "2322",
    'date': current_date,
    'shares': 900,
    'close': 1}

# 計算10天報酬率
def ROI10(df):
    print('開始ROI10')
    try:
        last_close = df.iloc[-1]['close']
        ten_days_ago_close = df.iloc[9]['close']
        df['ROI10'] = (last_close - ten_days_ago_close) / ten_days_ago_close
        return df
    except:
        print_exc()

# 計算60天移動平均線的函數
def calculate_ma60(df):
    print('開始calculate_ma60')
    try:
        if 'close' not in df.columns:
            return df
        df['MA60'] = df['close'].rolling(window=60, min_periods=1).mean()
        return df
    except:
        print_exc()

# 爬取當日開盤價
def get_open_price(stock_id, current_date):
    print('開始get_open_price')
    try:
        parameter = {
            "dataset": "TaiwanStockPrice",
            "start_date": current_date.strftime("%Y-%m-%d"),
            "end_date": current_date.strftime("%Y-%m-%d"),
            "data_id": str(stock_id),
            "token": token,
        }
        res = requests.get(url, params=parameter)
        data = res.json()
        print(f"Processing {stock_id}")
        #print(data)
        if 'error' in data:
            print(f"Error: {data['error']}")
            return None
        elif not data['data']:
            print(f"No data available for {stock_id} on {current_date}")
            return None
        else:
            open_price = float(data['data'][0]['open'])
            return open_price
    except:
        print_exc()
        return None
# 爬取當日收盤價
def get_close_price(stock_id, current_date):
    print('開始get_close_price')
    try:
        parameter = {
            "dataset": "TaiwanStockPrice",
            "start_date": current_date.strftime("%Y-%m-%d"),
            "end_date": current_date.strftime("%Y-%m-%d"),
            "data_id": str(stock_id),
            "token": token,
        }
        res = requests.get(url, params=parameter)
        data = res.json()
        print(f"Processing {stock_id}")
        #print(data)
        if 'error' in data:
            print(f"Error: {data['error']}")
            return None
        elif not data['data']:
            print(f"No data available for {stock_id} on {current_date}")
            return None
        else:
            close_price = float(data['data'][0]['close'])
            return close_price
    except:
        print_exc()
        return None

#買賣超資料處理及篩選
def select_fd_stc():
    print('開始select_fd_stc')
    try:
        # 取得投信買賣超資料(日)
        df_fd_d = pd.read_csv(r'D:/Datasets/2023.03.18/主動投信/2021/20210111主動投信.csv', encoding='ANSI')
        df_fd_d['買賣超股數'] = df_fd_d['買賣超股數'].str.replace(',', '').astype(float)
        df_fd_d = df_fd_d[df_fd_d['買賣超股數'] >= 200]
        stocks = pd.DataFrame({'證券代號': df_fd_d['證券代號'].unique()}).sort_values(by='證券代號')

        # 取得投信買賣超資料(年)
        df_fd_y = pd.read_csv(r'D:/Datasets/合併後/2021主動投信(時間排序).csv', encoding='ANSI', parse_dates=['Date'])
        df_fd_y = df_fd_y[df_fd_y['Overbought or oversold'] >= 0]
        df_fd_y = df_fd_y.loc[(df_fd_y['Date'] >= ten_date) & (df_fd_y['Date'] <= start_date)]
        df_stocks_fd_y = df_fd_y[df_fd_y['stock_id'].astype(str).isin(stocks['證券代號'])]

        # 統計每個股票在買賣超名單中出現的次數
        counts_stocks_fd = df_stocks_fd_y['stock_id'].value_counts().reset_index()
        counts_stocks_fd.columns = ['stock_id', 'count']
        counts_stocks_fd = counts_stocks_fd[counts_stocks_fd['count'] >= 2]  # 加入次數大於一的條件篩選
        counts_stocks_fd.sort_values('count', ascending=False, inplace=True)
        stock_ids = counts_stocks_fd['stock_id'].tolist()
        return stock_ids
    except:
        print_exc()
stock_ids = select_fd_stc()
print("買賣超資料處理及篩選:", stock_ids)


# In[83]:


# 選擇隔日買進的股票  ##只用60MA當濾網 (##要用60MA跟ROI當濾網的話把#的啟動)
def select_buy_stc():
    print("開始select_buy_stc")
    stock_ids = select_fd_stc()
    if not stock_ids:
        print("沒有符合選股條件的股票")
        return []
    try:
        # 創建空的DataFrame來存儲所有股票的數據
        all_buy_data = pd.DataFrame()
        # 爬取所有股票的數據
        for stock_id in stock_ids:
            print(f"Processing {stock_id}")
            parameter = {
                "dataset": "TaiwanStockPrice",
                "start_date": quarterly_date.strftime("%Y-%m-%d"),
                "end_date": current_date.strftime("%Y-%m-%d"),
                "data_id": stock_id,
                "token": token,
                }
            res = requests.get(url, params=parameter)
            data = res.json()
            df = pd.DataFrame(data["data"])
            # 計算移動平均線60MA及報酬率ROI
            df = calculate_ma60(df)
            df = ROI10(df)
            #print(df)
            #print(df.columns)
            #print(type(df))
            all_buy_data = pd.concat([all_buy_data, df], ignore_index=True)
            #print(all_buy_data)
            #print(type(all_buy_data))

        # 根據條件篩選符合的股票
        select = (all_buy_data['date'] == current_date.strftime('%Y-%m-%d')) & (all_buy_data['close'] >= all_buy_data['MA60']) #& (all_buy_data['ROI10'] < 0.05)
        selected_data = all_buy_data.loc[select, ['date', 'stock_id', 'open', 'close', 'MA60']]
        selected_data = selected_data.rename(columns={"stock_id": "stock_ids"})
       
        if len(selected_data) == 0:
            print("沒有符合選股條件的股票")
            return []
        buy_list = selected_data["stock_ids"].to_list()
        print("隔日需買進的股票:", buy_list)
        return buy_list
    except:
        print_exc()
#buy_list = select_buy_stc()
#print("選擇隔日買進的股票:", buy_list)


def select_sell_stc():
    sell_list = []
    print('開始select_sell_stc')
    try:
        if len(now_stocks) == 1:
            print("沒有符合賣出條件的股票")
            return []

        # 創建空的DataFrame來存儲所有股票的數據
        all_sell_data = pd.DataFrame()

        # 先把已持有60天且未在buy_list中的股票加入sell_list中
        for stock in now_stocks:
            stock_id = stock['stock_id']
            if stock_id == 'cash':
                continue
            if stock_id in buy_list:
                continue
            bought_date = stock['last_date']
            diff_days = (current_date - bought_date).days
            if diff_days >= 60:
                sell_list.append(stock_id)

        # 爬取所有股票的數據
        for stock in now_stocks:
            stock_id = stock['stock_id']
            if stock_id == 'cash':
                continue
            if stock_id in sell_list:
                continue  # 如果已經在sell_list中就不用再爬取數據
            parameter = {
                "dataset": "TaiwanStockPrice",
                #"start_date": current_date,
                #"end_date": current_date,
                "start_date":quarterly_date.strftime("%Y-%m-%d"),
                "end_date":current_date.strftime("%Y-%m-%d"),
                "data_id": stock_id,
                "token": token,
                }
            res = requests.get(url, params=parameter)
            data = res.json()
            df = pd.DataFrame(data["data"])
            # 計算60MA
            df = calculate_ma60(df)
            all_sell_data = pd.concat([all_sell_data, df], ignore_index=True)

        # 根據條件篩選符合的股票
        select = (all_sell_data['date'] == current_date.strftime('%Y-%m-%d')) & (all_sell_data['close'] < all_sell_data['MA60']) & (~all_sell_data['stock_id'].isin(buy_list)) & (~all_sell_data['stock_id'].isin(sell_list))
        selected_data = all_sell_data.loc[select, ['date', 'stock_id', 'open', 'close', 'MA60']]
        selected_data = selected_data.rename(columns={"stock_id": "stock_ids"})
        sell_list += selected_data["stock_ids"].to_list()
        
        print("隔日需賣出的股票:", sell_list)
        return sell_list
    except:
        print_exc()
#sell_list = select_sell_stc()
#print("選擇隔日賣出的股票:", sell_list)


# # 開盤

# ### 定義買入與賣出

# In[85]:


def buy_buylist():
    print('开始 buy_buylist')
    print(buy_list)
    funds_per_stock = daily_trade_limit / len(buy_list)
    try:
        for stock_id in buy_list:
            buy_price = get_open_price(stock_id, current_date)
            if buy_price is None:
                continue
            shares = funds_per_stock // buy_price
            if shares > 0:
                new_stock = {
                    "stock_id": stock_id,
                    "last_date": current_date,
                    "shares": shares,
                    "price": buy_price,
                    "earnings": 0,
                }
                stock_index = None
                for i, stock in enumerate(now_stocks):
                    if stock["stock_id"] == stock_id:
                        stock_index = i
                        break
                if stock_index is not None:
                    now_stocks[stock_index]["shares"] += shares
                    now_stocks[stock_index]["last_date"] = current_date
                    
                else:
                    now_stocks.append(new_stock)
                now_stocks[0]["shares"] -= shares * buy_price
                transaction = {
                    "date": current_date,
                    "stock": stock_id,
                    "shares": shares,
                    "price": buy_price,
                    "action": "buy",
                }
                all_transactions.append(transaction)
                print(transaction)
        buy_list.clear()
        print("购买清单已清空：", buy_list)
    except:
        print_exc()


def sell_selllist():
    print('開始sell_selllist')
    try:
        for stock_id in sell_list:
            sell_price = get_open_price(stock_id, current_date)
            for i, stock in enumerate(now_stocks):
                if stock['stock_id'] == stock_id:
                    shares = now_stocks[i]['shares']
                    now_stocks[0]['shares'] += shares * sell_price 
                    transaction = {
                        'date': current_date,
                        'stock': stock_id,
                        'shares': shares,
                        'price': sell_price,
                        'action': 'sell'}
                    all_transactions.append(transaction)
                    print(transaction)
                    del now_stocks[i]
                    break
        # 清空 'sell_list'
        sell_list.clear()
        print('清空sell_list:', sell_list)
    except:
        print_exc()
#sell_selllist()
#print(now_stocks)


# ### # 計算總資產

def count_total_value():
    print('開始count_total_value')
    try:
        total_value = 0
        for stock_info in now_stocks:
            if stock_info['stock_id'] == 'cash':
                total_value += now_stocks[0]['shares']
            else:
                shares = stock_info['shares']
                if shares == 0:  # 如果持有量为零则跳过计算
                    continue
                price = get_close_price(stock_info['stock_id'], current_date)
                #if price is None:
                    #continue
                total_value += shares * price

        # 記錄每日總資產
        daily_total_value.append(total_value)
        print("現在總資產:", total_value )
        return total_value
    except:
        print_exc()
#total_value = count_total_value()
        
# === [ 主程式 - 這裡是主要的 lambda_handler 程式進入點區段，相當於 main() ]==================================================================================== 
def lambda_handler(event, context):
    print('*'*40 + '開始my_strategy'+'*'*40)
    global current_date, cash, all_stock_data, total_value, buy_list, sell_list, daily_trade_limit
    try:
        while current_date <= end_date:
            print('-'*30, current_date, '開盤'+'-'*30)
        #查看目前持有之股票
            print("目前持有:", now_stocks)
        # 開盤   
            # 賣出'sell_list'中的股票
            if sell_list:
                sell_selllist()
            # 買入'buy_list'中的股票
            if buy_list:
                buy_buylist()
            # 計算總資產
            total_value = count_total_value()
            
            # 收盤
            # 獲取明天需要買入的股票清單
            buy_list = select_buy_stc()
            
            # 選擇明天要賣出的股票加入sell_list
            sell_list = select_sell_stc()
            #個人資料查詢
            # 獲取所有買賣操作的紀錄
            print(all_transactions)
            
            for item in all_transactions:
                if item['action'] == 'buy':
                    print(f"{today}\n推薦明天購買的證券ID是{item['stock']}\n")
                elif item['action'] == 'sell':
                    print(f"{today}\n推薦明天賣出的證券ID是{item['stock']}\n")
            # 更新日期
            current_date = pd.to_datetime(str(current_date)) + pd.Timedelta(days=tracking_interval.days)
        pass
    except:
        print_exc()