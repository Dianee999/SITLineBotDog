from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from backtrader.feeds import PandasData
from traceback import print_exc
import backtrader as bt
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import warnings
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置画图时的中文显示
plt.rcParams["axes.unicode_minus"] = False  # 设置画图时的负号显示
warnings.simplefilter('ignore', category=FutureWarning)
pd.set_option("display.max_rows", 10)

#建立cerebro
cerebro = bt.Cerebro()

#資料讀取及匯入
#stc_data = pd.read_csv(r'C:\jupyter_base\finance_\Fund_project\MutualFund_2022_stcs.csv', parse_dates=False)#股票資料
fund_data = pd.read_csv(r'C:\jupyter_base\finance_\Fund_project\MutualFund_2022.csv', index_col=0, parse_dates=True)#買賣超資料
price_data = pd.read_csv(r'C:\jupyter_base\finance_\Fund_project\MutualFund_2022_stc_prc.csv', index_col=0, parse_dates=True)#所有股票交易價格

#資料格式整理
#fund_data
fund_data = fund_data.reset_index()
fund_data = fund_data.drop(0)
fund_data = fund_data.rename(columns={"index":"Date"})
fund_data.drop(['stock_name'], axis=1, inplace=True)
fund_data = fund_data.rename(columns={"Date":"datetime"})
fund_data = fund_data.rename(columns={'stock_id': 'ticker'})
#price_data
price_data = price_data.reset_index()
price_data = price_data.drop(0)
price_data = price_data.rename(columns={"index":"stock_id"})
price_data = price_data.rename(columns={"Date":"datetime"})
price_data = price_data.rename(columns={'stock_id': 'ticker'})
#print(stc_data)#240個股票代號
#print(fund_data)#5019筆買賣超紀錄
print(price_data)#38452筆歷史交易價格






class SMAStrategy(bt.Strategy):
    params = dict(
        buy_sell_threshold=200,  # 购买/卖出阈值
    )
    def __init__(self):
        # 定义待交易的股票列表
        self.stocks_to_trade = []
        # 遍历 data0 中的所有股票
        for data in self.datas[0]:
            # 如果买卖超额大于等于阈值，则将该股票添加到待交易列表中
            if data.lines.net_buy[0] >= self.p.buy_sell_threshold:
                self.stocks_to_trade.append(data._name)

    def next(self):
        # 遍历待交易列表中的股票，进行交易
        for stock in self.stocks_to_trade:
            data = self.getdatabyname(stock)

            # 如果当前持仓为0，则买入该股票
            if not self.getposition(data).size:
                self.buy(data)

                # 记录买入日期
                self.buy_date[stock] = self.datas[0].datetime.date(0)

            # 如果当前持仓不为0，则检查是否需要卖出
            else:
                # 计算持仓时间
                hold_days = (self.datas[0].datetime.date(0) - self.buy_date[stock]).days

                # 如果持仓时间超过 5 天，则卖出该股票
                if hold_days >= 5:
                    self.sell(data)

                    # 清除买入日期记录
                    del self.buy_date[stock]

    def __init__(self):
        self.dataclose = self.dataclose[1] > self.sma[1]
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.sma = bt.indicators.MovingAverageSimple(self.data1, period=20)
    def __next__(self):
        if not self.position:
            if self.dataclose[0] >= self.sma[0]*1.02:
                self.buy()
        else:
            if self.dataclose[0] >= self.ama[0]*1.09  :
                    self.close()
            elif self.dataclose[0] <= self.buy_price * 0.9:
                self.close()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Complted]:
            self.log(
                'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f'%
                (order.executed.price,
                 order.executed.value,
                 order.executed.comm))
            self.buyprice = order.executed.price
            self.buycomm = order.executed.comm
        else:
            self.log('BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f'%
                    (order.executed.price,
                    order.executed.value,
                    order.executed.comm))
        self.bar_executed = len(self)
    elif order.status in [order.Canceled, order.Margin, order.Rejected]:
        self.log('Order Canceled/Margin/Rejected')
    self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2F, NET %.2F'%
                 (trade.pnl, trade.pnlcomn))
    def log(self, txt, dt=None, doprint=True):
        if doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

if __name__ =='__main__'
    # 將日期欄位轉換成 datetime 格式
    price_data['datetime'] = pd.to_datetime(price_data['datetime'], format='%Y-%m-%d')
    fund_data['datetime'] = pd.to_datetime(fund_data['datetime'], format='%Y-%m-%d')
    # 將每個 dataframe 轉換成 backtrader 的資料格式
    fund_data = bt.feeds.PandasData(dataname=fund_data.set_index(['datetime', 'ticker']))
    price_data = bt.feeds.PandasData(dataname=price_data.set_index(['datetime', 'ticker']))
    cerebro.adddata(fund_data, name='fund_data')
    cerebro.adddata(price_data, name='price_data')
    cerebro.addstrategy(SMAStrategy)