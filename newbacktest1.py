import datetime

import matplotlib.pyplot as plt
from pandas import DataFrame
from sqlalchemy import and_

from backtest_strategies.BOLL import BOLLStrategy
from backtest_strategies.CCI import CCIStrategy
from backtest_strategies.MACD import MACDStrategy
from backtest_strategies.RSI import RSIStrategy
from backtest_strategies.MixStrategy import MixStrategy
from easyquant.quotation import use_quotation

from dbconnect import OdsRsiTradeData, DataFetcher

id = 0


def insert_database(session,
                    stock_code,
                    lower_rsi,
                    upper_rsi,
                    rsi_date_diff_short,
                    rsi_date_diff_middle,
                    rsi_date_diff_long,
                    signal_count,
                    base_rate,
                    strategy_rate):
    odsrsitradedata_data = OdsRsiTradeData(
        id=None,
        stock_code=stock_code,
        lower_rsi=lower_rsi,
        upper_rsi=upper_rsi,
        rsi_date_diff_short=rsi_date_diff_short,
        rsi_date_diff_middle=rsi_date_diff_middle,
        rsi_date_diff_long=rsi_date_diff_long,
        signal_count=signal_count,
        base_rate=base_rate,
        strategy_rate=strategy_rate
    )
    session.add(odsrsitradedata_data)
    # session.commit()


print('backtest 回测 测试 ')

# quotation = use_quotation('jqdata')
# quotation = use_quotation('')
quotation = use_quotation('local')

trade_days = quotation.get_all_trade_days()

#
end_date = datetime.datetime.today().strftime('%Y-%m-%d')

stock_code = "sh000001"
size = 500

bars = quotation.get_bars(stock_code, size, end_dt=end_date)

# strategy = CCIStrategy(stock_code, bars, days=size)
# strategy = BOLLStrategy(stock_code, bars, days=size)

# strategy = MACDStrategy(stock_code, bars, days=size)

# strategy = RSIStrategy(stock_code, bars, days=size)

# for i in range(10, 50):
#     for j in range(50, 90):
#         for s in range(1, 10):
#             for l in range(s, 30):
#                 for m in range(s, l):
#                     strategy = MixStrategy(stock_code, bars, days=size, lower_rsi=i, upper_rsi=j, rsi_date_diff_long=l,
#                                            rsi_date_diff_middle=m, rsi_date_diff_short=s)
#                     # strategy = MixStrategy(stock_code, bars, days=size, lower_rsi=40, upper_rsi=50, rsi_date_diff_long=15,
#                     #                        rsi_date_diff_middle=10, rsi_date_diff_short=5)
#                     strategy.process()
#                     # strategy.show_plt()
#                     result = strategy.output_earning_rate()
#                     strategy_rate = result["strategy"].values[-1]
#                     base_rate = result["base"].values[-1]
#                     consecutive_series = (result['signals'] == 1) & (result['signals'].shift() == 0)
#                     signal_count = sum(consecutive_series)
#                     print(signal_count, base_rate, strategy_rate)
#                     print("id:", id)
#
#                     insert_database(id, stock_code,
#                                     i,
#                                     j,
#                                     s,
#                                     m,
#                                     l,
#                                     signal_count,
#                                     base_rate,
#                                     strategy_rate)
#                     id = id + 1
import threading
import time


# 定义任务函数
def task(session, begin, end):
    strategy = MixStrategy(stock_code, bars, days=size, lower_rsi=0, upper_rsi=0,
                           rsi_date_diff_long=0,
                           rsi_date_diff_middle=0, rsi_date_diff_short=0)
    for s in range(2, 10):
        strategy.rsi_date_diff_short = s
        short = strategy.get_scores_for_other_date_diff(s, df=bars)
        strategy.bars['short'] = short
        for l in range(s, 30):
            strategy.rsi_date_diff_long = l
            long = strategy.get_scores_for_other_date_diff(l, df=bars)
            strategy.bars['long'] = long
            for m in range(s, l):
                strategy.rsi_date_diff_middle = m
                middle = strategy.get_scores_for_other_date_diff(m, df=bars)
                strategy.bars['middle'] = middle

                for i in range(begin, end):
                    for j in range(end, 90):
                        strategy.signals = []
                        position = 0
                        for d in range(size):
                            try:
                                rsi_middle = middle[d - 1]
                                rsi_long = long[d - 1]
                                rsi_short = short[d - 1]

                                # 判断是否处于多头市场
                                if rsi_long <= rsi_short:
                                    if_uper_singal = 1
                                else:
                                    if_uper_singal = 0
                                # 多头市场且短期rsi低估,黄金交叉
                                if rsi_middle < i and if_uper_singal:
                                    singal = 1
                                # 空头市场且短期rsi高估，死亡交叉
                                elif rsi_middle > j and if_uper_singal == 0:
                                    singal = 0
                                else:
                                    singal = -1
                                if singal == -1:
                                    # 保持不变
                                    strategy.signals.append(position)
                                else:
                                    position = singal
                                    strategy.signals.append(singal)
                            except:
                                strategy.signals.append(position)

                        result = strategy.output_earning_rate()
                        strategy_rate = result["strategy"].values[-1]
                        base_rate = result["base"].values[-1]
                        consecutive_series = (result['signals'] == 1) & (result['signals'].shift() == 0)
                        signal_count = sum(consecutive_series)

                        insert_database(session,
                                        stock_code,
                                        i,
                                        j,
                                        s,
                                        m,
                                        l,
                                        signal_count,
                                        base_rate,
                                        strategy_rate)
                print(stock_code,
                      l,
                      m,
                      s)
                session.commit()


# 创建多个线程


threads = []
for i in range(5, 50, 4):  # 创建3个线程
    print(i)
    datafetcher = DataFetcher()
    session = datafetcher.session
    thread = threading.Thread(target=task, args=(session, i, i + 4))
    threads.append(thread)
    thread.start()

# 等待所有线程结束
for thread in threads:
    thread.join()

print("All tasks are finished.")
