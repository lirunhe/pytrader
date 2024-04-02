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

strategy = MixStrategy(stock_code, bars, days=size, lower_rsi=41, upper_rsi=44, rsi_date_diff_long=21,rsi_date_diff_middle=6, rsi_date_diff_short=2)


strategy.process()
strategy.show_plt()