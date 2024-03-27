import datetime

import matplotlib.pyplot as plt

from backtest_strategies.BOLL import BOLLStrategy
from backtest_strategies.CCI import CCIStrategy
from backtest_strategies.MACD import MACDStrategy
from backtest_strategies.RSI import RSIStrategy
from backtest_strategies.MixStrategy import MixStrategy
from easyquant.quotation import use_quotation

print('backtest 回测 测试 ')

# quotation = use_quotation('jqdata')
# quotation = use_quotation('')
quotation = use_quotation('local')


trade_days = quotation.get_all_trade_days()

#
end_date = datetime.datetime.today().strftime('%Y-%m-%d')

stock_code="sh000001"
size = 500

bars = quotation.get_bars(stock_code, size,
                          end_dt=end_date)

# strategy = CCIStrategy(stock_code, bars, days=size)
# strategy = BOLLStrategy(stock_code, bars, days=size)

# strategy = MACDStrategy(stock_code, bars, days=size)

# strategy = RSIStrategy(stock_code, bars, days=size)
strategy = MixStrategy(stock_code, bars, days=size, lower_rsi=40, upper_rsi=50, rsi_date_diff_long=15,
                 rsi_date_diff_middle=10, rsi_date_diff_short=5)
strategy.process()
strategy.show_plt()