from pandas import DataFrame
from talib._ta_lib import *

from backtest_strategies.backtest_strategy_template import BacktestStrategyTemplate


class RSIStrategy(BacktestStrategyTemplate):
    def __init__(self, stock_code, bars: DataFrame, days=250, lower_rsi=40, upper_rsi=65, rsi_date_diff_long=21,
                 rsi_date_diff_middle=12, rsi_date_diff_short=6, ):
        super().__init__(stock_code, bars, days)
        # 买入线
        self.lower_rsi = lower_rsi
        # 卖出线
        self.upper_rsi = upper_rsi
        # 以日周期为参数标淮，然后三根线对应时间参数分别为6日、12日、24日，时间越短越灵敏,我这里默认设置为21天
        self.rsi_date_diff_long = rsi_date_diff_long
        self.rsi_date_diff_middle = rsi_date_diff_middle
        self.rsi_date_diff_short = rsi_date_diff_short

    def get_singal(self, df: DataFrame):
        try:
            rsi = self.get_scores(df)[-1]
            if rsi < self.lower_rsi:
                return 1

            if rsi > self.upper_rsi:
                return 0

            return -1
        except:
            return -1

    def get_scores(self, df: DataFrame):
        # 计算长期的rsi指标
        # 此处返回所有日期的rsi指标
        # date
        # 2023-12-04    62.662361
        # 2023-12-05    61.323572
        # 2023-12-06    62.351753
        return RSI(df.close, self.rsi_date_diff_long)

    def get_scores_for_other_date_diff(self, rsi_date_diff, df: DataFrame):
        # 基于不同的周期计算rsi指数
        return RSI(df.close, rsi_date_diff)

    # 判断是否是rsi金叉
    def get_singal_for_other_date_diff(self, df: DataFrame):
        try:
            rsi_middle = self.get_scores_for_other_date_diff(self.rsi_date_diff_middle, df)[-1]
            rsi_long = self.get_scores_for_other_date_diff(self.rsi_date_diff_long, df)[-1]
            rsi_short = self.get_scores_for_other_date_diff(self.rsi_date_diff_short, df)[-1]
            # 判断是否处于多头市场
            if rsi_long <= rsi_short:
                if_uper_singal = 1
            else:
                if_uper_singal = 0
            # 多头市场且短期rsi低估
            if rsi_middle < self.lower_rsi and if_uper_singal:
                return 1
            # 空头市场且短期rsi高估
            if rsi_middle > self.upper_rsi and if_uper_singal == 0:
                return 0

            return -1
        except:
            return -1
