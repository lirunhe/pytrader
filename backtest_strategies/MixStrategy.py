"""
基本思路
针对个股：
    除考虑正常的量价指标、换手率指标外，还需要额外考虑大环境的变化
        美元指数
        汇率
        外资流入流出
        大盘成交量成交额
        行业板块成交量成交额
        概念板块成交量成交额
    其中，针对美元指数、汇率、外资流入流出主要看境外资本对于境内资产价格的态度，相应的，这些数据也会有对应的趋势存在，会对特定股价有一定的影响因子
        但是，该部分数据面临的问题是历史数据获取的问题拿不到太多历史数据，以及不知道哪些数据是应该纳入数据资产的
    针对大盘成交量成交额等，其实在一定意义上会更加收到外部影响的，即外部因素-》大盘-》个股的传导链条，但是不应该忽视外部的因素在个股的影响
    回到大盘成交量成交价，一定程度上反应整体的市场情绪，更多的是内资的直接体现
    针对行业板块、概念板块，事实上只需要看该股票收入占比最大、N日涨跌趋势最接近的，从而确定该股票与所处的具体的市场炒作情绪的关联，更好的评估是否具备操作价值
    另外一块，就是对于外部影响的或者说是基本面的因素，譬如涉及制裁、涉及荣誉、涉及突破等，这些难以直接从过往数据找到参考系，往往会形成一个突变，需要设置相关的权重模型
    最后，针对基本的运营情况，市盈率、市销率、实现率等等来自于公司内生发展动力的因素，也要加入进来，这个可以参考“净利润断层”这个概念去调参

"""
from pandas import DataFrame

from backtest_strategies.BOLL import BOLLStrategy
from backtest_strategies.CCI import CCIStrategy
from backtest_strategies.RSI import RSIStrategy
import matplotlib.pyplot as plt
from pandas import DataFrame
import matplotlib

matplotlib.use('TkAgg')


class MixStrategy(BOLLStrategy, RSIStrategy):
    # 后续调参可以统一通过这里来实现，跑若干轮？取成功率最高的策略
    # TODO 计划将大盘、汇率等指标也纳入考量，但是数据拿不到
    def __init__(self, stock_code, bars: DataFrame, days=250, lower_rsi=40, upper_rsi=65, rsi_date_diff_long=21,
                 rsi_date_diff_middle=12, rsi_date_diff_short=6):

        super().__init__(stock_code, bars, days)
        self.rsistrategy = RSIStrategy(stock_code=stock_code,
                                       bars=bars,
                                       days=days,
                                       lower_rsi=lower_rsi,
                                       upper_rsi=upper_rsi,
                                       rsi_date_diff_long=rsi_date_diff_long,
                                       rsi_date_diff_middle=rsi_date_diff_middle,
                                       rsi_date_diff_short=rsi_date_diff_short)
        self.bollstrategy = BOLLStrategy(stock_code=stock_code,
                                         bars=bars,
                                         days=days,
                                         timeperiod=14,
                                         nbdevup=2,
                                         nbdevdn=2,
                                         matype=0)

    def output_earning_rate(self):
        df = self.bars[-self.days:]
        df["signals"] = self.signals
        df["strategy"] = (1 + df.close.pct_change(1).fillna(0) * self.signals).cumprod()
        df["base"] = df['close'] / df['close'][0]
        df['rate_diff'] = df["strategy"] - df["base"] + 1
        print(df["strategy"].values[-1:])
        print(df["base"].values[-1:])
        print(df["rate_diff"].values[-1:] - 1)
        return df

    def show_plt(self):
        df = self.output_earning_rate()
        fig, axes = plt.subplots(3, 1, sharex=True, figsize=(18, 12))
        df[['strategy', 'base', 'rate_diff']].plot(ax=axes[0], grid=True, title='strategy', figsize=(20, 10))
        df[['signals']].plot(ax=axes[1], grid=True, title='signals', figsize=(20, 10))
        self.show_score(df, axes[2])
        plt.show()

    def show_score(self, df, ax):
        df['score'] = self.rsistrategy.get_scores(df)
        df.score.plot(ax=ax, grid=True, title='score', figsize=(20, 10))

    def process(self):
        # 默认是空仓状态的，1表示满仓 TODO 同样的，这里也可以去输出仓位控制到signals列表里面
        position = 0
        for i in range(self.days):
            # 当前的持仓，是上一天的信号
            singal = self.get_singal(self.bars[:-self.days + i - 1])
            if singal == -1:
                # 保持不变
                self.signals.append(position)
            else:
                position = singal
                self.signals.append(singal)

    # TODO 关于信号这个，目前貌似是直接给出买入卖出的信号，后面考虑将各个信号的数据均拿过来加权计算，基于加权求和值来执行操作策略
    def get_singal(self, bars: DataFrame):
        # if self.rsistrategy.get_singal(df=bars) == 1 and self.bollstrategy.get_singal(df=bars) == 1:
        #     return self.rsistrategy.get_singal(df=bars)
        # else:
        #     return 0
        return self.rsistrategy.get_singal_for_other_date_diff(df=bars)
