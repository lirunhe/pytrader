import datetime
import time

from pandas import DataFrame
from sqlalchemy import and_, desc

from dbconnect import OdsStockKlineDay, DataFetcher
from easyquant.quotation import use_quotation, Quotation




class DownloadDayKline:
    def __init__(self, stock_list: list, use_quotation: Quotation, session, date_diff=10,
                 end_date=datetime.datetime.now().strftime("%Y-%m-%d")):
        self.stock_all_data_dict = {
        }
        self.stock_list = stock_list
        self.date_diff = date_diff
        self.use_quotation = use_quotation
        self.end_date = end_date
        self.session = session

    def check_date_diff(self, stock_code):
        stock_all_data = self.session.query(OdsStockKlineDay.dt).filter(
            and_(OdsStockKlineDay.stock_code == stock_code
                 )).order_by(desc(OdsStockKlineDay.dt)).first()
        if stock_all_data is None:
            self.date_diff = 500
        else:
            if datetime.datetime.combine(stock_all_data.dt, datetime.datetime.min.time()) >= datetime.datetime.now() - datetime.timedelta(days=self.date_diff):
                pass
            else:
                self.date_diff = datetime.datetime.now().date() - stock_all_data.dt

    def get_bars(self, stock_code) -> DataFrame:
        size = self.date_diff
        end_dt = self.end_date
        return self.use_quotation.get_bars(stock_code, count=size, end_dt=end_dt)

    def insert_database(self, bars: DataFrame, stock_code):
        for row in bars.itertuples(index=True):
            dt = row[0]
            dt = dt.strftime('%Y-%m-%d')
            open = row[1]
            close = row[2]
            high = row[3]
            low = row[4]
            volume = row[5]
            ma_price5 = row[6]
            ma_volume5 = row[7]
            if dt in self.stock_all_data_dict:
                if self.stock_all_data_dict[dt] != volume:
                    data = self.session.query(
                        OdsStockKlineDay.stock_code,
                        OdsStockKlineDay.open,
                        OdsStockKlineDay.closed,
                        OdsStockKlineDay.high,
                        OdsStockKlineDay.low,
                        OdsStockKlineDay.trade_count,
                        OdsStockKlineDay.trade_sum,
                        OdsStockKlineDay.dt,
                        OdsStockKlineDay.update_time

                    ).filter(
                        and_(OdsStockKlineDay.stock_code == stock_code, OdsStockKlineDay.dt == dt)).first()
                    data.open = open
                    data.closed = close
                    data.high = high
                    data.low = low
                    data.trade_count = volume
                    data.trade_sum = 0
                    data.update_time = datetime.datetime.now()
            else:
                new_user = OdsStockKlineDay(stock_code=stock_code,
                                            open=open,
                                            closed=close,
                                            high=high,
                                            low=low,
                                            trade_count=volume,
                                            trade_sum=0,
                                            dt=dt,
                                            update_time=datetime.datetime.now())
                self.session.add(new_user)

        self.session.commit()

    def get_stock_dict(self, stock_code):
        stock_all_data = self.session.query(OdsStockKlineDay.dt, OdsStockKlineDay.trade_count).filter(
            and_(OdsStockKlineDay.stock_code == stock_code
                 # TODO 理论上应该是交易日往前推若干天
                 # OdsStockKlineDay.update_time >= datetime.datetime.now() - datetime.timedelta(days=self.date_diff)
                 )).all()

        if stock_all_data is not None:
            for i in stock_all_data:
                self.stock_all_data_dict[i.dt.strftime('%Y-%m-%d')] = i.trade_count

    def run(self):
        for stock_code in self.stock_list:
            self.check_date_diff(stock_code)
            self.stock_all_data_dict = {}
            self.get_stock_dict(stock_code)
            self.insert_database(self.get_bars(stock_code), stock_code)
            time.sleep(10)


if __name__ == '__main__':

    # quotation = use_quotation('jqdata')
    quotation = use_quotation('')
    stock_list = ['513330','600438','000503','300059','159742','sh000001','sz399001','sz399001','sz399006']
    datafetcher=DataFetcher()
    day_kline = DownloadDayKline(stock_list, quotation, session=datafetcher.session)
    day_kline.run()

    datafetcher.session.close()


"""

https://push2.eastmoney.com/api/qt/stock/get?invt=2&fltt=1&cb=jQuery35105945489097596375_1710687450461&fields=f58%2Cf107%2Cf57%2Cf43%2Cf59%2Cf169%2Cf170%2Cf152%2Cf46%2Cf60%2Cf44%2Cf45%2Cf171%2Cf47%2Cf86%2Cf292&secid=100.HSAHP&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=%7C0%7C0%7C0%7Cweb&_=1710687450462
"""

"""
https://stock.xueqiu.com/v5/stock/chart/minute.json?symbol=HKHSAHP&period=1d

"""

"""
https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20_fx_susdcny2024_3_17=/NewForexService.getDayKLine?symbol=fx_susdcny&_=2024_3_17

"""

"""
https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20_DINIW2024_3_17=/NewForexService.getDayKLine?symbol=DINIW&_=2024_3_17

"""

"""
https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112300013818511246284082_1710690617074&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=50&pageNumber=1&reportName=RPT_MUTUAL_DEAL_HISTORY&columns=ALL&source=WEB&client=WEB&filter=(MUTUAL_TYPE%3D%22005%22)

"""