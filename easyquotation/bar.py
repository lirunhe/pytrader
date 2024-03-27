import json, requests, datetime
import pandas as pd  #

import requests
from lxml import etree
# from fake_useragent import UserAgent
import random
import time
import urllib
import json
import pandas as pd
import re


# ua = UserAgent()

def is_shanghai(stock_code):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    assert type(stock_code) is str, "stock code need str type"
    sh_head = ("50", "51", "60", "90", "110", "113",
               "132", "204", "5", "6", "9", "7", 'sh')
    return stock_code.startswith(sh_head)


def Spider_stock(code_list, begin, end=datetime.date.today().strftime('%Y%m%d'), flag=0):
    """

    @param code_list:
    @param begin: 开始时间
    @param end: 结束时间
    @param flag: 股票类型：默认0表示深市股票；1表示沪市股票；100表示为恒生？
    @return:
    """
    url = 'https://6.push2his.eastmoney.com/api/qt/stock/kline/get?'
    header = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62",
        'Cookie': 'qgqp_b_id=e66305de7e730aa89f1c877cc0849ad1; qRecords=%5B%7B%22name%22%3A%22%u6D77%u9E25%u4F4F%u5DE5%22%2C%22code%22%3A%22SZ002084%22%7D%5D; st_pvi=80622161013438; st_sp=2022-09-29%2022%3A47%3A13; st_inirUrl=https%3A%2F%2Fcn.bing.com%2F; HAList=ty-1-000300-%u6CAA%u6DF1300%2Cty-0-002108-%u6CA7%u5DDE%u660E%u73E0%2Cty-1-600455-%u535A%u901A%u80A1%u4EFD%2Cty-0-002246-%u5317%u5316%u80A1%u4EFD',
        'Referer': 'https://data.eastmoney.com/',
        'Host': 'push2his.eastmoney.com'}
    stock_df = pd.DataFrame(columns=['股票代码', '股票名称', "时间", '开盘价', '收盘价', '最高价', '最低价', "涨跌幅", '涨跌额',
                                     "成交量", "成交额", "振幅", "换手率"])
    for code in code_list:
        # 构建url参数
        jq = re.sub('\D', '', '1.12.3' + str(random.random()))
        tm = int(time.time() * 1000)

        c = 1 if is_shanghai(code) else 0
        params = {'cb': 'jQuery{}_{}'.format(jq, tm),
                  'fields1': urllib.request.unquote('f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6', encoding='utf-8'),
                  'fields2': urllib.request.unquote('f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61',
                                                    encoding='utf-8'),
                  'ut': 'b2884a393a59ad64002292a3e90d46a5',
                  'klt': '101',
                  'fqt': '1',
                  'secid': '{}.{}'.format(c, code),
                  'beg': begin,
                  'end': end,
                  '_': '{}'.format(tm)
                  }
        # 发送请求
        res = requests.get(url.format(code), headers=header, params=params)
        res.encoding = "utf-8"
        # 去除js数据中的无关字符，以便符合json数据格式
        html = res.text.lstrip('jQuery{}_{}'.format(jq, tm) + '(')
        html = html.rstrip(');')
        # 转换为json数据
        js_html = json.loads(html)
        js_data = js_html['data']
        js_klines = js_data['klines']
        day_num = len(js_klines)
        for num in range(day_num):
            stock_df.loc[len(stock_df)] = [str(js_data['code']), js_data['name'], js_klines[num].split(",")[0],
                                           js_klines[num].split(",")[1],
                                           js_klines[num].split(",")[2], js_klines[num].split(",")[3],
                                           js_klines[num].split(",")[4],
                                           js_klines[num].split(",")[8], js_klines[num].split(",")[9],
                                           js_klines[num].split(",")[5],
                                           js_klines[num].split(",")[6], js_klines[num].split(",")[7],
                                           js_klines[num].split(",")[10]
                                           ]
        time.sleep(0.1)
    return stock_df


# 腾讯日线
def get_price_day_tx(code, end_date='', count=10, frequency='1d'):  # 日线获取
    unit = 'week' if frequency in '1w' else 'month' if frequency in '1M' else 'day'  # 判断日线，周线，月线
    if end_date:
        end_date = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime.date) else end_date.split(' ')[0]
    end_date = '' if end_date == datetime.datetime.now().strftime('%Y-%m-%d') else end_date  # 如果日期今天就变成空
    URL = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},{unit},,{end_date},{count},qfq'
    st = json.loads(requests.get(URL).content)
    ms = 'qfq' + unit
    stk = st['data'][code]
    buf = stk[ms] if ms in stk else stk[unit]  # 指数返回不是qfqday,是day
    df = pd.DataFrame(buf, columns=['trade_date', 'open', 'close', 'high', 'low', 'volume'], dtype='float')
    df.time = pd.to_datetime(df.time)
    df.set_index(['trade_date'], inplace=True)
    return df


# 腾讯分钟线,理论上应该设置传入结束日期为datetime类型而非date
def get_price_min_tx(code, end_date=None, count=10, frequency='1d'):  # 分钟线获取
    ts = int(frequency[:-1]) if frequency[:-1].isdigit() else 1  # 解析K线周期数
    if end_date:
        end_date = end_date.strftime('%Y-%m-%d') if isinstance(end_date, datetime.date) else end_date.split(' ')[0]
        formatted_string = end_date.strftime("%Y%m%d%H%M") if isinstance(end_date,
                                                                         datetime.date) else datetime.datetime.strptime(
            end_date.split(' ')[0], "%Y-%m-%d").strftime("%Y%m%d%H%M")
        # TODO 这里有一个问题，就是只能获取到当前时间的数据而非历史数据，故分钟级别的数据回溯得另外寻求出路，只能拿到近15天的数据
        URL = f'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m{ts},{formatted_string},{count}'
    else:
        URL = f'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m{ts},,{count}'
    st = json.loads(requests.get(URL).content)
    buf = st['data'][code]['m' + str(ts)]
    df = pd.DataFrame(buf, columns=['time', 'open', 'close', 'high', 'low', 'volume', 'n1', 'n2'])
    df = df[['time', 'open', 'close', 'high', 'low', 'volume']]
    df[['open', 'close', 'high', 'low', 'volume']] = df[['open', 'close', 'high', 'low', 'volume']].astype('float')
    df.time = pd.to_datetime(df.time)
    df.set_index(['time'], inplace=True)
    df.index.name = ''  # 处理索引
    df['close'][-1] = float(st['data'][code]['qt'][code][3])  # 最新基金数据是3位的
    return df


# sina新浪全周期获取函数，分钟线 5m,15m,30m,60m  日线1d=240m   周线1w=1200m  1月=7200m
def get_price_sina(code, end_date='', count=10, frequency='60m'):  # 新浪全周期获取函数
    frequency = frequency.replace('1d', '240m').replace('1w', '1200m').replace('1M', '7200m')
    mcount = count
    ts = int(frequency[:-1]) if frequency[:-1].isdigit() else 1  # 解析K线周期数
    if (end_date != '') & (frequency in ['240m', '1200m', '7200m']):
        end_date = pd.to_datetime(end_date) if not isinstance(end_date, datetime.date) else end_date  # 转换成datetime
        unit = 4 if frequency == '1200m' else 29 if frequency == '7200m' else 1  # 4,29多几个数据不影响速度
        count = count + (datetime.datetime.now() - end_date).days // unit  # 结束时间到今天有多少天自然日(肯定 >交易日)
        print(code, end_date, count)
    URL = f'http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={code}&scale={ts}&ma=5&datalen={count}'
    print(URL)
    dstr = json.loads(requests.get(URL).content)
    # df = pd.DataFrame(dstr, columns=['day', 'open', 'high', 'low', 'close', 'volume','ma_price5','ma_volume5'], dtype='float')
    df = pd.DataFrame(dstr, columns=['day', 'open', 'high', 'low', 'close', 'volume', 'ma_price5', 'ma_volume5'])
    df = df[['day', 'open', 'close', 'high', 'low', 'volume', 'ma_price5', 'ma_volume5']]
    df[['open', 'close', 'high', 'low', 'volume', 'ma_price5', 'ma_volume5']] = df[
        ['open', 'close', 'high', 'low', 'volume', 'ma_price5', 'ma_volume5']].astype('float')
    df.day = pd.to_datetime(df.day)
    df.set_index(['day'], inplace=True)
    df.index.name = ''  # 处理索引
    if (end_date != '') & (frequency in ['240m', '1200m', '7200m']):
        return df[df.index <= end_date][-mcount:]  # 日线带结束时间先返回
    return df


def get_price(code, end_date='', count=10, frequency='1d', fields=[]):  # 对外暴露只有唯一函数，这样对用户才是最友好的
    xcode = code.replace('.XSHG', '').replace('.XSHE', '')  # 证券代码编码兼容处理
    xcode = 'sh' + xcode if ('XSHG' in code) else 'sz' + xcode if ('XSHE' in code) else code

    if frequency in ['1d', '1w', '1M']:  # 1d日线  1w周线  1M月线
        try:
            return get_price_sina(xcode, end_date=end_date, count=count, frequency=frequency)  # 主力
        except:
            return get_price_day_tx(xcode, end_date=end_date, count=count, frequency=frequency)  # 备用

    if frequency in ['1m', '5m', '15m', '30m', '60m']:  # 分钟线 ,1m只有腾讯接口  5分钟5m   60分钟60m
        if frequency in '1m': return get_price_min_tx(xcode, end_date=end_date, count=count, frequency=frequency)
        try:
            return get_price_sina(xcode, end_date=end_date, count=count, frequency=frequency)  # 主力
        except:
            return get_price_min_tx(xcode, end_date=end_date, count=count, frequency=frequency)  # 备用


if __name__ == '__main__':
    # df = get_price('sh000001', frequency='1d', count=100)  # 支持'1d'日, '1w'周, '1M'月
    # print('上证指数日线行情\n', df)

    df = get_price('000001.XSHG', frequency='5m', end_date='2021-01-01', count=120)  # 支持'1m','5m','15m','30m','60m'
    print('上证指数分钟线\n', df)
