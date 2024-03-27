from decimal import Decimal

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Numeric, Date, create_engine, MetaData, Table, Index, and_, TIMESTAMP, \
    desc
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from db_config import DB_URL
# 创建一个基类
Base = declarative_base()


class OdsStockKlineDay(Base):
    __tablename__ = 'ods_stock_kline_day'
    __table_args__ = {'schema': 'ods'}

    stock_code = Column(String(32), primary_key=True, nullable=False)
    open = Column(Numeric, nullable=True)
    closed = Column(Numeric, nullable=True)
    high = Column(Numeric, nullable=True)
    low = Column(Numeric, nullable=True)
    trade_count = Column(Numeric, nullable=True)
    trade_sum = Column(Numeric, nullable=True)
    dt = Column(Date, primary_key=True, nullable=False)
    update_time = Column(TIMESTAMP)

    # 定义数据获取类


class DataFetcher:
    def __init__(self, column_rename_map={}, index_cloumn='dt'):
        self.index_cloumn = index_cloumn
        self.column_rename_map = column_rename_map
        engine = create_engine(DB_URL)

        # 创建会话类
        Session = sessionmaker(bind=engine)
        session = Session()
        self.session = Session()

    def fetch_data_as_df(self, query):
        """
        执行SQLAlchemy查询并将结果转化为pandas DataFrame
        """
        # 使用session执行查询
        result = self.session.execute(query)

        # 执行查询并获取结果
        results = result.all()

        # 将结果转换为字典列表
        rows = [dict(row._asdict()) for row in results]

        # 创建DataFrame，并设置dt为索引
        df = pd.DataFrame(rows).set_index(self.index_cloumn)
        # 按照索引进行排序，默认正排序
        df = df.sort_index()

        # 务必注意数据类型需要为指定类型才可以出图
        # 函数用于将Decimal转换为float，非Decimal值保持不变
        def convert_decimal_to_float(value):
            if isinstance(value, Decimal):
                return float(value)
            return value

            # 应用函数到DataFrame的每一列，仅当列的数据类型为object时

        df = df.applymap(convert_decimal_to_float)

        # 重命名列
        df = df.rename(columns=self.column_rename_map)

        return df

    def close_session(self):
        """
        关闭数据库会话
        """
        self.session.close()

        # 创建数据库连接

    def __del__(self):
        # 析构函数，断开连接
        self.close_session()
