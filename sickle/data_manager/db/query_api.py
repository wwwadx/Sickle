from .pg_con import PostgreSql
import re
import numpy as np
import pandas as pd
import datetime as dt
import io
import sqlalchemy
from tzlocal import get_localzone
from psycopg2 import ProgrammingError, InternalError


def correct_fetch_columns(fields):
    if fields == '*':
        return fields
    else:
        field_list = re.split(r'[,\s]\s*', fields)
        field_list_correct = list()
        for i, str in enumerate(field_list):
            field_list_correct.append('\"' + str + '\"')
        line_correct = ','.join(value for value in field_list_correct)

        return line_correct


class QueryApi:
    def __init__(self, db_name):
        self.db = PostgreSql(db_name)

    def get_all_tables(self):
        """获取当前数据库中的全部表名

        Parameters
        ----------

        Returns
        -------

        result : list
            全部表名字的list
        """
        engine = self.db.get_engine()
        inspector = sqlalchemy.inspect(engine)
        result = [i for i in inspector.get_table_names()]
        return result

    def get_table_columns(self, table_name):
        """获取对应table的全部columns名

        Parameters
        ----------

        table_name : str
            数据库中的表名，eg.table = 'ST_LIST'.

        Returns
        -------

        result : list
            对应表全部columns的list
        """
        engine = self.db.get_engine()
        inspector = sqlalchemy.inspect(engine)
        result = [i['name'] for i in inspector.get_columns(table_name)]
        return result

    def table_comon(self, table, fields, key_dropna=True):
        """从数据库中获取数据

        Parameters
        ----------

        table : str
            数据库中的表名，eg.table = 'ST_LIST'.

        fields : str
            希望获取的字段名，eg. fields = '*'.

        key_dropna: bool
            是否去除na值

        Returns
        -------

        df : pd.DataFrame
            行情数据，index为date类型的日期，columns为各个指标名称
        """
        fields = correct_fetch_columns(fields)
        command_str = 'select ' + fields + ' from \"' + table + '\"'
        df = self.db.do_execute_with_return(command_str)
        if key_dropna:
            df.dropna(inplace=True, how='all')
        return df

    def table_all(self, table_name):
        """从数据库中获取对应表名的整张表

        Parameters
        ----------

        table_name : str
            数据库中的表名，eg.table = 'ST_LIST'.


        Returns
        -------

        df : pd.DataFrame
            数据库表名对应的整张表
        """
        fields = correct_fetch_columns('*')
        command_str = 'select ' + fields + ' from ' + table_name
        df = self.db.do_execute_with_return(command_str)
        return df

    def market_data(self, table, fields, start_date, end_date, night=False):
        """从数据库中获取行情数据
        Parameters
        ----------
        table : str
            数据库中的表名，eg.table = 'ST_LIST'.
        fields : str
            希望获取的字段名，eg. fields = 'DATETIME,CODES,SEC_NAME'.
        start_date : str
            起始时间，eg.'2015-01-01'.
        end_date : str
            结束时间，eg.'2016-01-01'.
        Returns
        -------
        df : pd.DataFrame
            行情数据，index为date类型的日期，columns为各个指标名称.
        """
        fields = correct_fetch_columns(fields)
        end_date = end_date + ' 23:59:59'
        command_str = 'select ' + fields + ' from \"' + table + \
                      '\" WHERE \"datetime\">=\'' + start_date + \
                      '\' and \"datetime\"<=\'' + end_date + '\''
        df = self.db.do_execute_with_return(command_str)
        if len(df) > 0:
            df.drop_duplicates(inplace=True)
            df = df.set_index('datetime').sort_index()
            tz_zone = get_localzone()
            if df.index.tzinfo == None:
                df = df.tz_localize(tz_zone)
            else:
                df = df.tz_convert(tz_zone)
            if not night:
                # 过滤掉全部夜盘数据
                filter_night = [i for i in df.index if i.time() <= dt.time(15) and i.time() >= dt.time(9)]
                df = df.loc[filter_night]
        return df

    def factor_data(self, factor_name, start_date, end_date,
                    key_pivot=True, key_sort_index=True, trade_days=[]):
        """从因子数据库获取因子数据.
        Parameters
        ----------
        factor_name : str
            因子名，与数据库中的表名一致, eg. 'D_BP'.
        start_date : str
            起始时间，eg.'2015-01-01'.
        end_date : str
            结束时间，eg.'2016-01-01'.
        key_pivot : bool
            结果的形式，True产生矩阵形式的因子
        key_sort_index : bool
            是否对结果按index进行排序
        trade_days : pd.DataFrame
            换仓日序列，可由fetch.trade_days产生.
        Returns
        -------
        df : pd.DataFrame
            因子值，index为date类型的日期，columns为字符串形式的各股代码.
        """
        fields = 'datetime,codes,' + factor_name
        fields = correct_fetch_columns(fields)
        start_date = str(start_date)
        end_date = str(end_date) + ' 23:59:59'
        if len(trade_days) == 0:
            if start_date == end_date:
                command_str = 'select ' + fields + ' from \"' + \
                              factor_name + '\" WHERE \"datetime\"=\'' + start_date + '\''
            else:
                command_str = 'select ' + fields + ' from \"' + factor_name + \
                              '\" WHERE \"datetime\">=\'' + start_date + \
                              '\' and \"datetime\"<=\'' + end_date + '\''
            df = self.db.do_execute_with_return(command_str)
        else:
            command_str = "SELECT * FROM \"" + \
                          factor_name + "\" where \"datetime\" in ("
            str_trade_days = [
                "'" + d.strftime("%Y-%m-%d") + "'" for d in trade_days]
            command_str = command_str + ','.join(str_trade_days) + ")"
            df = self.db.do_execute_with_return(command_str)
        if df is not None and len(df) > 0:
            df.drop_duplicates(subset=['datetime', 'codes'], inplace=True)
            if key_pivot:
                df = df.pivot_table(
                    index='datetime', columns='codes', values=factor_name)
            else:
                df.set_index(['datetime', 'codes'], inplace=True)
                df.dropna(how='all', inplace=True)
                if key_sort_index:
                    df.sort_index(inplace=True)
        return df

    def latest_day(self, table):
        """取出目标数据表中最近一条的时间.
        Parameters
        ----------
        table : str
            数据库中的表名，eg.table = 'ST_LIST'.
        Returns
        -------
        latestDay: datetime.datetime
            最近一天的日期.
        """
        command_str = 'SELECT MAX(\"datetime\") FROM \"%s\"' % table
        latest_day = self.db.do_execute_with_return(command_str)
        if latest_day is None:
            print('{} is empty! Please Check!(message from latest_day)'.format(table))
        else:
            latest_day = latest_day.values[0][0]
            if latest_day is not None:
                tz_zone = get_localzone()
                latest_day = latest_day.astimezone(tz_zone)
                latest_day = pd.to_datetime(latest_day).date()
        return latest_day

    def earliest_day(self, table):
        """取出目标数据表中最早一条的时间.
        Parameters
        ----------
        table : str
            数据库中的表名，eg.table = 'ST_LIST'.
        Returns
        -------
        latestDay: datetime.datetime
            最近一天的日期.
        """
        command_str = 'SELECT MIN(\"datetime\") FROM \"%s\"' % table
        earliest = self.db.do_execute_with_return(command_str)
        if earliest is None:
            print('{} is empty! Please Check!'.format(table))
        else:
            earliest = earliest.values[0][0]
            tz_zone = get_localzone()
            earliest = earliest.astimezone(tz_zone)
            earliest = pd.to_datetime(earliest).date()
        return earliest

    def all_contracts(self):
        engine = self.db.get_engine()
        result = list(pd.read_sql_table('instruments', engine)['symbol'])
        return result

    def get_trading_days(self):
        engine = self.db.get_engine()
        result = pd.read_sql_table('trading_dates_cn', engine)
        result = result.set_index('trading_date').index
        return result

    def is_trading_day(self, date):
        """判断给定日期是否是交易日
        Parameters
        ----------
        date : str or datetime.datetime or pandas.Timestamp
            参考日期，可以使用字符串类型的日期表示或者datetime对象
            或者pandas Timestamp对象
        Returns
        -------
        is_trading_day : bool
            是否是交易日
        """
        trading_dates = self.get_trading_days()
        return any(trading_dates == date)

    def last_trading_day(self, dt_given=dt.datetime.now().date(),
                         n=1):
        """获得上一交易日
        Parameters
        ----------
        dt_given : str or datetime.datetime or pandas.Timestamp
            参考日期，计算上一交易日会返回基于该日期的上一个交易日，可以使用
            字符串类型的日期表示或者datetime对象或者pandas Timestamp对象
        n : int
            倒数第几个交易日
        Returns
        -------
        last_trading_day : pd.Timestamp
            上一交易日
        """
        trading_dates = self.get_trading_days()
        passed_dates = trading_dates[trading_dates < str(dt_given)]
        try:
            return passed_dates[-n]
        except IndexError:
            return np.nan

    def copy_data_frame_to_pg(self, table_name, data_to_write):
        """获得上一交易日
        Parameters
        ----------
        table_name : str
            写入数据库的表名
        data_to_write : pd.DataFrame
            需要写入的数据, 存在DATETIME, CODES列则设置为index
        Returns
        -------
        """
        s_buf = io.StringIO()
        data_to_write.to_csv(s_buf)
        s_buf.seek(0)
        all_table_names = self.db.get_all_tb_names()
        conn = self.db.raw_con()
        cur = conn.cursor()
        if table_name not in all_table_names:
            data_to_write[0:2].to_sql(
                table_name,
                con=self.db.get_engine(),
                if_exists='fail',
                index=True,
                chunksize=10000,
                dtype={
                    'DATETIME': sqlalchemy.types.TIMESTAMP,
                    'CODES': sqlalchemy.types.VARCHAR(255)
                }
            )
            sql_clear = 'DELETE FROM "{}"'.format(table_name)
            try:
                cur.execute(sql_clear)
                conn.commit()
            except ProgrammingError or InternalError:
                conn.rollback()
            print('Create new table \"%s\" successfully!' % table_name)
        to_sql = """COPY \"%s\" FROM STDIN WITH CSV HEADER"""
        cur.copy_expert(sql=to_sql % table_name, file=s_buf)
        conn.commit()
        cur.close()
        print('\"%s\" has been written to PG!!!' % table_name)




