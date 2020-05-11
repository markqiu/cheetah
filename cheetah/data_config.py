from collections import namedtuple
from logging import getLogger

import os
from datetime import date
import pandas as pd
import numpy as np

from .config_operator import ConfigOperator
from ..flow_operator import FastTdate
from ..mysql import MySQLBase
from ..loader import get_config

# 精度设置
pd.set_option("precision", 4)

logger = getLogger(__name__)


class CaihuiTableOperator(object):
    """
    财汇数据库信息管理

    SingleTimeSeriesTable: 定义时间序列数据表的属性，支持多表联合查询操作，必须提供联合查询的sql语句
    1. 对于时间日期字段，可以用datetime_index或pare_dates来标识。被标识的列将被转换为datetime格式。
    转换方法用的定义采用pandas的标准方式，即用冒号隔开，后面以strftime格式（http://strftime.org/）标识出转换方法。

    SingleVersionTable: 定义非时间序列数据表的属性，支持多表联合查询操作，必须提供联合查询的sql语句

    根据data_info3.py中定义的财华数据表的基础属性，以表为单位生成对应的主查询语句，不考虑where语句和order by
    访问caihui数据库，返回pandas数据框并缓存在内存或者文件中
    """

    SingleTimeSeriesTable = namedtuple(
        "SingleTimeSeriesTable",
        [
            "key_name",
            "table_name",
            "datetime_index",
            "other_indices",
            "other_cols",
            "ch_index",
            "ch_keys",
            "ch_other_cols",
            "sql",
            "parse_dates",
            "date_cols",
            "docs",
        ],
    )

    SingleVersionTable = namedtuple(
        "SingleVersionTable",
        [
            "key_name",
            "table_name",
            "datetime_index",
            "other_indices",
            "other_cols",
            "ch_index",
            "ch_keys",
            "ch_other_cols",
            "sql",
            "parse_dates",
            "date_cols",
            "docs",
        ],
    )

    # 内存中的df缓存
    in_mem_df_dict = {}

    # 通联数据表信息
    table_dict = {}

    # hdf5_path是缓存文件存放路径
    hdf5_path = None

    __mysql_conn = None

    _today_date = None  # 用于填充数据库中时间序列的缺失值

    @classmethod
    def __init__(cls, caihui_config_info):
        table_info_dict = caihui_config_info["TABLES"]

        if not cls.table_dict:
            # 初始化财华表信息
            cls._init_caihui_table(table_info_dict)
            cls.__compile()

        if not cls.hdf5_path:
            """
          初始化文件缓存目录配置
          """
            cls.hdf5_path = get_config("CACHE_DIR")

        if not cls.__mysql_conn:
            cls.__mysql_conn = MySQLBase(
                host=get_config("TLDB_HOST"),
                port=get_config("TLDB_PORT"),
                user=get_config("TLDB_USERNAME"),
                db=get_config("TLDB_NAME"),
                passwd=get_config("TLDB_PASSWORD"),
            )

        if not cls._today_date:
            cls._today_date = date.today()

    @classmethod
    def _init_caihui_table(cls, table_info_dict):
        for table_name, single_table_info in table_info_dict.items():
            if "type" not in single_table_info:
                logger.error("error in table_operator get_caihui_dict, 52.")
                continue
            if single_table_info["type"] == "single_time_series_table":
                del single_table_info["type"]
                single_table_info["table_name"] = table_name
                single_table_info["key_name"] = table_name + ":{start_datetime}:{end_datetime}"
                # 为向下兼容老的没有updatetime_index的配置文件
                if "date_cols" in single_table_info:
                    cls.table_dict[table_name] = cls.SingleTimeSeriesTable(**single_table_info)
                else:
                    cls.table_dict[table_name] = cls.SingleTimeSeriesTable(**single_table_info, date_cols=[])
            elif single_table_info["type"] == "single_version_table":
                del single_table_info["type"]
                single_table_info["table_name"] = table_name
                single_table_info["key_name"] = table_name + ":{version_name}"
                if "date_cols" in single_table_info:
                    cls.table_dict[table_name] = cls.SingleVersionTable(**single_table_info)
                else:
                    cls.table_dict[table_name] = cls.SingleVersionTable(**single_table_info, date_cols=[])
            else:
                logger.error("error in table_operator get_caihui_dict.")

    @classmethod
    def __compile(cls):
        """
        根据属性计算查询语句，填充table_dict字典以及其中的TableExtSQL对象
        TableExtSQL 对象类型namedtuple，定义：
            datetime_index：
            keys：
            type：
            sql：
        :return:
        """
        for table_name, table in cls.table_dict.items():
            if isinstance(table, cls.SingleTimeSeriesTable):
                keys = [table.datetime_index] + list(table.other_indices)
                TableExtSQL = namedtuple("TableExtSQL", table._fields + ("keys", "type"))

                table_new = TableExtSQL._make(list(table + (keys, cls.SingleTimeSeriesTable)))
                # 更新数据字典
                cls.table_dict[table_name] = table_new
            elif isinstance(table, cls.SingleVersionTable):
                keys = [table.datetime_index] + list(table.other_indices)
                TableExtSQL = namedtuple("TableExtSQL", table._fields + ("keys", "type"))
                table_new = TableExtSQL._make(list(table + (keys, cls.SingleVersionTable)))
                # 更新数据字典
                cls.table_dict[table_name] = table_new
            else:
                raise RuntimeError("未定义的表类别：{}, 表名：{}".format(type(table), table_name))

    @classmethod
    def query(cls, table_name, start_datetime: str = None, end_datetime: str = None):
        """
        根据参数caihui真假，判断是从才华还是通联数据库获取单张表的数据并返回pandas数据框
        :param table_name: table_info中定义的表名
        :param start_datetime: 开始时间
        :param end_datetime: 结束时间， start_datetime和end_datetime必须同时出现，如果其中和一个送None,则表示version模式
        :return: pandas数据框
        """
        if table_name not in cls.table_dict:
            raise RuntimeError(f"没有定义{table_name}表")

        table = cls.table_dict[table_name]

        if start_datetime and end_datetime and int(start_datetime) > int(end_datetime):
            raise RuntimeError("开始日期比结束日期大：start:{} end: {}".format(start_datetime, end_datetime))
        if start_datetime and end_datetime:
            sql = table.sql.format(start=start_datetime, end=end_datetime)
            table_type = "time"
        else:  # 不送start_datetime或end_datetime表示是version模式
            sql = table.sql
            table_type = "version"
        try:
            conn = cls.__mysql_conn._pool.connection()
            # df = pd.read_sql(sql, con=conn, index_col=table.keys, parse_dates=table.parse_dates)
            df = pd.read_sql(sql, con=conn, parse_dates=table.parse_dates)
            #  下面是吧相关的时间index名称换为date，
            # 不换的话会报 Exception: Data must be datetime indexed or have a column named 'date'异常
            # https://github.com/manahl/arctic/issues/256

            # 保证获取的交易所与caihui数据一致，上下兼容
            if len(table.other_indices) > 1:
                exchange = table.other_indices[-1]
                df[exchange] = df[exchange].apply(lambda x: "CNSESH" if "XSHG" in x else "CNSESZ")
            # 将date类型的列转换为datetime类型（arctic不支持写入date类型的数据）
            for col in table.date_cols:
                # 补充时间序列的缺失值，防止数据类型转化时出现错误
                df[col] = pd.to_datetime(df[col])
            df = df.fillna(np.nan)  # 因为通联数据中有None值，arctic库不支持None的存储，将None转变为nan

            df = df.set_index(table.keys)
            # 为保证上下兼容，字段名保持一致
            index_name = ["date"] + table.ch_keys
            df.index.names = index_name
            col_names = dict(zip(table.other_cols, table.ch_other_cols))
            df = df.rename(columns=col_names)

            if table_type == "time":
                df.sort_index(inplace=True)
                # 从mysql读出的df类型有问题会导致后面arctic的报错，下面把类型转化成正确的格式
                cursor = conn.cursor()
                cursor.execute(sql)
                column_type_tuple = cursor.fetchall()  # (('SCSTC47', 'double', 'YES', '', None, ''), ....)
                numeric_column_name = []
                for column_item in column_type_tuple:
                    if column_item[0] not in table.other_cols:
                        continue
                    if (
                        column_item[1].startswith("int")
                        or column_item[1].startswith("double")
                        or column_item[1].startswith("decimal")
                        or column_item[1].startswith("float")
                    ):
                        numeric_column_name.append(column_item[0])
                # 将数字的列转化成对应的数字格式
                df[numeric_column_name] = df[numeric_column_name].apply(lambda x: pd.to_numeric(x, errors="ignore"))
        finally:
            conn.close()
        return df

    @classmethod
    def generate_key(cls, table_name, start_datetime, end_datetime, version_name):
        """
        统一的key生成函数，用于生成在hdf5文件和内存中保存的key
        :param table_name: 数据表名
        :param start_datetime: 开始时间
        :param end_datetime: 结束时间
        :param version_name: 版本名
        :return: key值
        :except: KeyError
        """
        table = cls.table_dict[table_name]
        return table.key_name.format(start_datetime=start_datetime, end_datetime=end_datetime, version_name=version_name)

    @classmethod
    def to_cache(cls, df, table_name, start_datetime: str = None, end_datetime: str = None, version_name: str = None):
        """
        写入缓存文件。
        文件缓存内容设计(hdf5格式)：
          CACHE_DIR->{table_name}.hdf5 ->key={table_name}:{start}:{end} or {table_name}:{version_name}
        :param df: 将要缓存的dataframe
        :param table_name: 对应的表名
        :param start_datetime: 开始时间
        :param end_datetime: 结束时间
        :param version_name: 版本名称
        :param lock: LockFile，lockfile的文件锁对象，如果传入一个外部已获取的锁，则函数内部直接使用。
                    一般用于在query函数前使用，来保证多个进程同时访问财汇时，同时只有一个进程访问（因为多个同时访问容易死掉）。
        :param timeout: int，写入锁获取超时，缺省为1秒，如果未获取到锁则返回false
        :return: bool, 写入成功与否
        """
        # 如果df是空，则直接返回
        if df.empty:
            return

        key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)

        """
        TODO 增加key和保存内容的检查，见check_key_with_content.
        传入的日期和df中包含的实际日期可能不付，导致数据不一致的问题。
        目前无法增加，因为有些表如FINANCIALRATIOS是不连续的，建议把这部分表采用versiontable来管理。
        上述问题修改后才可以增加检查。
       """

        # 写入前需获取对应表文件的lock，等待timeout秒,超时则放弃(抛出LockTimeout)
        file_name = os.path.join(cls.hdf5_path, "{}.hdf5".format(table_name))
        with pd.HDFStore(file_name, mode="a") as store:
            if cls.get_type(table_name) in (cls.SingleVersionTable):
                # 如果是versiontabl类别，直接保存
                store.put(key, df, format="t")
                return True
            start_datetime = int(start_datetime)
            end_datetime = int(end_datetime)
            for cached_key in store.keys():
                cached_table_name, start_cached, end_cached = cached_key.split(":")
                start_cached = int(start_cached)
                end_cached = int(end_cached)
                if start_datetime >= start_cached and end_datetime <= end_cached:
                    """
                请求的数据完全包裹在cached的数据中，不需存储
                       cached_start o------------------o cached_end
                              start   o--------o end

                result:cached_start o------------------o  cached_end
                """
                    return
                elif start_datetime <= start_cached and end_datetime >= end_cached:
                    """
                请求的数据完全包裹了cached数据，直接覆盖存储
                       cached_start o------------------o cached_end
                        start   o--------------------------o end

                result: start   o--------------------------o  end
                """
                    store.put(key, df, format="t")
                    store.remove(cached_key)
                    return
                elif start_datetime <= start_cached and end_datetime < end_cached and end_datetime >= start_cached:
                    """
                请求的数据与cached数据交叉且start小于等于cached_start，拼接存储的后面部分，然后存储
                       start_cached o------------------o end_cached
                        start  o---------o end

                  result: start o----------------------o end_cached
                """
                    start_cached = FastTdate.last_tdate(start_cached)
                    df = df.loc[:start_cached]
                    cached_df = store.get(cached_key)
                    df = df.append(cached_df)
                    new_key = cls.generate_key(table_name, start_datetime, end_cached, version_name)
                    store.put(new_key, df, format="t")
                    store.remove(cached_key)
                    return
                elif start_datetime > start_cached and start_datetime <= end_cached and end_datetime >= end_cached:
                    """
                请求的数据与cached数据交叉且end大于等于cached_end，拼接后面部分，然后存储
                       start_cached o------------------o end_cached
                                         start o-----------o end

                result start_cached o----------------------o end
                """
                    end_cached = FastTdate.next_tdate(end_cached)  # 为了与已cached的内容错开日期
                    df = df.loc[end_cached:]
                    cached_df = store.get(cached_key)
                    cached_df = cached_df.append(df)
                    new_key = cls.generate_key(table_name, start_cached, end_datetime, version_name)
                    store.put(new_key, cached_df, format="t")
                    store.remove(cached_key)
                    return
            # eof

            # 执行到这里就说明没有重叠项或者是空库，直接写入
            store.put(key, df, format="t")

    @classmethod
    def is_cached(cls, table_name, start_datetime=None, end_datetime=None, version_name=None):
        try:
            key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)
            file_name = os.path.join(cls.hdf5_path, "{}.hdf5".format(table_name))
            if not os.path.isfile(file_name):
                return False
            with pd.HDFStore(file_name, mode="r") as store:
                if cls.get_type(table_name) in (cls.SingleVersionTable):
                    # TODO 是否拆分version 和 timeseries为两个类
                    if not version_name:
                        version_name = "latest"
                        key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)
                    return key in store
                for cached_key in store.keys():
                    cached_table_name, cached_start, cached_end = cached_key.split(":")
                    if table_name == cached_table_name[1:] and int(start_datetime) >= int(cached_start) and int(end_datetime) <= int(cached_end):
                        """
                        cached数据完全包裹了请求的数据
                               cached_start o------------------o cached_end
                                      start   o--------o end
                    """
                        return True
                    # eoi
                # eof
            # eow
        except KeyError:
            return False
        return False

    @classmethod
    def from_cache(cls, table_name, start_datetime: str = None, end_datetime: str = None, version_name=None, cols=None):
        """
        从缓存中获取需要的数据
        :param table_name:
        :param start_datetime:
        :param end_datetime:
        :param version_name:
        :return: 获取到的数据
        """
        key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)
        file_name = os.path.join(cls.hdf5_path, "{}.hdf5".format(table_name))
        if not os.path.isfile(file_name):
            return None
        if cls.get_type(table_name) in (cls.SingleVersionTable):
            where = None
        else:
            where = "{datetime_index}>=pd.Timestamp(start_datetime) &" "{datetime_index}<=pd.Timestamp(end_datetime)".format(
                datetime_index=cls.get_date_index_name(table_name)
            )
            # where = 'date>=pd.Timestamp(start_datetime) & date<=pd.Timestamp(end_datetime)'
        with pd.HDFStore(file_name, mode="r") as store:
            if cls.get_type(table_name) in (cls.SingleVersionTable):
                return store.get(key)
            for cached_key in store.keys():
                cached_table_name, cached_start, cached_end = cached_key.split(":")
                # cached_table_name[1:]是为了把store的key中第一个‘/’字符需要去掉
                if table_name == cached_table_name[1:] and int(start_datetime) >= int(cached_start) and int(end_datetime) <= int(cached_end):
                    df = store.select(cached_key, where=where, columns=cols)
                    return df

    @classmethod
    def get_type(cls, table_name):
        return cls.table_dict[table_name].type

    @classmethod
    def get_date_index_name(cls, table_name):
        if cls.table_dict[table_name].ch_index:
            return cls.table_dict[table_name].ch_index
        return cls.table_dict[table_name].datetime_index

    @classmethod
    def get_other_indices(cls, table_name):
        if cls.table_dict[table_name].ch_keys:
            return cls.table_dict[table_name].ch_keys
        return cls.table_dict[table_name].other_indices

    # @classmethod
    # def get_updatetime_index(cls, table_name):
    #     return cls.table_dict[table_name].updatetime_index

    @classmethod
    def get_key_name(cls, table_name):
        return cls.table_dict[table_name].key_name

    @classmethod
    def is_in_mem(cls, table_name, start_datetime=None, end_datetime=None, version_name=None):
        # TODO 实现按照重复的日期区间判断是否在缓存中，如果日期区间是覆盖的，则返回true
        try:
            key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)
            return key in cls.in_mem_df_dict.keys()
        except KeyError:
            return False

    def check_key_with_content(cls, table_name, start_datetime, end_datetime, df):
        # 检查key的时间和df的时间的一致性
        if cls.get_type(table_name) in (cls.SingleTimeSeriesTable,):
            date_values = df.index.get_level_values(cls.get_date_index_name(table_name))
            # 有些表用交易日无法判断，比如chidquote，直接判断日期，有些表需要转换为交易日如chdquote
            if start_datetime != min(date_values).strftime("%Y%m%d") and FastTdate.next_tdate(start_datetime) != FastTdate.transfer2tdate(
                min(date_values).strftime("%Y%m%d")
            ):
                raise AssertionError("开始日期与实际数据日期不符", table_name)
            if end_datetime != max(date_values).strftime("%Y%m%d") and FastTdate.last_tdate(end_datetime) != FastTdate.transfer2tdate(
                max(date_values).strftime("%Y%m%d")
            ):
                raise AssertionError("结束日期与实际数据日期不符", table_name)

    @classmethod
    def from_mem(cls, table_name, start_datetime=None, end_datetime=None, version_name=None):
        """
        copy dataframe from mem for future use or read
        :param table_name: 表名
        :param start_datetime: 开始日期
        :param end_datetime: 结束日期
        :param version_name: 版本名
        :return: return the df or df's copy
        """
        key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)
        return cls.in_mem_df_dict[key].copy()

    @classmethod
    def to_mem(cls, df, table_name, start_datetime=None, end_datetime=None, version_name=None):
        key = cls.generate_key(table_name, start_datetime, end_datetime, version_name)
        cls.in_mem_df_dict[key] = df


class MinuteLineOperator(object):
    def __init__(self, minite_line_config_info):
        self.data_src_info = minite_line_config_info["DATA_SRC_INFO"]

    def query(self):
        pass


class IndexStockOperator(object):
    def __init__(self, index_stock_config_info):
        self.data_src_info = index_stock_config_info["DATA_SRC_INFO"]

    def query(self):
        pass


class DataConfig(object):
    conf_data_info = None
    caihui_table_obj = None
    index_stock_obj = None
    minute_line_data_obj = None

    @classmethod
    def __init__(cls):
        ConfigOperator()
        cls.conf_data_info = ConfigOperator.get_data_config_info()
        for data_name, data_info in cls.conf_data_info.items():
            if data_name == "CAIHUI_TABLE":
                cls.caihui_table_obj = CaihuiTableOperator(data_info)
            elif data_name == "MINUTE_LINE" or data_name == "MINUTE_LINE_ADJ":
                cls.minute_line_data_obj = MinuteLineOperator(data_info)
            elif data_name.startswith("INDEX_STOCK_"):
                cls.index_stock_obj = IndexStockOperator(data_info)
            else:
                logger.error("config error")
