"""超级数据框
数据源 --(获取)--> 数据项 ---(计算)--> 因子 --(组装)--> 装备 --(组装)--> 机器人
"""
from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic.types import ConstrainedStr, NoneStr, Callable, List, Optional
from typing import Tuple, Union, Dict
import numpy as np

from typing import Set, Tuple

from pyarrow._plasma import ObjectNotAvailable
from pyarrow.lib import SerializationContext
from ruamel.yaml import YAML

from .models import SuperModel
from .service import DataMan, T
import logging
from datetime import datetime, timedelta


def load_metadata(conf_file) -> SuperDataFrameModel:
    """ 加载配置文件中的所有字段
    """
    yaml = YAML(typ="safe")
    with open(conf_file, "r", encoding="utf8") as file:
        conf = yaml.load(file)
        try:
            factor_dict = {}
            for item in conf["Factors"]:
                logging.debug(f"正在处理Factor：{item}")
                conf["Factors"][item]["name"] = item
                factor_dict[item] = Factor(**conf["Factors"][item])
            sdf = SuperDataFrameModel(columns=factor_dict)
            logging.info(f"处理配置完成，Factors: {[*conf['Factors'].keys()]}")
        except Exception:
            logging.error(f"读取数据配置发生错误，请检查配置是否合法！错误配置名称: {item}， 信息：{conf}", exc_info=True)
            quit(-1)
    return sdf


class Column(SuperModel):
    name: ConstrainedStr  # 数据列的统一名称
    dtype: str
    belong_to: ConstrainedStr  # 从哪张adam表获取
    description: NoneStr
    alias: Optional[List]  # 数据列的别名，第一个必须是数据表的列名，及belong_to中设置的数据表的列名。如果没有设置，则列名与name相同。如果有多个别名，则从第二个开始是其他的别名。

    class Config:
        arbitrary_types_allowed = [np.ndarray]


class Index(BaseModel):
    tdate: Column = Field(..., description="数据的日期")


class Factor(Column):
    """因子。因子是通过数据计算出来的。
    """

    calculate_method: Optional[Tuple[Callable, List]]
    dependencies: Optional[List]


class SuperDataFrameModel(SuperModel):
    columns: Dict[ConstrainedStr, Factor]

    def get_column(self, col_name: str) -> Column:
        return self.columns[col_name]

    def has_column(self, col_name: ConstrainedStr) -> bool:
        return col_name in self.columns

    def get_belong_to(self, col_name: ConstrainedStr) -> str or None:
        if self.has_column(col_name):
            return self.columns[col_name].belong_to
        else:
            return None

    def get_table_colname(self, col_name: ConstrainedStr) -> str or None:
        if self.has_column(col_name):
            alias = self.get_alias(col_name)
            if alias:
                return alias[0]
            else:
                return col_name
        else:
            return None

    def get_alias(self, col_name: ConstrainedStr) -> str or None:
        if self.has_column(col_name):
            if "alias" in self.columns[col_name].fields:
                return self.columns[col_name].alias
        return None

    def update_column(self, col: Column):
        if col.name in self.columns:
            self.columns[col.name] = col
        else:
            raise RuntimeError(f"没有这个column: {col}")

    def add_column(self, col: Column):
        if col.name not in self.columns:
            self.columns[col.name] = col
        else:
            raise RuntimeError(f"列: {col}已存在!")

    def remove_column(self, col_name: str):
        if col_name in self.columns:
            del self.columns[col_name]
        else:
            raise RuntimeError(f"没有这个column: {col_name}")


class SuperDataFrameMan(DataMan):
    """
    基础功能：数据及映射关系的灵活定义、数据增删查更新，数据检查和清洗，数据压缩，数据保存
    数据服务：数据灵活高速查询、拼装、转换

    """

    def fetch_data(self, start: datetime, end: datetime) -> T:
        pass

    @staticmethod
    def merge_data(old_data: T, insert_data: T) -> T:
        pass

    def check_data(self, data: T or ObjectNotAvailable) -> Set[Tuple[datetime, datetime]]:
        pass

    @classmethod
    def get_serialization_context(cls) -> SerializationContext:
        pass

    @staticmethod
    def serialize(data):
        pass

    @staticmethod
    def deserialize(data):
        pass

    def __init__(self, config_file_name, start=datetime(2010, 1, 1), today_toggle=True):
        """

        :param config_file_name:
        :param start: 数据的开始时间，缺省为20100101
        """
        super().__init__("super_data_frame", "超级数据框", start, today_toggle=today_toggle)
        self.sdf = self.check_config(load_metadata(config_file_name))
        self.init_dataframe()

    def check_config(self, config: SuperDataFrameModel):
        """  TODO 检查是否重名

        :param config:
        :return:
        """
        return config

    def init_dataframe(self):
        """以交易日为索引，建立起初始数据框"""
        pass

    def update_data(self):
        pass

    def get(self, start, end, cols):
        # TODO deal with alias to canonical name
        return self.df[[cols]].query("@start<= tdate <= end")
