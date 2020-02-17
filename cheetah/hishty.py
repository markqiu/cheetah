import logging
from copy import deepcopy
from datetime import datetime
from typing import List, Tuple, Dict, Set

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow.plasma import ObjectNotAvailable
from pyarrow.lib import SerializationContext

from .service import DataMan, T
from .. import get_series_data, FastTdate
from ..fb.date_time import tdates_2_tdate_ranges
from ..utils import create_empty_dataframe


class Hishty(object):
    def __init__(self, start, end, symbol_infos=None):
        """

        :param start:
        :param end:
        :param symbol_infos: 如果传入了则不再取数据
        """
        self.start = start
        self.end = end
        self.symbol_infos = symbol_infos if symbol_infos is not None else self.symbol_hishty(start, end)
        self.div_dates = self.symbol_infos.keys()

    def symbol_hishty(self, start: str, end: str):
        """
        记录分红、送股、转赠、配股的股票信息
        :param start:
        :param end:
        :return: dict-> {tdate: {symbol:{div_date:..,symbol:...}, symbol:{...}},tdate: {...}}
        """
        hishty_info = dict()

        pg = get_series_data("equ_allot", start, end, cols=["ALLOTMENT_RATIO", "ALLOTMENT_PRICE"])
        pg.reset_index(inplace=True)
        pg = pg.rename(columns={"EX_RIGHTS_DATE": "EX_DIV_DATE"})
        if pg.empty:
            li = ["PER_CASH_DIV", "PER_SHARE_DIV_RATIO", "PER_SHARE_TRANS_RATIO"]  # 临时变量
            pg = create_empty_dataframe(pg.columns.tolist() + li)
        else:
            pg["PER_CASH_DIV"] = np.nan
            pg["PER_SHARE_DIV_RATIO"] = np.nan
            pg["PER_SHARE_TRANS_RATIO"] = np.nan

        ret = get_series_data("equ_div", start, end, cols=["EX_DIV_DATE", "PER_CASH_DIV", "PER_SHARE_DIV_RATIO", "PER_SHARE_TRANS_RATIO"])
        ret.reset_index(inplace=True)
        if ret.empty:
            li = ["ALLOTMENT_RATIO", "ALLOTMENT_PRICE"]
            ret = create_empty_dataframe(ret.columns.tolist() + li)
        else:
            ret["ALLOTMENT_RATIO"] = np.nan
            ret["ALLOTMENT_PRICE"] = np.nan
        ret = pd.concat([ret, pg])
        if not ret.empty:
            ret = ret.reset_index(drop=True)
            ret["PER_CASH_DIV"] = ret["PER_CASH_DIV"].apply(lambda x: x if x > 0 else 0)
            ret["PER_SHARE_DIV_RATIO"] = ret["PER_SHARE_DIV_RATIO"].apply(lambda x: x if x > 0 else 0)
            ret["PER_SHARE_TRANS_RATIO"] = ret["PER_SHARE_TRANS_RATIO"].apply(lambda x: x if x > 0 else 0)
            ret["PER_SHARE_DIV"] = ret["PER_SHARE_DIV_RATIO"] + ret["PER_SHARE_TRANS_RATIO"]
            ret["ALLOTMENT_RATIO"] = ret["ALLOTMENT_RATIO"].apply(lambda x: x if x > 0 else 0)
            ret["ALLOTMENT_PRICE"] = ret["ALLOTMENT_PRICE"].apply(lambda x: x if x > 0 else 0)
            ret["EX_DIV_DATE"] = ret["EX_DIV_DATE"].apply(lambda x: x.strftime("%Y%m%d"))
            ret = ret[["EX_DIV_DATE", "SYMBOL", "PER_CASH_DIV", "PER_SHARE_DIV", "ALLOTMENT_RATIO", "ALLOTMENT_PRICE"]]
            ret = [list(x) for x in ret.values]
            keys = ["div_date", "symbol", "cash_div", "share_div", "pg_rate", "pg_price"]
            for data_line in ret:
                info = dict(zip(keys, data_line))
                if info["div_date"] in hishty_info:

                    if info["symbol"] in hishty_info[info["div_date"]]:
                        hishty_info[info["div_date"]][info["symbol"]]["cash_div"] += info["cash_div"]
                        hishty_info[info["div_date"]][info["symbol"]]["share_div"] += info["share_div"]
                    else:
                        hishty_info[info["div_date"]][info["symbol"]] = info
                else:
                    hishty_info[info["div_date"]] = {info["symbol"]: info}
        return hishty_info

    def div_opt(self, tdate: str, bonus_fundbal: float, stock: dict):
        """
        由于送股、分红、配股等原因，处理股票持仓信息，一些概率小的没有做处理
        :param tdate:日期
        :param bonus_fundbal:
        :param stock: 昨日策略给出的信号之一
        :return:
        """
        signal_list = list()
        tmp_stock = dict(stock)
        fundbal = bonus_fundbal

        # 如果这一天有股票除权
        if tdate in self.div_dates:
            # 本次处理的股票的除权信息
            hishty = self.symbol_infos[tdate][tmp_stock["symbol"]] if tmp_stock["symbol"] in self.symbol_infos[tdate] else {}
            if hishty:
                # 转赠/送股
                if hishty["share_div"]:
                    avilible_stkeffeft = tmp_stock["stkbal"] * hishty["share_div"]
                    tmp_stock["stkbal"] += avilible_stkeffeft
                    tmp_stock["avg_price"] = tmp_stock["avg_price"] / (1 + hishty["share_div"])
                    # 转赠/送股信号
                    signal = {
                        "SYMBOL": tmp_stock["symbol"],
                        "TPRICE": 0,  # 除权交易的价格给
                        "SIGNAL": 220010,  # 除权
                        "TDATE": tdate,
                        "SNAME": tmp_stock["sname"],
                        "OPERATOR": 0,  # 不需填
                        "MARKET": tmp_stock["market"],
                        "STKEFFEFT": avilible_stkeffeft,
                        "TAX": 0,
                    }
                    signal_list.append(signal)
                # 分红
                if hishty["cash_div"]:
                    avilible_fundbal = tmp_stock["stkbal"] * hishty["cash_div"]
                    fundbal += avilible_fundbal
                    tmp_stock["avg_price"] = tmp_stock["avg_price"] - hishty["cash_div"]
                    tmp_stock["mktval"] -= avilible_fundbal
                    # 除息的信号
                    signal = {
                        "SYMBOL": tmp_stock["symbol"],
                        "TPRICE": 0,  # 除权交易的价格给
                        "SIGNAL": 221007,  # 除息
                        "TDATE": tdate,
                        "SNAME": tmp_stock["sname"],
                        "OPERATOR": 0,  # 不需填
                        "MARKET": tmp_stock["market"],
                        "STKEFFEFT": avilible_fundbal,
                        "TAX": 0,
                    }
                    signal_list.append(signal)
                # 配股
                if hishty["pg_rate"] and hishty["pg_price"]:
                    avilible_stkeffeft = stock["stkbal"] * hishty["pg_rate"]
                    if (avilible_stkeffeft * hishty["pg_price"]) > fundbal:
                        avilible_stkeffeft = int((fundbal / hishty["pg_price"]) / 100) * 100
                    fundbal = fundbal - avilible_stkeffeft * hishty["pg_price"]
                    tmp_stock["stkbal"] += avilible_stkeffeft
                    tmp_stock["avg_price"] = (tmp_stock["avg_price"] + hishty["pg_price"] * hishty["pg_rate"]) / (1 + hishty["pg_rate"])
                    tmp_stock["mktval"] += avilible_stkeffeft * hishty["pg_price"]
                    # 交易费用（税）
                    tax = avilible_stkeffeft * hishty["pg_price"] * 0.0016
                    real_tax = tax if tax > 5 else 5
                    # 以配股价买入股票
                    signal = {
                        "SYMBOL": tmp_stock["symbol"],
                        "TPRICE": hishty["pg_price"],
                        "SIGNAL": 1,  # 买入
                        "TDATE": tdate,
                        "SNAME": tmp_stock["sname"],
                        "OPERATOR": 1,
                        "MARKET": tmp_stock["market"],
                        "STKEFFEFT": avilible_stkeffeft,
                        "TAX": abs(real_tax),
                    }
                    signal_list.append(signal)

        return fundbal, signal_list, tmp_stock

    def div_signal(self, tdate: str, signal: dict):
        tmp_signal = dict(signal)
        if tdate in self.div_dates:
            hishty = self.symbol_infos[tdate][tmp_signal["SYMBOL"]] if tmp_signal["SYMBOL"] in self.symbol_infos[tdate] else {}
            keys = ["div_date", "symbol", "cash_div", "share_div", "pg_rate", "pg_price"]
            if hishty:
                # 转赠/送股
                if hishty["share_div"]:
                    tmp_signal["TPRICE"] = tmp_signal["TPRICE"] / (1 + hishty["share_div"])
                    tmp_signal["STKEFFEFT"] = int(tmp_signal["STKEFFEFT"] * (1 + hishty["share_div"]) / 100) * 100
                # 分红
                if hishty["cash_div"]:
                    tmp_signal["TPRICE"] = tmp_signal["TPRICE"] - hishty["cash_div"]
                # 配股
                if hishty["pg_rate"] and hishty["pg_price"]:
                    tmp_signal["TPRICE"] = (tmp_signal["TPRICE"] + hishty["pg_price"] * hishty["pg_rate"]) / (1 + hishty["pg_rate"])

        return tmp_signal


hishty_info_ = None


def get_hishty(start: datetime or str, end: datetime or str):
    start = start.strftime("%Y%m%d") if isinstance(start, datetime) else start
    end = end.strftime("%Y%m%d") if isinstance(end, datetime) else end
    global hishty_info_
    if hishty_info_:
        if int(hishty_info_.end) >= int(end) and int(hishty_info_.start) <= int(start):
            return Hishty(start, end, {tdate: hishty_info_.symbol_infos[tdate] for tdate in hishty_info_.symbol_infos if int(start) <= int(tdate) <= int(end)})
        else:
            logging.debug(f"更新hishty数据，start={start}, end={end}, hishty_info_.start={hishty_info_.start}, hishty_info_.end={hishty_info_.end}")
    logging.debug(f"更新hishty数据，start={start}, end={end}")
    hishty_info_ = Hishty(start, end)
    return hishty_info_


class HishtyMan(DataMan):
    __HISHTY_OBJECT_ID__ = "hishty______________"

    def __init__(self, start: datetime = datetime(2010, 1, 1), today_toggle=False):
        super().__init__(self.__HISHTY_OBJECT_ID__, "分红送配信息", start, today_toggle)

    def fetch_data(self, start: datetime, end: datetime) -> Hishty:
        return Hishty(start, end)

    @staticmethod
    def merge_data(old_data: Hishty, insert_data: Hishty) -> Hishty:
        symbol_infos = deepcopy(old_data.symbol_infos)
        symbol_infos.update(insert_data.symbol_infos)
        return Hishty(min(symbol_infos), max(symbol_infos), symbol_infos)

    def check_data(self, data: Hishty or ObjectNotAvailable) -> Set[Tuple[datetime, datetime]]:
        start_tdate = self.数据区间[0]
        today = datetime.now()
        if self.取当日数据 and FastTdate.is_tdate(today):
            end_date = today
        else:
            end_date = FastTdate.last_tdate(today)

        all_trade_days = set(FastTdate.query_all_tdates(start_tdate, end_date, include_stop=True))
        if not data or data is ObjectNotAvailable:  # 没有数据的情况
            return tdates_2_tdate_ranges(all_trade_days)
        assert isinstance(data, Hishty), f"数据格式不正确，应为Hishty，实为{type(data)}"
        hishty = Hishty(start_tdate, end_date)
        error_trade_days = set().union({key for key in hishty.symbol_infos if key not in data.symbol_infos})
        return tdates_2_tdate_ranges(error_trade_days)

    @classmethod
    def get_serialization_context(cls) -> SerializationContext:
        context = SerializationContext()
        context.register_type(Hishty, Hishty.__name__, custom_serializer=cls.serialize, custom_deserializer=cls.deserialize)
        return context

    @staticmethod
    def serialize(data: Hishty):
        return pa.default_serialization_context().serialize(data.symbol_infos).to_components()

    @staticmethod
    def deserialize(data):
        symbol_infos = pa.deserialize_components(data)
        return Hishty(start=min(symbol_infos), end=max(symbol_infos), symbol_infos=symbol_infos)
