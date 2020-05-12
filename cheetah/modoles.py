from pydantic import BaseModel, BaseConfig
from datetime import datetime, timezone
from pandas import Timestamp


__uniform_field_name__ = {
    "stock_quantity": "可用数量",
    "timing_sid_list": "择时装备列表",
    "risk_sid_list": "风控装备列表",
    "stock_sid_list": "选股装备列表",
    "trade_sid_list": "交易装备列表",
    "run_type": "运行方式",
    "reduce_mode": "减仓模式",
    "sold_filter_strategy_list": "卖出筛选器",
    "buy_filter_strategy_list": "买入筛选器",
    "filter_buy_stock_symbol": "保证持仓数量筛选器",
}


def convert2uniform_alias(field_name: str):
    if field_name in __uniform_field_name__:
        return __uniform_field_name__[field_name]
    else:
        return field_name


class SuperModel(BaseModel):
    class Config(BaseConfig):
        allow_population_by_field_name = True
        json_encoders = {datetime: lambda dt: dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"), Timestamp: lambda x: x.date()}
        alias_generator = convert2uniform_alias
