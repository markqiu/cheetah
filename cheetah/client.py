import logging
from datetime import datetime

from pyarrow import plasma

from .hishty import Hishty, HishtyMan
from .plasma_store import equipment_id_to_object_id, object_id_to_equipment_id
from ..singleton import Singleton
from ..loader import get_config


class DataClient(Singleton):
    def __init__(self, plasma_store_name: str = None):
        self.plasma_store_name = plasma_store_name or get_config("DATA_SERVICE_NAME")
        self.__plasma_client__ = plasma.connect(plasma_store_name)
        logging.info("plasma服务器连接成功！")

    def get(self, equipment_id, serialization_context):
        """获取共享内存中的数据"""
        object_id = equipment_id_to_object_id(equipment_id)
        result = self.__plasma_client__.get(object_id, timeout_ms=0, serialization_context=serialization_context)
        return result

    def contains(self, equipment_id):
        return self.__plasma_client__.contains(equipment_id_to_object_id(equipment_id))

    def list(self):
        return [object_id_to_equipment_id(object_id) for object_id in self.__plasma_client__.list()]

    def store_capacity(self):
        return self.__plasma_client__.store_capacity()

    def get_hishty(self, start: datetime, end: datetime) -> Hishty or None:
        """获取分红数据, 注意：如果没有数据则永远等待，请自行先调用contains判断是否具备数据！"""
        hishty = self.get(HishtyMan.__HISHTY_OBJECT_ID__, serialization_context=HishtyMan.get_serialization_context())
        return Hishty(start, end, {key: hishty.symbol_infos[key] for key in hishty.symbol_infos if start.strftime("%Y%m%d") <= key <= end.strftime("%Y%m%d")})
