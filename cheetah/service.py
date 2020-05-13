import logging
import threading
from datetime import datetime
from typing import List, Any, Tuple, Dict, TypeVar, Set
from abc import ABC, abstractmethod

from pyarrow import plasma, SerializationContext
from pyarrow.plasma import PlasmaStoreFull, ObjectNotAvailable, PlasmaObjectExists

from .plasma_store import start_plasma_store, equipment_id_to_object_id
from .singleton import Singleton

T = TypeVar("T")


class DataMan(ABC):
    def __init__(self, id: str, name: str, start: datetime, today_toggle=False):
        """

        :param id: 数据的唯一id
        :param name: 数据的名称
        :param start: 设置保存数据的最开始时间，缺省为2010-01-01
        :param today_toggle: 是否取当日数据，缺省为否
        """
        self._数据标识符 = id
        self._数据名称 = name
        self._数据区间: Tuple[datetime, datetime] = (start, start)
        self._取当日数据: bool = today_toggle
        self._plasma_store_id = None

    @property
    def 数据获取ID(self):
        return self._plasma_store_id

    @数据获取ID.setter
    def 数据获取ID(self, new_object_id: str):
        self._plasma_store_id = new_object_id

    @property
    def 取当日数据(self):
        return self._取当日数据

    @取当日数据.setter
    def 取当日数据(self, flag):
        self._取当日数据 = flag

    @property
    def 数据标识符(self):
        return self._数据标识符

    @property
    def 数据名称(self):
        return self._数据名称

    @property
    def 数据区间(self) -> Tuple[datetime, datetime] or None:
        return self._数据区间

    @数据区间.setter
    def 数据区间(self, end: datetime) -> None:
        self._数据区间 = (self._数据区间[0], end)

    @abstractmethod
    def fetch_data(self, start: datetime, end: datetime) -> T:
        return NotImplemented

    @staticmethod
    @abstractmethod
    def merge_data(old_data: T, insert_data: T) -> T:
        return NotImplemented

    @abstractmethod
    def check_data(self, data: T or ObjectNotAvailable) -> Set[Tuple[datetime, datetime]]:
        return NotImplemented

    @classmethod
    @abstractmethod
    def get_serialization_context(cls) -> SerializationContext:
        return NotImplemented

    @staticmethod
    @abstractmethod
    def serialize(data):
        return NotImplemented

    @staticmethod
    @abstractmethod
    def deserialize(data):
        return NotImplemented


class DataService(Singleton, threading.Thread):
    """数据加载到共享内存"""

    # 记录内存中需要缓存的数据及其状态
    __objects__: Dict[str, DataMan]

    def __init__(self, plasma_store_name: str, object_list: List[DataMan], plasma_shm_size: int = 2000, update_interval: int = 300):
        """ 初始化plasma store和需要维护的数据列表
        :param plasma_store_name: 启动plasma store server的进程名称
        :param plasma_shm_size: 为缓存分配的内存大小, 以M为单位，缺省2000
        :param object_list: 拟缓存的对象列表
        :param update_interval: 更新间隔（s），缺省300秒
        """
        logging.info("开始启动plasma服务器...")
        if hasattr(self, "proc"):
            logging.debug("发现之前的旧服务器，正在杀掉...")
            self.proc.kill()
        self.plasma_store_name, self.proc = start_plasma_store(plasma_store_name, int(plasma_shm_size * 1e6))
        logging.info("plasma服务器启动成功！")

        self.__objects__ = {item.数据标识符: item for item in object_list}

        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.update_interval = update_interval

    def add_object(self, item: DataMan):
        self.__objects__[item.数据标识符] = item

    def remove_object(self, object_id: str):
        if self.contains(object_id):
            self._delete_plasma_object(None, self.objects[object_id].数据获取ID)
        return self.__objects__.pop(object_id)

    def has_object(self, object_id: str):
        return object_id in self.__objects__

    @property
    def objects(self) -> Dict[str, DataMan]:
        return self.__objects__

    def get_object_status(self, item_id):
        return self.__objects__[item_id]

    def deal_object(self, dataman: DataMan):
        plasma_client = plasma.connect(self.plasma_store_name)
        try:
            object_id = equipment_id_to_object_id(dataman.数据标识符)
            stored_data = plasma_client.get(object_id, timeout_ms=0, serialization_context=dataman.get_serialization_context())
            if stored_data is ObjectNotAvailable:
                logging.info(f"{dataman.数据名称}: 内存中还没有的数据。")
            missing_data_ranges = dataman.check_data(stored_data)
            if missing_data_ranges:
                logging.info(f"{dataman.数据名称}: 需要更新, 缺少的数据区间为：{missing_data_ranges}")
                merged_data = stored_data
                for start, end in missing_data_ranges:
                    fetched_data = dataman.fetch_data(start, end)
                    if merged_data is not ObjectNotAvailable:
                        merged_data = dataman.merge_data(merged_data, fetched_data)
                    else:
                        merged_data = fetched_data
                    logging.info(f"{dataman.数据名称}: ({start},{end})的数据获取和合并成功。")
                if plasma_client.contains(object_id):
                    logging.info(f"{dataman.数据名称}: 正在删除旧数据，请等待...")
                    plasma_client = self._delete_plasma_object(plasma_client, object_id)
                    logging.info(f"{dataman.数据名称}: 旧数据删除成功！")
                dataman.数据获取ID = plasma_client.put(merged_data, object_id=object_id, serialization_context=dataman.get_serialization_context())
                dataman.数据区间 = max(missing_data_ranges)[1]  # 扩展数据区间的后界
                logging.info(f"{dataman.数据名称}: 数据更新完成，重新保存ID为:{dataman.数据获取ID}, 新数据区间为：{dataman.数据区间}")
        except (KeyboardInterrupt, SystemExit):
            self.stop_event.set()
        except PlasmaStoreFull:
            logging.error(f"{dataman.数据名称}: 内存已经全部用满，保存数据失败。请清理内存!!!!")
        except Exception:
            logging.exception(f"{dataman.数据名称}: 本次更新出错, 在下次更新再重试，或请管理员检查原因。")
        finally:
            plasma_client.disconnect()

    def run(self):
        while not self.stop_event.is_set():
            for dataman in self.objects.copy().values():
                self.deal_object(dataman)
                if self.stop_event.is_set():
                    break
            logging.info(f"本次更新完成，{self.update_interval}秒后将重新执行数据更新...")
            self.check_object_list()
            self.stop_event.wait(self.update_interval)

    def contains(self, object_id: str, plasma_store_contains=False):
        """service中是否存在一个对象，缺省不判断plasma store中已经有这个对象，如果打开开关则同时判断plasma store中也有此对象"""
        if plasma_store_contains:
            return object_id in self.objects and self.objects[object_id].数据获取ID is not None and self._plasma_store_contains(object_id)
        else:
            return object_id in self.objects

    def _plasma_store_contains(self, object_id: str):
        plasma_client = plasma.connect(self.plasma_store_name)
        result = plasma_client.contains(equipment_id_to_object_id(object_id))
        plasma_client.disconnect()
        return result

    def check_object_list(self) -> List[bool]:
        plasma_client = plasma.connect(self.plasma_store_name)
        object_dict = plasma_client.list()
        result = []
        for item in self.objects:
            is_ok = plasma_client.contains(self.objects[item].数据获取ID)
            result.append(is_ok)
            if is_ok:
                data_size = round(object_dict[self.objects[item].数据获取ID]["data_size"] / 1024 / 1024, 2)
            else:
                data_size = 0
            logging.info(
                f"服务器自查信息：数据名称:{self.objects[item].数据名称}, 数据区间:{self.objects[item].数据区间}, 数据标识符:{self.objects[item].数据标识符}, 数据是否存在：{is_ok}, 数据大小: {data_size}M"
            )
        plasma_client.disconnect()
        return result

    def stop(self):
        logging.info("收到停止数据服务指令，开始停止服务..., 可能需要几分钟时间，请耐心等待!")
        self.stop_event.set()
        if self.proc.poll() is None:
            self.proc.kill()
        logging.info("数据服务已停止！")

    def _delete_plasma_object(self, plasma_client, object_id):
        """安全的删除plasma store中的对象。用内部函数_delete会莫名其妙的挂掉，故开发了此函数
        有时，产出plasam store中的对象会不起作用（也没错报错，但是对象依然还存在），因为这个对象可能仍然被引用，特别是被当前plasma_client引用，
        所以本函数会先断开连接，切断引用关系，然后再尝试多次删除，成功后再重建连接，并返回。
        Parameters
        ----------
        plasma_client: 必须传入当前的plama_client，因为有些时候某个object会被plasma_client给引用(refs)
        object_id: 欲删除的object_id

        Returns
        -------
        plasma_client
        """
        if plasma_client:
            plasma_client.disconnect()  # 断开重连以释放对object的ref
        plasma_client = plasma.connect(self.plasma_store_name)
        plasma_client.delete([object_id])
        retry_times = 120
        # 貌似plasam的delete是个后台线程处理的，删除后并不是立即生效，而是待下次删除线程工作时才会真正之星，下面代码就是等待执行线程生效后再返回，避免删除后发现对象仍然存在。
        while plasma_client.contains(object_id):
            self.stop_event.wait(1)
            if retry_times < 0:
                break
            else:
                retry_times -= 1
        return plasma_client
