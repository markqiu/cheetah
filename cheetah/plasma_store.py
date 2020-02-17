import os
import subprocess

import pyarrow.plasma as plasma

PLASMA_DIR = os.path.dirname(plasma.__file__)


def start_plasma_store(plasma_store_name: str, plasma_store_memory: int, verbose=False):
    """ 启动plasma store server
    :param plasma_store_name: 这个机器唯一的sock的名称
    :param plasma_store_memory: 给服务分配的内存大小
    :param verbose:
    :return:
    """
    # Pyarrow version > 0.14
    plasma_store_executable = os.path.join(PLASMA_DIR, "plasma-store-server")

    if not os.path.exists(plasma_store_executable):
        # Pyarrow version <= 0.14
        plasma_store_executable = os.path.join(PLASMA_DIR, "plasma_store_server")

    command = [plasma_store_executable, "-s", plasma_store_name, "-m", str(plasma_store_memory)]

    stdout = stderr = None if verbose else subprocess.DEVNULL

    proc = subprocess.Popen(command, stdout=stdout, stderr=stderr)

    return plasma_store_name, proc


def equipment_id_to_object_id(equipment_id: str) -> plasma.ObjectID:
    """equipment_id转换为object_id的统一算法"""
    return plasma.ObjectID(equipment_id.ljust(20, "_").encode())  # object_id是装备标识后面自动补_，补足20位


def object_id_to_equipment_id(object_id: plasma.ObjectID) -> str:
    """object_id转换为equipment_id的统一算法"""
    return str(object_id.binary().rstrip(b"_"))  # 删除掉右边的_则还原为object_id
