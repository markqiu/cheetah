import os
import pytest

from stralib.adam.exceptions import AdamDataException
from stralib.adam.data_config import DataConfig
from stralib.adam.arctic_store import TimeSeriesStore, get_mongo_client
from stralib import CAIHUI_TIME_SERIES_LIBRARY_NAME

from pathlib import Path


@pytest.fixture(scope="class")
def project_root(request) -> str:
    """Returns project root folder."""
    project_root = Path(__file__).parent.parent.as_posix()
    if request.cls:
        request.cls.project_root = project_root
    return project_root


@pytest.fixture(scope="class")
def conf_dir(request, project_root) -> str:
    """
    重新读取配置文件，并返回配置文件目录
    Parameters
    ----------
    project_root

    Returns
    -------

    """
    from stralib import loader

    # 读入测试配置
    conf_dir = os.path.join(project_root, "conf")
    loader.load_config(cfg_dir=conf_dir)
    if request.cls:
        request.cls.conf_dir = conf_dir
    yield conf_dir
    loader.load_config()
