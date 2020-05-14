import logging
import os

from cheetah.super_dataframe import SuperDataFrameMan, load_metadata


def test_load_metadata(conf_dir):
    sdf_man = SuperDataFrameMan(os.path.join(conf_dir, "metadata.yaml"))
    assert "CHDQUOTE_ADJ" == sdf_man.sdf.get_belong_to("symbol")
    assert ["证券代码"] == sdf_man.sdf.get_alias("symbol")
    assert "证券代码" == sdf_man.sdf.get_table_colname("symbol")
    assert "risk_signal_identity" == sdf_man.sdf.get_table_colname("risk_signal_identity")
    assert "TCLOSE_20" == sdf_man.sdf.get_table_colname("20日前收盘价")
