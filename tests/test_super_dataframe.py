import logging
import os

from cheetah.super_dataframe import SuperDataFrameMan, load_metadata


def test_load_metadata(conf_dir, CHDQUOTE_ADJ):
    logging.basicConfig(level=logging.DEBUG)
    sdf = SuperDataFrameMan(os.path.join(conf_dir, "metadata.yaml"))
