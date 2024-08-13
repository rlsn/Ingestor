"""
This script contains global configurations of the module
rlsn 2024
"""
from .core_api import *
from .utils import AttrDict

sys_config = AttrDict.from_json("./confs/system.conf")
CACHE_DIR = sys_config.cache_dir
DATA_DIR = sys_config.data_dir
META_PATH = sys_config.meta_path
AUTO_CLEAR_CACHE = sys_config.auto_clear_cache
DEFAULT_SUBSET_NAME = sys_config.default_subset_name