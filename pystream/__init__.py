from pystream import functional
from pystream.utils.logger import LOGGER
from pystream.utils.general import (
    _PIPELINE_NAME_IN_PROFILE,
    get_profiler_db_folder,
    set_profiler_db_folder,
)
from pystream.pipeline.pipeline import Pipeline
from pystream.stage.stage import Stage

__version__ = "0.1.3"

logger = LOGGER

MAIN_PIPELINE_NAME = _PIPELINE_NAME_IN_PROFILE
