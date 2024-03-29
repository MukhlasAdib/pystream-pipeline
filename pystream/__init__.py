from pystream import functional
from pystream.utils.logger import LOGGER
from pystream.utils.general import (
    _PIPELINE_NAME_IN_PROFILE,
    get_profiler_db_folder,
    set_profiler_db_folder,
)
from pystream.pipeline.pipeline import Pipeline
from pystream.stage.stage import Stage

__version__ = "0.2.0"

logger = LOGGER
"""
The PyStream logger object from logging package
"""

MAIN_PIPELINE_NAME = _PIPELINE_NAME_IN_PROFILE
"""
The name of main pipeline in profiling result
"""
