import os
from pathlib import Path

import pystream

_PYSTREAM_DIR = str(Path(pystream.__file__).parent.absolute())
_FINAL_STAGE_NAME = "FinalStage"

_PIPELINE_NAME_IN_PROFILE = "MainPipeline"
_PROFILE_LEVEL_SEPARATOR = "__"
_PROFILER_DB_FOLDER = os.path.join(_PYSTREAM_DIR, "user_data")


def set_profiler_db_folder(folder_path: str) -> None:
    global _PROFILER_DB_FOLDER
    _PROFILER_DB_FOLDER = folder_path


def get_profiler_db_folder() -> str:
    return _PROFILER_DB_FOLDER
