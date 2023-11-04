import os
from pathlib import Path

import pystream

_PYSTREAM_DIR = str(Path(pystream.__file__).parent.absolute())
_FINAL_STAGE_NAME = "FinalStage"

_PIPELINE_NAME_IN_PROFILE = "MainPipeline"
_PROFILE_LEVEL_SEPARATOR = "__"
_PROFILER_DB_FOLDER = os.path.join(_PYSTREAM_DIR, "user_data")


def set_profiler_db_folder(folder_path: str) -> None:
    """Set the folder for the profiler SQLite database.
    This should be called before the Pipeline instantiation

    Args:
        folder_path (str): path to directory that contains
            the database
    """
    global _PROFILER_DB_FOLDER
    _PROFILER_DB_FOLDER = folder_path


def get_profiler_db_folder() -> str:
    """Get the folder for the profiler SQLite database

    Returns:
        str: path to directory that contains the database
    """
    return _PROFILER_DB_FOLDER
