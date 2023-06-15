from pathlib import Path

import pystream

_PYSTREAM_DIR = str(Path(pystream.__file__).parent.absolute())
_PIPELINE_NAME_IN_PROFILE = "MainPipeline"
_FINAL_STAGE_NAME = "FinalStage"
_PROFILE_LEVEL_SEPARATOR = "__"
