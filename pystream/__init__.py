from pathlib import Path

import pystream
from pystream import functional
from pystream.pipeline.pipeline import Pipeline
from pystream.stage.stage import Stage

__version__ = "0.1.2"

_PYSTREAM_DIR = str(Path(pystream.__file__).parent.absolute())
