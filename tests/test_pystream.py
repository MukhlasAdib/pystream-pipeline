from pystream import __version__, MAIN_PIPELINE_NAME
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE


def test_version():
    assert __version__ == "0.2.0"


def test_import():
    from pystream import Stage, Pipeline, MAIN_PIPELINE_NAME, logger
    from pystream.functional import func_parallel_thread, func_serial


def test_constants():
    assert MAIN_PIPELINE_NAME == _PIPELINE_NAME_IN_PROFILE
