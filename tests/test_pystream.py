from pystream import __version__


def test_version():
    assert __version__ == "0.1.2"


def test_import():
    from pystream import Stage, Pipeline
    from pystream.functional import func_parallel_thread, func_serial
