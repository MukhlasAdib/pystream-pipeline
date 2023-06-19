from pystream import get_profiler_db_folder, set_profiler_db_folder


def test_get_and_set_profiler_db_folder():
    test_path = "dummy/path"
    set_profiler_db_folder(test_path)
    assert get_profiler_db_folder() == test_path
