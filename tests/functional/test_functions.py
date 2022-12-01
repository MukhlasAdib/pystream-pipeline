from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from pystream.functional import func_parallel_thread, func_serial


FUNCTION_NUM = 5
FUNCTION_WAIT = 0.5


def get_dummy_function(val: int):
    def _func(data: list):
        time.sleep(FUNCTION_WAIT)
        data.append(val)
        return data

    return _func


@pytest.fixture
def dummy_functions():
    funcs = []
    for i in range(FUNCTION_NUM):
        funcs.append(get_dummy_function(i))
    return funcs


def test_func_parallel_thread(dummy_functions):
    parallel_func = func_parallel_thread(dummy_functions)
    data = []
    start_time = time.perf_counter()
    ret = parallel_func(data)
    delta_time = time.perf_counter() - start_time
    for i in range(FUNCTION_NUM):
        assert i in ret
    assert delta_time == pytest.approx(FUNCTION_WAIT, rel=0.1)


def test_func_parallel_thread_custom_pool(dummy_functions):
    my_pool = ThreadPoolExecutor(max_workers=10)
    parallel_func = func_parallel_thread(dummy_functions, executor=my_pool)
    data = []
    start_time = time.perf_counter()
    ret = parallel_func(data)
    delta_time = time.perf_counter() - start_time
    for i in range(FUNCTION_NUM):
        assert i in ret
    assert delta_time == pytest.approx(FUNCTION_WAIT, rel=0.1)


def test_func_serial(dummy_functions):
    serial_func = func_serial(dummy_functions)
    data = []
    start_time = time.perf_counter()
    ret = serial_func(data)
    delta_time = time.perf_counter() - start_time
    assert ret == [i for i in range(FUNCTION_NUM)]
    assert delta_time == pytest.approx(FUNCTION_WAIT * FUNCTION_NUM, rel=0.1)
