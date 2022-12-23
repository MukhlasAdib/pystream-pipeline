import time

from pystream.data.pipeline_data import ProfileData
from pystream.utils.profiler import ProfileDBHandler, ProfilerHandler


def generate_data(length: int = 1, wait: float = 0.5):
    started = {}
    ended = {}
    current = time.time()
    for i in range(length):
        name = chr(i + 65)
        started[name] = current
        current += wait
        ended[name] = current
    return ProfileData(started=started, ended=ended)


def test_profiler():
    profiler_handler = ProfilerHandler()
    data = generate_data(5, 0.5)
    profiler_handler.process_data(data)
    time.sleep(0.1)
    data = generate_data(5, 0.5)
    profiler_handler.process_data(data)
    time.sleep(0.1)
    data = generate_data(5, 0.5)
    profiler_handler.process_data(data)

    profiler_handler.summarize()
