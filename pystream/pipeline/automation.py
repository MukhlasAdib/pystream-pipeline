import time

from threading import Event, Thread

from pystream.general.errors import PipelineUndefined
from pystream.pipeline.pipeline import Pipeline


class PipelineAutomation(Thread):
    def __init__(self, pipeline: Pipeline, period: float) -> None:
        self.pipeline = pipeline
        self._loop_period = period
        self._loop_is_start = Event()
        self._loop_thread = Thread(
            target=self._loop_handler, name="PyStream-Automation", daemon=True
        )

    def start(self):
        self._loop_is_start.set()
        self._loop_thread.start()

    def stop(self):
        self._loop_is_start.clear()
        self._loop_thread.join()

    def _loop_handler(self) -> None:
        """Function to be run by the input generator thread"""
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        self._loop_is_start.wait()
        check_period = max(0.001, self._loop_period / 10)
        while self._loop_is_start.is_set():
            last_update = time.time()
            data = self.pipeline._generate_pipeline_data()
            self.pipeline.forward(data)
            while time.time() - last_update < self._loop_period:
                time.sleep(check_period)
