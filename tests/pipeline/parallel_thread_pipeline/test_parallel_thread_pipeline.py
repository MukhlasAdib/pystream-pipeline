from queue import Queue
from threading import Event
import time

import pytest

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.parallel_thread_pipeline.pipeline import (
    StageLinks,
    ParallelThreadPipeline,
    StageThread,
    send_output,
)
from pystream.pipeline.utils.profiler import ProfilerHandler
from pystream.stage.container import StageContainer
from pystream.utils.errors import PipelineTerminated
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE, _PROFILE_LEVEL_SEPARATOR


def test_send_output():
    data = PipelineData(data="test")
    data_queue = Queue(maxsize=1)

    # Test normal send
    ret = send_output(
        data=data,
        output_queue=data_queue,
        block=True,
        replace=False,
        timeout=1,
    )
    assert ret == True
    assert data_queue.qsize() == 1

    # Test failed send
    ret = send_output(
        data=data,
        output_queue=data_queue,
        block=True,
        replace=False,
        timeout=1,
    )
    assert ret == False
    assert data_queue.qsize() == 1

    # Test send with replacement
    new_data = PipelineData(data="hello")
    ret = send_output(
        data=new_data,
        output_queue=data_queue,
        block=True,
        replace=True,
        timeout=1,
    )
    assert ret == True
    assert data_queue.qsize() == 1
    get_data = data_queue.get(timeout=1)
    assert get_data.data == "hello"


class TestStageThread:
    @pytest.fixture(autouse=True)
    def _create_thread(self, dummy_stage):
        self.input_queue = Queue(maxsize=1)
        self.output_queue = Queue(maxsize=1)
        self.stopper = Event()
        self.starter = Event()
        self.link = StageLinks(
            input_queue=self.input_queue,
            output_queue=self.output_queue,
            stopper=self.stopper,
            starter=self.starter,
        )
        self.stage = StageContainer(dummy_stage(val="stage", wait=0.1))
        self.stage_thread = StageThread(self.stage, self.link)

    def test_init_and_start(self):
        self.stage_thread.start()
        time.sleep(0.5)
        assert self.starter.is_set()

    def test_forward_data_blocking(self):
        self.stage_thread.start()
        self.stage_thread.all_out = True
        self.stage_thread.send_output_timeout = 1
        self.stage_thread.replace_output = False
        self.input_queue.put(PipelineData(data=[]), timeout=1)
        time.sleep(0.5)
        assert self.input_queue.qsize() == 0
        assert self.output_queue.qsize() == 1

        self.input_queue.put(PipelineData(data=["hi"]), timeout=1)
        time.sleep(1.2)
        assert self.input_queue.qsize() == 0
        assert self.output_queue.qsize() == 1
        assert self.output_queue.get(timeout=1).data == ["stage"]

    def test_forward_data_replace_output(self):
        self.stage_thread.start()
        self.stage_thread.all_out = False
        self.stage_thread.send_output_timeout = 1
        self.stage_thread.replace_output = True
        self.input_queue.put(PipelineData(data=["hi"]), timeout=1)
        time.sleep(0.1)
        self.input_queue.put(PipelineData(data=["hi again"]), timeout=1)
        time.sleep(0.5)
        assert self.input_queue.qsize() == 0
        assert self.output_queue.qsize() == 1
        assert self.output_queue.get(timeout=1).data == ["hi again", "stage"]

    def test_process_cleanup(self):
        self.stage_thread.start()
        time.sleep(0.1)
        self.stage_thread.process_cleanup()
        assert self.stopper.is_set()
        assert self.stage.stage.val is None  # type: ignore


class TestParallelThreadPipeline:
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage):
        self.num_stages = 5
        self.stages = []
        self.names = []
        for i in range(self.num_stages):
            self.stages.append(dummy_stage(val=i, wait=0.1))
            name = f"Sample_{i}"
            self.names.append(name)
        self.profiler = ProfilerHandler()
        self.pipeline = ParallelThreadPipeline(
            self.stages, self.names, profiler_handler=self.profiler
        )

    def test_init(self):
        assert len(self.pipeline.stages) == self.num_stages + 1
        assert len(self.pipeline.stage_threads) == self.num_stages + 1
        assert len(self.pipeline.stage_links) == self.num_stages + 1
        for i, stage in enumerate(self.pipeline.stages[:-1]):
            assert stage.name == self.names[i]
        for stage_thread in self.pipeline.stage_threads:
            assert stage_thread.links.starter.is_set()
            assert stage_thread.is_alive()

    def test_forward_and_get_results_and_profiler(self):
        assert self.pipeline.get_results().data is None
        for _ in range(3):
            data = PipelineData(data=[])
            data.profile.tick_start(_PIPELINE_NAME_IN_PROFILE)
            self.pipeline.forward(data)
            time.sleep(0.2)
        time.sleep(1)
        res = self.pipeline.get_results()
        assert res.data == list(range(self.num_stages))

        latency, throughput = self.profiler.summarize()
        assert len(latency) == self.num_stages + 1
        assert len(throughput) == self.num_stages + 1
        for name in self.names:
            level_name = f"{_PIPELINE_NAME_IN_PROFILE}{_PROFILE_LEVEL_SEPARATOR}{name}"
            assert level_name in latency
            assert level_name in throughput
        assert _PIPELINE_NAME_IN_PROFILE in latency
        assert _PIPELINE_NAME_IN_PROFILE in throughput
        for lat, fps in zip(latency.values(), throughput.values()):
            assert lat > 0
            assert fps > 0

    def test_cleanup(self):
        self.pipeline.cleanup()
        for stage_thread in self.pipeline.stage_threads:
            assert stage_thread.links.stopper.is_set()
            assert not stage_thread.is_alive()
        for stage in self.stages:
            assert stage.val is None

    def test_forward_terminated(self):
        self.pipeline.cleanup()
        with pytest.raises(PipelineTerminated):
            self.pipeline.forward(PipelineData(data=[]))
