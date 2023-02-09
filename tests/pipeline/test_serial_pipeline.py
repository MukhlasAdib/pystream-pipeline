import pytest

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.serial_pipeline import SerialPipeline
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE
from pystream.utils.profiler import ProfilerHandler


class TestSerialPipeline:
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage):
        self.stages = []
        self.names = []
        self.num_stages = 3
        self.wait_time = 0.1
        for i in range(self.num_stages):
            dummy = dummy_stage(val=i, wait=self.wait_time)
            name = f"Sample_{i}"
            self.stages.append(dummy)
            self.names.append(name)
        self.profiler = ProfilerHandler()
        self.pipeline = SerialPipeline(
            self.stages, self.names, profiler_handler=self.profiler
        )

    def test_init(self):
        assert len(self.pipeline.pipeline) == self.num_stages + 1
        for i, stage in enumerate(self.pipeline.pipeline[:-1]):
            assert stage.name == self.names[i]

    def test_forward_and_get_results_and_profiler(self):
        num_cycle = 5
        for _ in range(num_cycle):
            new_data = PipelineData(data=[])
            new_data.profile.tick_start(_PIPELINE_NAME_IN_PROFILE)
            ret = self.pipeline.forward(new_data)
            assert ret == True
            assert self.pipeline.results is new_data

            res = self.pipeline.get_results()
            assert res.data == [i for i in range(self.num_stages)]
            assert self.pipeline.results.data is None

        latency, throughput = self.profiler.summarize()
        assert len(latency) == self.num_stages + 1
        assert len(throughput) == self.num_stages + 1
        for name in self.names:
            assert name in latency
            assert name in throughput
        assert _PIPELINE_NAME_IN_PROFILE in latency
        assert _PIPELINE_NAME_IN_PROFILE in throughput
        for lat, fps in zip(latency.values(), throughput.values()):
            assert lat > 0
            assert fps > 0

    def test_cleanup(self):
        self.pipeline.cleanup()
        for stage in self.stages:
            assert stage.val is None
