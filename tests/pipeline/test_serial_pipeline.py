import pytest

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.serial_pipeline import SerialPipeline
from pystream.utils.profiler import ProfilerHandler


class TestSerialPipeline:
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage):
        self.stages = []
        self.num_stages = 3
        for i in range(self.num_stages):
            dummy = dummy_stage(val=i, wait=0.1)
            self.stages.append(dummy)
        self.profiler = ProfilerHandler()
        self.pipeline = SerialPipeline(self.stages, profiler_handler=self.profiler)

    def test_forward_and_get_results(self):
        for _ in range(5):
            new_data = PipelineData(data=[])
            ret = self.pipeline.forward(new_data)
            assert ret == True
            assert self.pipeline.results is new_data

            res = self.pipeline.get_results()
            assert res.data == [0, 1, 2]
            assert self.pipeline.results.data is None
        assert len(self.profiler.summarize()[0]) == self.num_stages
        assert len(self.profiler.summarize()[1]) == self.num_stages

    def test_cleanup(self):
        self.pipeline.cleanup()
        for stage in self.stages:
            assert stage.val is None
