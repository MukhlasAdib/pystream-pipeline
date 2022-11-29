import pytest

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.serial_pipeline import SerialPipeline


class TestSerialPipeline:
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage):
        self.stages = []
        for i in range(3):
            dummy = dummy_stage(val=i)
            self.stages.append(dummy)
        self.pipeline = SerialPipeline(self.stages)

    def test_forward_and_get_results(self):
        new_data = PipelineData(data=[])
        ret = self.pipeline.forward(new_data)
        assert ret == True
        assert self.pipeline.results is new_data

        res = self.pipeline.get_results()
        assert res.data == [0, 1, 2]
        assert self.pipeline.results.data is None

    def test_cleanup(self):
        self.pipeline.cleanup()
        for stage in self.stages:
            assert stage.val is None
