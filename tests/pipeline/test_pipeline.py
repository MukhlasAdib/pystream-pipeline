import time

import pytest

from pystream import Pipeline
from pystream.pipeline.serial_pipeline import SerialPipeline
from pystream.pipeline.parallel_pipeline import StagedThreadPipeline
from pystream.pipeline.pipeline import PipelineUndefined
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.data.pipeline_data import PipelineData


class MockPipeline(PipelineBase):
    def __init__(self):
        self.last_data = PipelineData()
        self.active = True

    def forward(self, data_input):
        self.last_data = data_input
        return True

    def get_results(self):
        return self.last_data

    def cleanup(self):
        self.active = False


INPUT_GENERATOR_OUTPUT = "input_generated"


def mock_input_generator():
    return INPUT_GENERATOR_OUTPUT


class TestPipeline:
    @pytest.fixture(autouse=True)
    def _create_pipeline(self):
        self.pipeline = Pipeline(mock_input_generator)

    def test_add(self, dummy_stage):
        assert len(self.pipeline.stages_sequence) == 0
        for _ in range(5):
            self.pipeline.add(dummy_stage())
        assert len(self.pipeline.stages_sequence) == 5

    def test_serialize(self, dummy_stage):
        assert self.pipeline.pipeline is None
        for _ in range(3):
            self.pipeline.add(dummy_stage())
        self.pipeline.serialize()
        assert isinstance(self.pipeline.pipeline, SerialPipeline)

    def test_parallelize(self, dummy_stage):
        assert self.pipeline.pipeline == None
        for _ in range(3):
            self.pipeline.add(dummy_stage())
        self.pipeline.parallelize()
        assert isinstance(self.pipeline.pipeline, StagedThreadPipeline)

    def test_forward(self):
        self.pipeline.pipeline = MockPipeline()
        new_data = "dummy"
        ret = self.pipeline.forward(new_data)
        assert ret == True
        assert isinstance(self.pipeline.pipeline.last_data, PipelineData)
        assert self.pipeline.pipeline.last_data.data == new_data

    def test_forward_generator(self):
        self.pipeline.pipeline = MockPipeline()
        ret = self.pipeline.forward()
        assert ret == True
        assert isinstance(self.pipeline.pipeline.last_data, PipelineData)
        assert self.pipeline.pipeline.last_data.data == INPUT_GENERATOR_OUTPUT

    def test_forward_exception(self):
        new_data = "dummy"
        with pytest.raises(PipelineUndefined):
            self.pipeline.forward(new_data)

    def test_get_results(self):
        self.pipeline.pipeline = MockPipeline()
        ret = self.pipeline.get_results()
        assert ret is None

        new_data = "dummy"
        self.pipeline.forward(new_data)
        ret = self.pipeline.get_results()
        assert ret == new_data

    def test_get_results_exception(self):
        with pytest.raises(PipelineUndefined):
            self.pipeline.get_results()

    def test_cleanup(self):
        mock_pipeline = MockPipeline()
        self.pipeline.pipeline = mock_pipeline
        self.pipeline.cleanup()
        assert mock_pipeline.active == False
        assert self.pipeline.pipeline is None

    def test_loop(self):
        self.pipeline.start_loop()
        assert self.pipeline._automation is not None
        assert self.pipeline._automation._loop_is_start.is_set()
        time.sleep(0.5)
        self.pipeline.stop_loop()
        assert not self.pipeline._automation._loop_is_start.is_set()

    def test_generate_pipeline_data(self):
        ret = self.pipeline._generate_pipeline_data()
        assert isinstance(ret, PipelineData)
        assert ret.data == INPUT_GENERATOR_OUTPUT
        new_data = "from_user"
        ret = self.pipeline._generate_pipeline_data(new_data)
        assert isinstance(ret, PipelineData)
        assert ret.data == new_data
