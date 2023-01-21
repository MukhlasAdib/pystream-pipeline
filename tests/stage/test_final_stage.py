import pytest

from pystream.data.pipeline_data import PipelineData, ProfileData
from pystream.stage.final_stage import FinalStage
from pystream.utils.general import _FINAL_STAGE_NAME, _PIPELINE_NAME_IN_PROFILE


class MockProfilerHandler:
    def __init__(self):
        self.data = None

    def process_data(self, data: ProfileData) -> None:
        self.data = data


class TestFinalStage:
    @pytest.fixture(autouse=True)
    def _init_stage(self) -> None:
        self.profiler = MockProfilerHandler()
        self.stage = FinalStage(self.profiler)

    def test_init(self):
        assert self.stage.name == _FINAL_STAGE_NAME
        assert self.stage.profiler_handler is self.profiler

    def test_call(self):
        data = PipelineData(data=[])
        ret = self.stage(data)
        assert len(ret.profile.started) == 0
        assert _PIPELINE_NAME_IN_PROFILE in ret.profile.ended
