import pytest
from typing import Type

import pystream.stage.container as _container
from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.stage.container import StageContainer
from pystream.utils.errors import InvalidStageName
from pystream.utils.general import _FINAL_STAGE_NAME, _PIPELINE_NAME_IN_PROFILE
from tests.conftest import DummyStage


DEFAULT_NAME = "DefaultName"


def mock_default_name():
    return DEFAULT_NAME


def dummy_stage_func(x):
    return x


class MockPipeline(PipelineBase):
    def __init__(self):
        self.data = None

    def forward(self, data_input: PipelineData) -> bool:
        self.data = data_input
        return True

    def get_results(self) -> PipelineData:
        return PipelineData()

    def cleanup(self) -> None:
        pass


class TestStageContainer:
    @pytest.fixture(autouse=True)
    def _init_stage(self, dummy_stage: Type[DummyStage]) -> None:
        self.val = 1
        self.stage = dummy_stage(val=self.val)
        self.name = "Test_Name"

    def test_name_class(self, monkeypatch):
        monkeypatch.setattr(_container, "get_default_stage_name", mock_default_name)
        cont = StageContainer(self.stage)
        assert cont.name == DEFAULT_NAME
        assert self.stage.name == DEFAULT_NAME

        cont = StageContainer(self.stage, self.name)
        assert cont.name == self.name
        assert self.stage.name == self.name

        self.stage.name = "TestName2"
        assert cont.name == self.stage.name

    def test_invalid_names(self):
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, "Test-Name")
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, "Test__Name")
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, "_TestName")
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, "TestName_")
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, _PIPELINE_NAME_IN_PROFILE)
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, _FINAL_STAGE_NAME)

    def test_call(self):
        cont = StageContainer(self.stage, self.name)
        data = PipelineData(data=[])
        ret = cont(data)
        assert ret.data == [self.val]
        assert self.name in ret.profile.data.substage
        assert ret.profile.data.substage[self.name].started is not None
        assert ret.profile.data.substage[self.name].ended is not None

    def test_call_to_pipeline(self):
        mock_pipeline = MockPipeline()
        cont = StageContainer(mock_pipeline, self.name)
        data = PipelineData(data=[])
        ret = cont(data)
        assert isinstance(cont.stage.data, PipelineData)  # type: ignore

    def test_cleanup(self):
        cont = StageContainer(self.stage, self.name)
        cont.cleanup()
        assert self.stage.val is None

    def test_name_func(self, monkeypatch):
        monkeypatch.setattr(_container, "get_default_stage_name", mock_default_name)
        cont = StageContainer(dummy_stage_func)
        assert cont.name == DEFAULT_NAME

        cont = StageContainer(dummy_stage_func, self.name)
        assert cont.name == self.name

    def test_cleanup_func(self):
        cont = StageContainer(dummy_stage_func)
        cont.cleanup()
