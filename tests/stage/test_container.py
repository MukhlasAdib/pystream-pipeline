import pytest
from typing import Type

from pystream.data.pipeline_data import PipelineData
from pystream.stage.container import StageContainer
from pystream.utils.errors import InvalidStageName
from pystream.utils.general import _FINAL_STAGE_NAME, _PIPELINE_NAME_IN_PROFILE
from tests.conftest import DummyStage


class TestStageContainer:
    @pytest.fixture(autouse=True)
    def _init_stage(self, dummy_stage: Type[DummyStage]) -> None:
        self.val = 1
        self.stage = dummy_stage(val=self.val)
        self.name = "Test_Name"
        self.cont = StageContainer(self.stage, self.name)

    def test_init(self):
        assert self.cont.name == self.name
        assert self.stage.name == self.name
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, "Test-Name")
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, _PIPELINE_NAME_IN_PROFILE)
        with pytest.raises(InvalidStageName):
            cont = StageContainer(self.stage, _FINAL_STAGE_NAME)

    def test_name_func(self):
        def dummy_stage_func(x):
            return x

        cont = StageContainer(dummy_stage_func, self.name)
        assert cont.name == self.name

    def test_call(self):
        data = PipelineData(data=[])
        ret = self.cont(data)
        assert ret.data == [self.val]
        assert self.name in ret.profile.started
        assert self.name in ret.profile.ended

    def test_cleanup(self):
        self.cont.cleanup()
        assert self.stage.val is None
