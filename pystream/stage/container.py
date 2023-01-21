from typing import Optional

from pystream.data.pipeline_data import PipelineData
from pystream.stage.stage import Stage, StageCallable
from pystream.utils.errors import InvalidStageName
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE


_STAGE_COUNTER = 0


def get_default_stage_name() -> str:
    global _STAGE_COUNTER
    _STAGE_COUNTER += 1
    return f"Stage_{_STAGE_COUNTER}"


def check_stage_name(name: str) -> None:
    if "-" in name:
        raise InvalidStageName("Stage name cannot contain dash ('-')")
    if _PIPELINE_NAME_IN_PROFILE == name:
        raise InvalidStageName(f"Stage name cannot be {_PIPELINE_NAME_IN_PROFILE}")


class StageContainer(Stage):
    def __init__(self, stage: StageCallable, name: Optional[str] = None) -> None:
        if name is None:
            name = get_default_stage_name()
        check_stage_name(name)
        self._name = name
        self.stage = stage

    def __call__(self, data: PipelineData) -> PipelineData:
        data.profile.tick_start(self.name)
        data.data = self.stage(data.data)
        data.profile.tick_end(self.name)
        return data

    def cleanup(self) -> None:
        self.stage.cleanup()

    @property
    def name(self) -> str:
        return self._name
