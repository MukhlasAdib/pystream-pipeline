import time
from typing import Optional, TypeVar

from pystream.data.pipeline_data import PipelineData
from pystream.stage.stage import Stage, StageCallable
from pystream.utils.errors import InvalidStageName

T = TypeVar("T")


_STAGE_COUNTER = 0


def get_default_stage_name() -> str:
    global _STAGE_COUNTER
    _STAGE_COUNTER += 1
    return f"Stage_{_STAGE_COUNTER}"


class StageContainer(Stage):
    def __init__(self, stage: StageCallable, name: Optional[str] = None) -> None:
        if name is None:
            name = get_default_stage_name()
        if "-" in name:
            raise InvalidStageName("Stage name cannot contain dash ('-')")
        self._name = name
        self.stage = stage

    def __call__(self, data: PipelineData) -> PipelineData:
        data.profile.started[self.name] = time.perf_counter()
        data.data = self.stage(data.data)
        data.profile.ended[self.name] = time.perf_counter()
        return data

    def cleanup(self) -> None:
        self.stage.cleanup()

    @property
    def name(self) -> str:
        return self._name
