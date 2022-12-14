from typing import Optional, TypeVar

from pystream.data.pipeline_data import PipelineData
from pystream.stage.profiler import end_measurement, start_measurement
from pystream.stage.stage import Stage, StageCallable

T = TypeVar("T")


_STAGE_COUNTER = 0


def get_default_stage_name() -> str:
    global _STAGE_COUNTER
    _STAGE_COUNTER += 1
    return f"Stage-{_STAGE_COUNTER}"


class StageContainer(Stage):
    def __init__(self, stage: StageCallable, name: Optional[str] = None) -> None:
        if name is None:
            name = get_default_stage_name()
        self.name = name
        self.stage = stage

    def __call__(self, data: PipelineData) -> PipelineData:
        start_measurement(self.name, data.profile)
        data = self.stage(data)
        end_measurement(self.name, data.profile)
        return data

    def cleanup(self) -> None:
        self.stage.cleanup()
