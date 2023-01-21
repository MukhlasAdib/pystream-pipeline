from typing import Optional

from pystream.data.pipeline_data import PipelineData
from pystream.stage.stage import Stage
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE
from pystream.utils.profiler import ProfilerHandler


class FinalStage(Stage):
    def __init__(self, profiler_handler: Optional[ProfilerHandler]) -> None:
        self.profiler_handler = profiler_handler
        self._name = "FinalStage"

    def __call__(self, data: PipelineData) -> PipelineData:
        data.profile.tick_end(_PIPELINE_NAME_IN_PROFILE)
        if self.profiler_handler is not None:
            self.profiler_handler.process_data(data.profile)
        return data

    def cleanup(self) -> None:
        pass

    @property
    def name(self) -> str:
        return self._name
