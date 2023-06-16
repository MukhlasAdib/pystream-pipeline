from typing import Optional, Protocol

from pystream.data.pipeline_data import PipelineData
from pystream.data.profiler_data import ProfileData
from pystream.stage.stage import Stage
from pystream.utils.general import _FINAL_STAGE_NAME


class ProfilerHandlerProtocol(Protocol):
    def process_data(self, data: ProfileData) -> None:
        ...


class FinalStage(Stage):
    def __init__(self, profiler_handler: Optional[ProfilerHandlerProtocol]) -> None:
        self.profiler_handler = profiler_handler
        self._name = _FINAL_STAGE_NAME

    def __call__(self, data: PipelineData) -> PipelineData:
        is_at_main = data.profile.is_at_main
        data.profile.tick_end()
        if self.profiler_handler is not None and is_at_main:
            self.profiler_handler.process_data(data.profile)
        return data

    def cleanup(self) -> None:
        pass

    @property
    def name(self) -> str:
        return self._name
