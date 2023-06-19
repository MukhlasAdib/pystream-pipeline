from typing import Optional

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.stage.final_stage import FinalStage
from pystream.stage.stage import Stage, StageCallable
from pystream.utils.errors import InvalidStageName, PipelineInitiationError
from pystream.utils.general import _FINAL_STAGE_NAME, _PIPELINE_NAME_IN_PROFILE


_STAGE_COUNTER = 0


def get_default_stage_name() -> str:
    global _STAGE_COUNTER
    _STAGE_COUNTER += 1
    return f"Stage_{_STAGE_COUNTER}"


def check_invalid_stage_name(name: str) -> None:
    if "-" in name:
        raise InvalidStageName("Stage name cannot contain dash ('-')")
    if "__" in name:
        raise InvalidStageName("Stage name cannot double underscore ('__')")
    if name.startswith("_"):
        raise InvalidStageName("Stage name cannot start with underscore ('_')")
    if name.endswith("_"):
        raise InvalidStageName("Stage name cannot end with underscore ('_')")
    if _PIPELINE_NAME_IN_PROFILE == name:
        raise InvalidStageName(f"Stage name cannot be {_PIPELINE_NAME_IN_PROFILE}")
    if _FINAL_STAGE_NAME == name:
        raise InvalidStageName(f"Stage name cannot be {_FINAL_STAGE_NAME}")


def get_stage_name(name: Optional[str], stage: StageCallable) -> str:
    if name is None:
        if isinstance(stage, Stage) and stage.name != "":
            name = stage.name
        else:
            name = get_default_stage_name()
    check_invalid_stage_name(name)
    return name


class StageContainer(Stage):
    def __init__(self, stage: StageCallable, name: Optional[str] = None) -> None:
        self._name = get_stage_name(name, stage)
        self.stage = stage
        if isinstance(stage, Stage):
            stage.name = self._name

    def __call__(self, data: PipelineData) -> PipelineData:
        data.profile.tick_start(self.name)
        data = self.stage(data)
        data.profile.tick_end()
        return data

    def cleanup(self) -> None:
        if isinstance(self.stage, Stage):
            self.stage.cleanup()

    @property
    def name(self) -> str:
        if isinstance(self.stage, Stage):
            name = self.stage.name
        else:
            name = self._name
        return name


class PipelineContainer(StageContainer):
    def __init__(self, stage: StageCallable, name: Optional[str] = None) -> None:
        if not isinstance(stage, PipelineBase):
            raise PipelineInitiationError(
                "Bug: PipelineContainer is used for non-pipeline stage"
            )
        if stage.stages is None:
            raise PipelineInitiationError(
                f"Bug: pipeline {type(stage).__name__} is not initialized properly"
            )
        final_stage = stage.stages[-1]
        if not isinstance(final_stage, FinalStage):
            raise PipelineInitiationError(
                f"Bug: pipeline {type(stage).__name__} final stage is not initialized properly"
            )
        final_stage.turn_profiler_off()
        super().__init__(stage, name)

    def __call__(self, data: PipelineData) -> PipelineData:
        data.profile.tick_start(self.name)
        data.data = self.stage(data.data)
        data.profile.tick_end()
        return data
