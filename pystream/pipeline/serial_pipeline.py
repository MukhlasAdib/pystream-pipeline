from typing import List, Optional

from pystream.data.pipeline_data import PipelineData
from pystream.stage.container import StageContainer
from pystream.stage.final_stage import FinalStage
from pystream.stage.stage import Stage, StageCallable
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.utils.profiler import ProfilerHandler


class SerialPipeline(PipelineBase):
    def __init__(
        self,
        stages: List[StageCallable],
        profiler_handler: Optional[ProfilerHandler] = None,
    ) -> None:
        self.final_stage = FinalStage(profiler_handler)
        self.pipeline: List[Stage] = [StageContainer(stage) for stage in stages]
        self.pipeline.append(self.final_stage)
        self.results = PipelineData()

    def forward(self, data: PipelineData) -> bool:
        for stage in self.pipeline:
            data = stage(data)
        self.results = data
        return True

    def get_results(self) -> PipelineData:
        ret = self.results
        self.results = PipelineData()
        return ret

    def cleanup(self) -> None:
        for stage in self.pipeline:
            if isinstance(stage, Stage):
                stage.cleanup()
