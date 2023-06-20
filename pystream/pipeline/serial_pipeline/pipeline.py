from typing import List, Optional

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.pipeline.utils.profiler import ProfilerHandler
from pystream.pipeline.utils.general import containerize_stages
from pystream.stage.final_stage import FinalStage
from pystream.stage.stage import Stage, StageCallable


class SerialPipeline(PipelineBase):
    def __init__(
        self,
        stages: List[StageCallable],
        names: List[Optional[str]],
        profiler_handler: Optional[ProfilerHandler] = None,
    ) -> None:
        """The class that will handle the serial pipeline.

        Args:
            stages (List[StageCallable]): The stages to be run
                in sequence.
            names (List[Optional[str]]): Stage names. If the name is None,
                default stage name will be given.
            profiler_handler (Optional[ProfilerHandler]): Handler for the profiler.
                If None, no profiling attempt will be done.
        """
        self.final_stage = FinalStage(profiler_handler)
        self.stages = containerize_stages(stages, names)
        self.stages.append(self.final_stage)
        self.results = PipelineData()

    def forward(self, data: PipelineData) -> bool:
        for stage in self.stages:
            data = stage(data)
        self.results = data
        return True

    def get_results(self) -> PipelineData:
        ret = self.results
        self.results = PipelineData()
        return ret

    def cleanup(self) -> None:
        for stage in self.stages:
            if isinstance(stage, Stage):
                stage.cleanup()
