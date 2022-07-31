from typing import Any, List

from pystream.data.pipeline_data import PipelineData
from pystream.stage.stage import Stage, StageCallable
from .pipeline_base import PipelineBase


class SerialPipeline(PipelineBase):
    def __init__(
        self,
        stages: List[StageCallable],
    ) -> None:
        self.pipeline = stages
        self.results = PipelineData()

    def forward(self, data: PipelineData) -> bool:
        for stage in self.pipeline:
            data.data = stage(data.data)
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
