from typing import List, Optional

from pystream.pipeline.pipeline_base import PipelineBase
from pystream.stage.container import PipelineContainer, StageContainer
from pystream.stage.stage import Stage, StageCallable


def containerize_stages(
    stages: List[StageCallable], names: List[Optional[str]]
) -> List[Stage]:
    containers = [
        PipelineContainer if isinstance(stage, PipelineBase) else StageContainer
        for stage in stages
    ]
    return [Cont(stage, name) for Cont, stage, name in zip(containers, stages, names)]
