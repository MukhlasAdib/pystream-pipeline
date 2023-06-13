from dataclasses import dataclass
from typing import Optional, Protocol

from pystream.data.pipeline_data import PipelineData


class StageQueueProtocol(Protocol):
    def put(self, item: PipelineData, block: bool = True, timeout: Optional[float] = None) -> None:
        ...

    def get(self, block: bool = True, timeout: Optional[float] = None) -> PipelineData:
        ...

    def empty(self) -> bool:
        ...


class StageEventProtocol(Protocol):
    def set(self) -> None:
        ...

    def is_set(self) -> bool:
        ...

    def wait(self, timeout: Optional[float] = None) -> bool:
        ...
    


@dataclass
class StageLinks:
    """Dataclass for links between stages."""

    # Queue for input data
    input_queue: StageQueueProtocol
    # Queue for output data
    output_queue: StageQueueProtocol
    # Event to stop the stage
    stopper: StageEventProtocol
    # Event to signal the stage is ready
    starter: StageEventProtocol