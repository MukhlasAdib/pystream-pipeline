from abc import ABC, abstractmethod
from typing import List

from pystream.stage.stage import StageCallable
from pystream.data.pipeline_data import PipelineData


class PipelineBase(ABC):
    @abstractmethod
    def __init__(
        self,
        stages: List[StageCallable],
    ) -> None:
        """The class that will handle the pipeline
        based on multi  threading.

        Args:
            stages (List[StageCallable]): The stages to be run
                in sequence.
        """
        pass

    @abstractmethod
    def forward(self, data_input: PipelineData) -> bool:
        """Send data to be processed by pipeline

        Args:
            data_input (PipelineData): the input data

        Returns:
            bool: True if the data is sent successfully, False otherwise
        """
        pass

    @abstractmethod
    def get_results(self) -> PipelineData:
        """Get output from the last stage

        Returns:
            Optional[PipelineData]: the obtained data
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Cleanup the pipeline."""
        pass
