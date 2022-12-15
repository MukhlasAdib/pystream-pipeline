from abc import ABC, abstractmethod

from pystream.data.pipeline_data import PipelineData


class PipelineBase(ABC):
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
            PipelineData: the obtained data
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Cleanup the pipeline."""
        pass
