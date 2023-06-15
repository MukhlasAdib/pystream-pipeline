from abc import abstractmethod
from typing import final

from pystream.stage.stage import Stage
from pystream.data.pipeline_data import PipelineData


class PipelineBase(Stage):
    @final
    def __call__(self, data: PipelineData) -> PipelineData:
        """Adapter that makes pipeline treated as stage

        Args:
            data (PipelineData): the input data

        Returns:
            PipelineData: the latest output data
        """
        self.forward(data)
        return self.get_results()

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
