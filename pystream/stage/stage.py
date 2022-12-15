from abc import ABC, abstractmethod
from typing import Callable, TypeVar, Union


T = TypeVar("T")


class StageAbstract(ABC):
    """Abstract class for the pipeline stage. All stage have to be
    cleaned up should be defined as a subclass of this class."""

    @abstractmethod
    def __call__(self, data: T) -> T:
        """Main process of the stage.

        Args:
            data (T): Input data

        Returns:
            T: output data, should be in the same type as input data
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Cleanup method for the stage. This method will be invoked
        during pipeline cleanup step"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the stage"""
        pass


class Stage(StageAbstract):
    @property
    def name(self) -> str:
        return ""


StageCallable = Union[Callable[[T], T], Stage]
