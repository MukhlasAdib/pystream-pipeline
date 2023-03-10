from abc import ABC, abstractmethod
from typing import Callable, TypeVar, Union


T = TypeVar("T")


class Stage(ABC):
    """Abstract class for the pipeline stage. All stage have to be
    cleaned up should be defined as a subclass of this class."""

    name: str = ""

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


StageCallable = Union[Callable[[T], T], Stage]
