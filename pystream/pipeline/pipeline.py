from __future__ import annotations

from threading import Event, Thread
import time
from typing import Any, Callable, List, Optional

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.stage.stage import StageCallable
from .serial_pipeline import SerialPipeline
from .parallel_pipeline import StagedThreadPipeline


class PipelineUndefined(Exception):
    pass


class Pipeline:
    """The pipeline constructor

    Args:
        input_generator (Optional[Callable[[], Any]], optional): Function that will be
            used to generate input data if you want the pipeline to run autonomously.
            If None, the input needs to be given by invoking "forward" method.
            Defaults to None.
    """

    def __init__(self, input_generator: Optional[Callable[[], Any]] = None) -> None:
        self.stages_sequence: List[StageCallable] = []
        self.pipeline: Optional[PipelineBase] = None
        self.__input_generator: Callable[[], Any] = lambda: None
        if input_generator is not None:
            self.__input_generator = input_generator

        self.__loop_period = 0.01
        self.__loop_is_start = Event()
        self.__loop_thread = Thread(
            target=self.__loop_handler, name="PyStream-InputGen", daemon=True
        )

    def add(self, stage: StageCallable) -> None:
        """Add a stage into the pipeline

        The stage is in type of StageCallable, which is Union[Callable[[T], T], Stage].
        Thus, a stage can be defined in two ways:

        (1) A stage can be a function that takes
        an input data (of any type)  and then returns an output data of the same type.

        (2) A stage can be a class that inherits from pystream.Stage class, which is an
        abstract class. Methods `__call__` and `cleanup` need to be defined there. Use this
        if the stage need a special cleanup procedure.

        Args:
            stage (StageCallable): the stage to be added
        """
        self.stages_sequence.append(stage)

    def serialize(self) -> Pipeline:
        """Turn the pipeline into serial pipeline. All stages will
        be run in sequential and blocking mode.

        Returns:
            Pipeline: this pipeline itself
        """
        self.pipeline = SerialPipeline(self.stages_sequence)
        return self

    def parallelize(
        self, block_input: bool = True, input_timeout: float = 10
    ) -> Pipeline:
        """Turn the pipeline into independent stage pipeline. Each stage
        will live in different thread and work asynchronously. However,
        the data will be passed to the stages in the same order as defined

        Args:
            block_input (bool, optional): Whether to set the forward method
                into blocking mode with the specified timeout in input_timeout.
                Defaults to True.
            input_timeout (float, optional): Blocking timeout for the forward
                method. Defaults to 10.

        Returns:
            Pipeline: this pipeline itself
        """
        self.pipeline = StagedThreadPipeline(
            self.stages_sequence, block_input=block_input, input_timeout=input_timeout
        )
        return self

    def forward(self, data: Any) -> bool:
        """Forward data into the pipeline

        Args:
            data (Any): the data. If data none, data generated
                from the input generator will be pushed instead.

        Raises:
            PipelineUndefined: raised if method `serialize` and
                `parallelize` has not been invoked.

        Returns:
            bool: True if the data has been forwarded successfully,
            False otherwise.
        """
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        pipeline_data = self.__generate_pipeline_data(data)
        return self.pipeline.forward(pipeline_data)

    def start_loop(self, period: float = 0.01) -> None:
        """Start the pipeline in autonomous mode. Data generated
        from input generator will be pushed into the pipeline at each
        defined period of time.

        Args:
            period (float, optional): Period to push the data.
                Defaults to 0.01.
        """
        self.__loop_period = period
        self.__loop_is_start.set()
        self.__loop_thread.start()

    def stop_loop(self) -> None:
        """Stop the autonomous operation of the pipeline"""
        self.__loop_is_start.clear()
        self.__loop_thread.join()

    def get_results(self) -> Any:
        """Get latest results from the pipeline

        Raises:
            PipelineUndefined: raised if method `serialize` and
                `parallelize` has not been invoked.

        Returns:
            Any: the last data from the pipeline. The same data cannot be
                read twice. If the new data is not available, None is
                returned.
        """
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        return self.pipeline.get_results().data

    def cleanup(self) -> None:
        """Stop and cleanup the pipeline. Do nothing if the pipeline has not
        been initialized"""
        if self.pipeline is None:
            return
        self.pipeline.cleanup()
        self.pipeline = None

    def __generate_pipeline_data(self, data: Any) -> PipelineData:
        """Handle whether to use input generator or given user data"""
        if data is None:
            return PipelineData(data=self.__input_generator())
        else:
            return PipelineData(data=data)

    def __loop_handler(self) -> None:
        """Function to be run by the input generator thread"""
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        self.__loop_is_start.wait()
        check_period = max(0.001, self.__loop_period / 10)
        while self.__loop_is_start.is_set():
            last_update = time.time()
            data = self.__generate_pipeline_data(None)
            self.pipeline.forward(data)
            while time.time() - last_update < self.__loop_period:
                time.sleep(check_period)
