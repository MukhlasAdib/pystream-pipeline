from typing import Any, Callable, Dict, List, Optional, Tuple

from pystream.data.pipeline_data import (
    InputGeneratorRequest,
    PipelineData,
    _request_generator,
)
from pystream.pipeline import SerialPipeline
from pystream.pipeline import ParallelThreadPipeline
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.pipeline.utils.automation import PipelineAutomation
from pystream.pipeline.utils.profiler import ProfilerHandler
from pystream.stage.stage import Stage, StageCallable
from pystream.utils.errors import PipelineUndefined
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE
from pystream.utils.logger import LOGGER


class Pipeline:
    """The pipeline constructor

    Args:
        input_generator (Optional[Callable[[], Any]], optional): Function that will be
            used to generate input data if you want the pipeline to run autonomously.
            If None, the input needs to be given by invoking "forward" method.
            Defaults to None.
        use_profiler (bool, optional): Whether to implement profiler to the pipeline.
            Defaults to False.
    """

    def __init__(
        self,
        input_generator: Optional[Callable[[], Any]] = None,
        use_profiler: bool = False,
    ) -> None:
        self.stages_sequence: List[StageCallable] = []
        self.stage_names: List[Optional[str]] = []
        self.pipeline: Optional[PipelineBase] = None

        self._input_generator: Callable[[], Any] = lambda: None
        if input_generator is not None:
            self._input_generator = input_generator

        self.profiler = ProfilerHandler() if use_profiler else None
        self._automation = None

    def add(self, stage: StageCallable, name: Optional[str] = None) -> None:
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
            name (Optional[str]): the stage name. If None default stage name will be given,
                i.e. Stage_i where i is the stage sequence number. Defaults to None.
        """
        self.stages_sequence.append(stage)
        self.stage_names.append(name)

    def serialize(self) -> "Pipeline":
        """Turn the pipeline into serial pipeline. All stages will
        be run in sequential and blocking mode.

        Returns:
            Pipeline: this pipeline itself
        """
        self.pipeline = SerialPipeline(
            self.stages_sequence, self.stage_names, profiler_handler=self.profiler
        )
        return self

    def parallelize(
        self,
        block_input: bool = True,
        input_timeout: float = 10,
        block_output: bool = False,
        output_timeout: float = 10,
    ) -> "Pipeline":
        """Turn the pipeline into independent stage pipeline. Each stage
        will live in different thread and work asynchronously. However,
        the data will be passed to the stages in the same order as defined

        Args:
            block_input (bool, optional): Whether to set the `forward` method
                into blocking mode if the first stage is busy with the specified
                timeout in input_timeout. Defaults to True.
            input_timeout (float, optional): Blocking timeout for the `forward`
                method in seconds. Defaults to 10.
            block_output (bool, optional): Whether to set the `get_results` method
                into blocking mode if there is not available data from the last
                stage. Defaults to False.
            output_timeout (float, optional): Blocking timeout for the `get_results`
                method in seconds. Defaults to 10.

        Returns:
            Pipeline: this pipeline itself
        """
        self.pipeline = ParallelThreadPipeline(
            self.stages_sequence,
            self.stage_names,
            block_input=block_input,
            input_timeout=input_timeout,
            block_output=block_output,
            output_timeout=output_timeout,
            profiler_handler=self.profiler,
        )
        return self

    def forward(self, data: Any = _request_generator) -> bool:
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
        pipeline_data = self._generate_pipeline_data(data)
        return self._push_pipeline_data(pipeline_data)

    def start_loop(self, period: float = 0.01) -> None:
        """Start the pipeline in autonomous mode. Data generated
        from input generator will be pushed into the pipeline at each
        defined period of time.

        Args:
            period (float, optional): Period to push the data.
                Defaults to 0.01.
        """
        self._automation = PipelineAutomation(pipeline=self, period=period)
        self._automation.start()

    def stop_loop(self) -> None:
        """Stop the autonomous operation of the pipeline"""
        if self._automation is None:
            return
        self._automation.stop()

    def get_results(self) -> Any:
        """Get latest results from the pipeline

        Raises:
            PipelineUndefined: raised if method `serialize` or
                `parallelize` has not been invoked.

        Returns:
            Any: the last data from the pipeline. The same data cannot be
                read twice. If the new data is not available, None is
                returned.
        """
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        return self.pipeline.get_results().data

    def as_stage(self) -> Stage:
        """Get the base pipeline executor, which can be treated as
        a stage. Useful if you want to create pipeline inside pipeline

        Raises:
            PipelineUndefined: raised if method `serialize` or
                `parallelize` has not been invoked.

        Returns:
            Stage: the base pipeline executor
        """
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        return self.pipeline

    def cleanup(self) -> None:
        """Stop and cleanup the pipeline. Do nothing if the pipeline has not
        been initialized"""
        if self.pipeline is not None:
            self.pipeline.cleanup()
            self.pipeline = None

    def get_profiles(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Get profiles data

        Returns:
            Tuple[Dict[str, float], Dict[str, float]]: dictionary of the latency (in
            seconds) and throughput (in data/second) data respectively. The data is a
            dict where the key is the stage name.
        """
        if self.profiler is None:
            LOGGER.error("Cannot get profiles because profiler is not activated")
            return {}, {}
        return self.profiler.summarize()

    def _generate_pipeline_data(self, data: Any = _request_generator) -> PipelineData:
        """Handle whether to use input generator or given user data"""
        if isinstance(data, InputGeneratorRequest):
            return PipelineData(data=self._input_generator())
        else:
            return PipelineData(data=data)

    def _push_pipeline_data(self, data: PipelineData) -> bool:
        """Push the pipeline data into the pipeline"""
        if self.pipeline is None:
            raise PipelineUndefined("Pipeline has not been defined")
        data.profile.tick_start(_PIPELINE_NAME_IN_PROFILE)
        return self.pipeline.forward(data)
