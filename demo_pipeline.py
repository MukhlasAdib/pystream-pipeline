"""
Demonstration of how PyStream can be used
This demo will run dummy pipeline that performs
some convolutions and makes a simple modification 
to a dictionary data.
"""

import functools
import time
from typing import Any, Callable, Dict, List, Union

import cv2
import numpy as np


from pystream import Pipeline, Stage
from pystream.functional import (
    func_serial,
    func_parallel_thread,
)

# Cycle period for the input data
INPUT_PERIOD = 0.2
# Time to run the pipeline
ON_TIME = 5
# An alias for the pipeline data type
MyData = Dict[str, Union[int, float]]


class DummyStage(Stage):
    """A dummy stage that performs some convolutions and modify dictionary given to it.
    If the stage is an instance, it has to be a subclass of Stage class (an abstract class)
    and defined the "__call__" and "cleanup" methods."""

    def __init__(self, name: str, measure: bool = False):
        """State init

        Args:
            name (str): name of stage
            measure (bool, optional): Whether we want to measure throughput
                of this stage. Defaults to False.
        """
        self.name = name
        self.measure = measure
        self.last_update = -1

    def __call__(self, data: MyData) -> MyData:
        """Main data processing of the stage,
        a mandatory method of the Stage class.

        Args:
            data (MyData): the input data

        Returns:
            MyData: the output data
        """
        img = np.random.randint(0, 255, size=(480, 720, 3), dtype=np.uint8)
        kernel = np.random.randint(-10, 10, size=(5, 5))
        for _ in range(100):
            img = cv2.filter2D(src=img, ddepth=-1, kernel=kernel)

        if self.name in data:
            data[self.name] += 1
        else:
            data[self.name] = 1
        if not self.measure:
            return data

        if self.last_update < 0:
            self.last_update = time.time()
        inter_time = time.time() - self.last_update
        data["throughput"] = 1 / inter_time if inter_time != 0 else 0
        self.last_update = time.time()
        return data

    def cleanup(self):
        """Cleanup method, called at the end of the pipeline.
        Currently does nothing but this is a mandatory method
        of the Stage class.
        """
        pass


def last_stage(data: MyData) -> MyData:
    """The last stage of the pipeline which prints the final data.
    This stage is not a class, but a callable function.
    If the stage is a callable, it has to take one argument, the data,
    and returns the output data of the same type.

    Args:
        data (MyData): the input data

    Returns:
        MyData: the output data
    """
    latency = time.time() - data["start"]
    if "throughput" in data:
        throughput = data["throughput"]
    else:
        throughput = 0
    print(f"Received (latency {latency} s) ; throughput {throughput} data/s")
    data.pop("start")
    data.pop("throughput")
    print(data)
    print()
    return data


def create_pipeline() -> Pipeline:
    """Create the dummy pipeline

    Returns:
        Pipeline: the pipeline
    """
    # Instantiate the pipeline object. The input generator will generate
    # a dictionary with a timestamp data in it each time it is called by
    # the pipeline.
    pipeline = Pipeline(input_generator=lambda: {"start": time.time()})
    # Now, add 5 dummy stages to the pipeline.
    pipeline.add(DummyStage("Stage1"))
    pipeline.add(DummyStage("Stage2"))
    pipeline.add(DummyStage("Stage3"))
    pipeline.add(DummyStage("Stage4"))
    # The fifth stage will do throughput measurement.
    pipeline.add(DummyStage("Stage5", measure=True))
    # Add the last stage that will print data to terminal
    pipeline.add(last_stage)
    return pipeline


def staged_serial():
    """Demonstration of pipeline in serial mode"""
    pipeline = create_pipeline()
    print("Starting pipeline in serial...")
    # Set the pipeline in serial mode
    pipeline.serialize()
    # Start the loop, which will make the pipeline
    # call the input generator at each given period
    pipeline.start_loop(INPUT_PERIOD)
    time.sleep(ON_TIME)
    print("Stopping pipeline...")
    # Stop the loop and cleanup the pipeline
    pipeline.stop_loop()
    pipeline.cleanup()


def staged_parallel():
    """Demonstration of pipeline is parallel mode.
    The workflow is the same as th serial one"""
    pipeline = create_pipeline()
    print("Starting pipeline in parallel...")
    pipeline.parallelize()
    pipeline.start_loop(INPUT_PERIOD)
    time.sleep(ON_TIME)
    print("Stopping pipeline...")
    pipeline.stop_loop()
    pipeline.cleanup()


def get_functional_pipeline(data: MyData) -> List[Callable[[], Any]]:
    """Get the pipeline in functional form. Because the data will be wrapped
    with the stages as partial functions, we might need to build the pipeline
    at each execution to ensure independency between each execution.

    Args:
        data (MyData): data to be injected

    Returns:
        List[Callable[[], Any]]: pipleine constructor, a list of stage callables
    """
    # Make the stages, and then wrap the stage and the data as a partial function
    func_stage1 = functools.partial(DummyStage("Stage1"), data)
    func_stage2 = functools.partial(DummyStage("Stage2"), data)
    func_stage3 = functools.partial(DummyStage("Stage3"), data)
    func_stage4 = functools.partial(DummyStage("Stage4"), data)
    func_stage5 = functools.partial(DummyStage("Stage5", measure=True), data)
    func_output = functools.partial(last_stage, data)
    return [
        func_stage1,
        func_stage2,
        func_stage3,
        func_stage4,
        func_stage5,
        func_output,
    ]


def functional_serial():
    print("Starting pipeline in functional serial mode...")
    for _ in range(10):
        data = {"start": time.time()}
        stages = get_functional_pipeline(data)
        func_pipeline = func_serial(stages)
        func_pipeline()
        time.sleep(INPUT_PERIOD)


def functional_thread():
    print("Starting pipeline in functional parallel thread mode...")
    for _ in range(10):
        data = {"start": time.time()}
        stages = get_functional_pipeline(data)
        func_pipeline_no_output = func_parallel_thread(stages[:-1])
        func_pipeline = func_serial([func_pipeline_no_output, stages[-1]])
        func_pipeline()
        time.sleep(INPUT_PERIOD)


if __name__ == "__main__":
    staged_serial()
    print()
    staged_parallel()
    print()
    functional_serial()
    print()
    functional_thread()
