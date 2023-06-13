from queue import Empty, Full, Queue
from threading import Event, get_ident, Thread
import time
from typing import List, Optional

from pystream.data.pipeline_data import PipelineData
from pystream.data.stage_data import StageLinks, StageQueueProtocol
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.stage.container import StageContainer
from pystream.stage.final_stage import FinalStage
from pystream.stage.stage import Stage, StageCallable
from pystream.utils.errors import PipelineTerminated
from pystream.utils.logger import LOGGER
from pystream.utils.profiler import ProfilerHandler


def send_output(
    data: PipelineData,
    output_queue: StageQueueProtocol,
    block: bool = True,
    replace: bool = False,
    timeout: float = 10,
) -> bool:
    """Send output to a pipeline queue.

    Args:
        data (PipelineData): data to be sent
        output_queue (Queue): target queue
        block (bool, optional): Whether to wait until the queue
            is empty (wait for 10 seconds). Defaults to True.
        replace (bool, optional): If true, when the queue is full
            (after the specified timeout if block is True), replace
            the data currently in the queue with the given new data.
            Defaults to False.
        timeout (float, optional): Waiting timeout to put data into
            the queue in seconds. Defaults to 10.

    Returns:
        bool: True if the data is successfully sent to the output queue
    """
    try:
        output_queue.put(data, block=block, timeout=timeout)
    except Full:
        if replace:
            output_queue.get(block=False)
            output_queue.put(data, block=False)
            return True
        else:
            return False
    else:
        return True


class StageThread(Thread):
    def __init__(
        self,
        stage: StageCallable,
        links: StageLinks,
        name: str = "Stage",
        all_out: bool = True,
        replace_output: bool = False,
    ) -> None:
        """Thread class for the stage

        Args:
            stage (StageCallable): the stage to be run
            links (StageLinks): the connection module of the stage
            name (str, optional): Name of the thread. Defaults to "Stage".
            all_out (bool, optional): Whether to operate in all out mode,
                i.e. all data that comes in must be send to output.
                Defaults to True.
            replace_output (bool, optional): If true, when the queue is full,
                replace the data currently in the queue with the new data.
                Defaults to False.
        """
        super().__init__(name=name, daemon=True)
        self.stage = stage
        self.links = links
        self.all_out = all_out
        self.output_enabled = True
        self.daemon = True
        self.replace_output = replace_output
        self.send_output_timeout = 10

    def run(self) -> None:
        self.start_thread()
        self.run_loop()

    def start_thread(self):
        self.print_log("Thread started...")
        time.sleep(0.1)
        self.links.starter.set()

    def run_loop(self):
        while not self.links.stopper.is_set():
            try:
                data: PipelineData = self.links.input_queue.get(timeout=1)
            except Empty:
                continue
            data = self.stage(data)
            if self.output_enabled:
                send_output(
                    data,
                    self.links.output_queue,
                    block=self.all_out,
                    replace=self.replace_output,
                    timeout=self.send_output_timeout,
                )
        self.process_cleanup()

    def process_cleanup(self):
        self.print_log(f"Terminating thread...")
        self.links.stopper.set()
        if isinstance(self.stage, Stage):
            self.stage.cleanup()
        while not self.links.input_queue.empty():
            self.links.input_queue.get()
        time.sleep(1)
        self.print_log(f"Thread terminated...")

    def print_log(self, msg: str) -> None:
        LOGGER.debug(f"({self.name} {get_ident()}) {msg}")


class StagedThreadPipeline(PipelineBase):
    def __init__(
        self,
        stages: List[StageCallable],
        names: List[Optional[str]],
        block_input: bool = True,
        input_timeout: float = 10,
        profiler_handler: Optional[ProfilerHandler] = None,
    ) -> None:
        """The class that will handle the parallel pipeline
        based on multi-threading.

        Args:
            stages (List[StageCallable]): The stages to be run
                in sequence.
            names (List[Optional[str]]): Stage names. If the name is None,
                default stage name will be given.
            block_input (bool, optional): Whether to set the forward method
                into blocking mode with the specified timeout in input_timeout.
                Defaults to True.
            input_timeout (float, optional): Blocking timeout for the forward
                method in seconds. Defaults to 10.
            profiler_handler (Optional[ProfilerHandler]): Handler for the profiler.
                If None, no profiling attempt will be done.
        """
        self.final_stage = FinalStage(profiler_handler)
        self.stages: List[Stage] = [
            StageContainer(stage, name) for stage, name in zip(stages, names)
        ]
        self.stages.append(self.final_stage)
        self.block_input = block_input
        self.input_timeout = input_timeout

        self.build_pipeline()
        self.run_pipeline()
        self.results = PipelineData()

    def build_pipeline(self):
        """Build the pipeline."""
        # Create the first link
        self.stopper = Event()
        self.starter = Event()
        # The first stage's input is the output
        # of the pipeline handler
        input_queue = Queue[PipelineData](maxsize=1)
        self.main_output_queue = input_queue
        self.stage_threads: List[StageThread] = []
        self.stage_links: List[StageLinks] = []
        # Create the stage threars one by one along with the links
        for _, stage in enumerate(self.stages):
            output_queue = Queue[PipelineData](maxsize=1)
            links = StageLinks(
                input_queue=input_queue,
                output_queue=output_queue,
                stopper=self.stopper,
                starter=self.starter,
            )
            self.stage_threads.append(StageThread(stage, links, stage.name))
            self.stage_links.append(links)
            input_queue = output_queue
        # Replace output of the last stage to avoid blocking
        self.stage_threads[-1].all_out = False
        self.stage_threads[-1].replace_output = True
        # The last stage's output is the input of the pipeline handler
        self.main_input_queue = input_queue

    def run_pipeline(self):
        """Run the pipeline."""
        for stage, link in zip(self.stage_threads, self.stage_links):
            stage.start()
            link.starter.wait()

    def forward(self, data_input: PipelineData) -> bool:
        """Send data to be processed by pipeline

        Args:
            data_input (PipelineData): the input data

        Raises:
            PipelineTerminated: raised if the pipeline is not active

        Returns:
            bool: True if the data is sent successfully, False if the
                queue is currently full
        """
        if self.stopper.is_set():
            raise PipelineTerminated("The pipeline has been terminated")
        stat = send_output(
            data_input,
            self.main_output_queue,
            block=self.block_input,
            timeout=self.input_timeout,
        )
        return stat

    def get_results(self) -> PipelineData:
        try:
            ret = self.main_input_queue.get(block=False)
        except Empty:
            return PipelineData()
        else:
            return ret

    def cleanup(self) -> None:
        self.stopper.set()
        for proc in self.stage_threads:
            proc.join()
        while not self.main_input_queue.empty():
            self.main_input_queue.get()
        time.sleep(1)
