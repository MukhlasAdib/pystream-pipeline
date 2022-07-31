from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import Event, get_ident, Thread
import time
from typing import List

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.pipeline_base import PipelineBase
from pystream.stage.stage import Stage, StageCallable


class PipelineTerminated(Exception):
    pass


def send_output(data: PipelineData, output_queue: Queue, block: bool = True) -> bool:
    """Send output to a pipeline queue.

    Args:
        data (PipelineData): data to be sent
        output_queue (Queue): target queue
        block (bool, optional): Whether to wait until the queue
            is empty (wait for 10 seconds). Defaults to True.

    Returns:
        bool: _description_
    """
    try:
        output_queue.put(data, block=block, timeout=10)
    except Full:
        return False
    else:
        return True


@dataclass
class StageLinks:
    """Dataclass for links between stages."""

    # Queue for input data
    input_queue: Queue
    # Queue for output data
    output_queue: Queue
    # Event to stop the stage
    stopper: Event
    # Event to signal the stage is ready
    starter: Event


class StageThread(Thread):
    def __init__(
        self,
        stage: StageCallable,
        links: StageLinks,
        name: str = "Stage",
        all_out: bool = True,
    ) -> None:
        """Thread class for the stage

        Args:
            stage (StageCallable): the stage to be run
            links (StageLinks): the connection module of the stage
            name (str, optional): Name of the thread. Defaults to "Stage".
            all_out (bool, optional): Whether to operate in all out mode,
                i.e. all data that comes in must be send to output.
                Defaults to True.
        """
        super().__init__(name=name, daemon=True)
        self.stage = stage
        self.links = links
        self.all_out = all_out
        self.output_enabled = True
        self.daemon = True

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
            data.data = self.stage(data.data)
            if self.output_enabled:
                send_output(data, self.links.output_queue, block=self.all_out)
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
        print(f"FROM {self.name} ({get_ident()}): {msg}")


class StagedThreadPipeline(PipelineBase):
    def __init__(
        self,
        stages: List[StageCallable],
    ) -> None:
        """The class that will handle the staged pipeline
        based on multi  threading.

        Args:
            stages (List[StageCallable]): The stages to be run
                in sequence.
        """
        self.stages = stages

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
        self.main_output_queue = input_queue = Queue(maxsize=1)
        self.stage_threads: List[StageThread] = []
        self.stage_links: List[StageLinks] = []
        # Create the stage threars one by one along with the links
        for _, stage in enumerate(self.stages):
            output_queue = Queue(maxsize=1)
            links = StageLinks(
                input_queue=input_queue,
                output_queue=output_queue,
                stopper=self.stopper,
                starter=self.starter,
            )
            self.stage_threads.append(StageThread(stage, links, f"PyStream-Stage"))
            self.stage_links.append(links)
            input_queue = output_queue
        # The last stage will not send output to avoid blocking
        # TODO: Handle output of pipeline without blocking
        self.stage_threads[-1].output_enabled = False
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
        stat = send_output(data_input, self.main_output_queue, block=False)
        return stat

    def get_results(self) -> PipelineData:
        """Get output from the last stage

        Returns:
            PipelineData: the obtained data
        """
        try:
            ret = self.main_input_queue.get(block=False)
        except Empty:
            return PipelineData()
        else:
            return ret

    def cleanup(self) -> None:
        """Cleanup the pipeline."""
        self.stopper.set()
        for proc in self.stage_threads:
            proc.join()
        while not self.main_input_queue.empty():
            self.main_input_queue.get()
        time.sleep(1)
