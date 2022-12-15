from pystream.data.pipeline_data import PipelineData
from pystream.stage.stage import Stage
from pystream.utils.profiler import ProfilerHandler


class FinalStage(Stage):
    def __init__(self):
        self.profiler_handler = ProfilerHandler()

    def __call__(self, data: PipelineData) -> PipelineData:
        self.profiler_handler.process_data(data.profile)
        return data

    def cleanup(self) -> None:
        self.profiler_handler.cleanup()
