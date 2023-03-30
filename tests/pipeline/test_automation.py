import time

import pytest

from pystream.data.pipeline_data import PipelineData
from pystream.pipeline.automation import PipelineAutomation


class MockInterfacePipeline:
    def __init__(self):
        self.times = []
        self.data_hist = []
        self.data_count = 0

    def _push_pipeline_data(self, data=PipelineData(None)):
        self.times.append(time.time())
        self.data_hist.append(data.data)
        return True

    def _generate_pipeline_data(self, data=None):
        self.data_count += 1
        return PipelineData(data=self.data_count)


class TestPipelineAutomation:
    @pytest.fixture(autouse=True)
    def _create_automation(self):
        self.period = 0.5
        self.mock_inteface_pipeline = MockInterfacePipeline()
        self.automation = PipelineAutomation(
            pipeline=self.mock_inteface_pipeline, period=self.period
        )

    def test_init(self):
        assert self.automation.pipeline == self.mock_inteface_pipeline
        assert self.automation._loop_period == self.period

    def test_start_stop(self):
        self.automation.start()
        assert self.automation._loop_is_start.is_set()
        assert self.automation._loop_thread.is_alive()
        time.sleep(2)
        self.automation.stop()
        assert not self.automation._loop_is_start.is_set()
        assert not self.automation._loop_thread.is_alive()

        assert self.mock_inteface_pipeline.data_count > 0
        assert self.mock_inteface_pipeline.data_count == len(
            self.mock_inteface_pipeline.times
        )
        assert len(self.mock_inteface_pipeline.data_hist) == len(
            self.mock_inteface_pipeline.times
        )
        times = self.mock_inteface_pipeline.times
        data_hist = self.mock_inteface_pipeline.data_hist
        for i in range(len(times)):
            # make sure the data order is correct
            assert data_hist[i] == i + 1
            if i + 1 < len(times):
                delta = times[i + 1] - times[i]
                # make sure the cycle period is within 10% error
                assert pytest.approx(self.period, rel=0.1) == delta
