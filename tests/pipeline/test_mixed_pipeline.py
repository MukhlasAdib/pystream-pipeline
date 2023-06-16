import time

import pytest

from pystream import Pipeline, Stage
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE, _PROFILE_LEVEL_SEPARATOR
from pystream.pipeline import SerialPipeline
from pystream.pipeline import ParallelThreadPipeline


class TestMixedPipeline:
    def _construct_pipeline(
        self,
        dummy_stage,
        num_stages=3,
        wait_time=0.1,
        mode="serial",
        child_idx=-1,
        num_child_stages=2,
        child_mode="thread",
        parent_name="",
    ):
        stages = {}
        child_stages = {}
        pipeline = Pipeline(input_generator=list, use_profiler=True)
        for i in range(num_stages):
            name = f"{parent_name}{i}"
            if i == child_idx:
                child_pipeline, child_stages, _ = self._construct_pipeline(
                    dummy_stage,
                    num_stages=num_child_stages,
                    wait_time=wait_time,
                    parent_name=name,
                    mode=child_mode,
                )
                dummy = child_pipeline.as_stage()
            else:
                dummy = dummy_stage(val=name, wait=wait_time)
                stages[name] = dummy
            pipeline.add(dummy, name)
        if mode == "serial":
            pipeline.serialize()
        elif mode == "thread":
            pipeline.parallelize()
        return pipeline, stages, child_stages

    def assert_forward_and_get_results(
        self, pipeline: Pipeline, loop_period, num_stages, child_idx, num_child_stages
    ):
        pipeline.start_loop(loop_period)
        time.sleep(2)
        ret = pipeline.get_results()
        lat, fps = pipeline.get_profiles()
        for i in range(num_stages):
            stage_name = f"{i}"
            profile_name = (
                f"{_PIPELINE_NAME_IN_PROFILE}{_PROFILE_LEVEL_SEPARATOR}{stage_name}"
            )
            assert profile_name in lat
            assert profile_name in fps
            assert lat[profile_name] > 0
            assert fps[profile_name] > 0

            if i == child_idx:
                for j in range(num_child_stages):
                    child_stage_name = f"{i}{j}"
                    child_profile_name = (
                        f"{profile_name}{_PROFILE_LEVEL_SEPARATOR}{child_stage_name}"
                    )
                    assert child_stage_name in ret
                    assert child_profile_name in lat
                    assert child_profile_name in fps
                    assert lat[child_profile_name] > 0
                    assert fps[child_profile_name] > 0
            else:
                assert stage_name in ret


class TestSerialInThread(TestMixedPipeline):
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage):
        self.num_stages = 3
        self.wait_time = 0.1
        self.mode = "thread"
        self.child_idx = 1
        self.num_child_stages = 2
        self.child_mode = "serial"

        self.pipeline, self.stages, self.child_stages = self._construct_pipeline(
            dummy_stage,
            num_stages=self.num_stages,
            wait_time=self.wait_time,
            mode=self.mode,
            child_idx=self.child_idx,
            num_child_stages=self.num_child_stages,
            child_mode=self.child_mode,
        )

    def test_init_parent(self):
        assert self.pipeline.pipeline is not None
        assert isinstance(self.pipeline.pipeline, ParallelThreadPipeline)
        assert len(self.pipeline.pipeline.stages) == self.num_stages + 1
        actual_stages = [s.name for s in self.pipeline.pipeline.stages]
        for k, v in self.stages.items():
            assert k in actual_stages
            assert isinstance(v, Stage)

    def test_init_child(self):
        assert isinstance(self.pipeline.pipeline, ParallelThreadPipeline)
        child_pipeline = self.pipeline.pipeline.stages[self.child_idx].stage  # type: ignore
        assert isinstance(child_pipeline, SerialPipeline)
        assert len(child_pipeline.pipeline) == self.num_child_stages + 1
        actual_stages = [s.name for s in child_pipeline.pipeline]
        for k, v in self.child_stages.items():
            assert k in actual_stages
            assert isinstance(v, Stage)

    def test_forward_and_get_results(self):
        self.assert_forward_and_get_results(
            self.pipeline,
            loop_period=self.wait_time,
            num_stages=self.num_stages,
            child_idx=self.child_idx,
            num_child_stages=self.num_child_stages,
        )
