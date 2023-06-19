import time
from pathlib import Path
from typing import Type

import pytest

from pystream import Pipeline, Stage
from pystream.utils.general import (
    _PIPELINE_NAME_IN_PROFILE,
    _PROFILE_LEVEL_SEPARATOR,
    set_profiler_db_folder,
)
from pystream.pipeline import SerialPipeline
from pystream.pipeline import ParallelThreadPipeline


class MixedPipelineTester:
    num_stages = 3
    wait_time = 0.1
    mode = ParallelThreadPipeline
    child_idx = 1
    num_child_stages = 2
    child_mode = SerialPipeline

    def _init_tester(self, dummy_stage, tmp_path: Path):
        set_profiler_db_folder(str(tmp_path))
        self.pipeline, self.stages, self.child_stages = self._construct_pipeline(
            dummy_stage,
            num_stages=self.num_stages,
            wait_time=self.wait_time,
            mode=self.mode,
            child_idx=self.child_idx,
            num_child_stages=self.num_child_stages,
            child_mode=self.child_mode,
            use_profiler=True,
        )

    def _construct_pipeline(
        self,
        dummy_stage,
        num_stages: int = 3,
        wait_time: float = 0.1,
        mode: Type = SerialPipeline,
        child_idx: int = -1,
        num_child_stages: int = 2,
        child_mode: Type = ParallelThreadPipeline,
        parent_name: str = "",
        use_profiler: bool = False,
    ):
        stages = {}
        child_stages = {}
        pipeline = Pipeline(input_generator=list, use_profiler=use_profiler)
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
        if mode is SerialPipeline:
            pipeline.serialize()
        elif mode is ParallelThreadPipeline:
            pipeline.parallelize(block_output=True, output_timeout=10)
        return pipeline, stages, child_stages

    def test_forward_and_get_results(self):
        self.pipeline.start_loop(self.wait_time)
        time.sleep(2)
        ret = self.pipeline.get_results()
        lat, fps = self.pipeline.get_profiles()
        for i in range(self.num_stages):
            stage_name = f"{i}"
            profile_name = (
                f"{_PIPELINE_NAME_IN_PROFILE}{_PROFILE_LEVEL_SEPARATOR}{stage_name}"
            )
            assert profile_name in lat
            assert profile_name in fps
            assert lat[profile_name] > 0
            assert fps[profile_name] > 0

            if i == self.child_idx:
                for j in range(self.num_child_stages):
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

    def test_cleanup(self):
        self.pipeline.cleanup()
        for s in list(self.stages.values()) + list(self.child_stages.values()):
            assert s.val is None

    def test_init_parent(self):
        assert self.pipeline.pipeline is not None
        assert isinstance(self.pipeline.pipeline, self.mode)
        assert len(self.pipeline.pipeline.stages) == self.num_stages + 1
        actual_stages = [s.name for s in self.pipeline.pipeline.stages]
        for k, v in self.stages.items():
            assert k in actual_stages
            assert isinstance(v, Stage)

    def test_init_child(self):
        assert isinstance(self.pipeline.pipeline, self.mode)
        child_pipeline = self.pipeline.pipeline.stages[self.child_idx].stage  # type: ignore
        assert isinstance(child_pipeline, self.child_mode)
        assert len(child_pipeline.stages) == self.num_child_stages + 1
        actual_stages = [s.name for s in child_pipeline.stages]
        for k, v in self.child_stages.items():
            assert k in actual_stages
            assert isinstance(v, Stage)


class TestSerialInThread(MixedPipelineTester):
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage, tmp_path: Path):
        set_profiler_db_folder(str(tmp_path))
        self.num_stages = 3
        self.wait_time = 0.1
        self.mode = ParallelThreadPipeline
        self.child_idx = 1
        self.num_child_stages = 2
        self.child_mode = SerialPipeline
        self._init_tester(dummy_stage, tmp_path)


class TestThreadInSerial(MixedPipelineTester):
    @pytest.fixture(autouse=True)
    def _create_pipeline(self, dummy_stage, tmp_path: Path):
        set_profiler_db_folder(str(tmp_path))
        self.num_stages = 3
        self.wait_time = 0.1
        self.mode = SerialPipeline
        self.child_idx = 1
        self.num_child_stages = 2
        self.child_mode = ParallelThreadPipeline
        self._init_tester(dummy_stage, tmp_path)
