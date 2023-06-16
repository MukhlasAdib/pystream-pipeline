import pytest

from pystream import Pipeline, Stage
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
        pipeline = Pipeline()
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
        child_pipeline = self.pipeline.pipeline.stages[self.child_idx].stage # type: ignore
        assert isinstance(child_pipeline, SerialPipeline)
        assert len(child_pipeline.pipeline) == self.num_child_stages + 1
        actual_stages = [s.name for s in child_pipeline.pipeline]
        for k, v in self.child_stages.items():
            assert k in actual_stages
            assert isinstance(v, Stage)
            
