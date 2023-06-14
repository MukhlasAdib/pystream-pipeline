import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

from pystream.utils.errors import ProfilingError

TimeData =  Dict[str, Union["TimeData", float]]

def insert_time_data(dict_data: TimeData, stages: List[str]):
        parent_dict = dict_data
        for stage_name in stages[:-1]:
            if not stage_name in parent_dict or isinstance(parent_dict[stage_name], float):
                parent_dict[stage_name] = {}
            sub_dict = parent_dict[stage_name]
            if isinstance(sub_dict, float):
                raise ProfilingError("Found a stage with the same name as a pipeline")
            parent_dict = sub_dict
        parent_dict[stages[-1]] = time.perf_counter()
        return dict_data


@dataclass
class ProfileData:
    started: TimeData = field(default_factory=dict)
    ended: TimeData = field(default_factory=dict)
    current_stages: List[str] = field(default_factory=list)

    def tick_start(self, name: str):
        self.current_stages.append(name)
        insert_time_data(self.started, self.current_stages)

    def tick_end(self):
        insert_time_data(self.ended, self.current_stages)
        self.current_stages.pop(-1)


@dataclass
class PipelineData:
    data: Any = None
    profile: ProfileData = field(default_factory=ProfileData)


class InputGeneratorRequest:
    pass


_request_generator = InputGeneratorRequest()
