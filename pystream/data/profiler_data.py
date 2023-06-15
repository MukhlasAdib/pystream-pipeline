import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE, _PROFILE_LEVEL_SEPARATOR


@dataclass
class TimeProfileData:
    started: Optional[float] = None
    ended: Optional[float] = None
    substage: Dict[str, "TimeProfileData"] = field(default_factory=dict)

    def flatten(self) -> Tuple[List[str], List[Optional[float]], List[Optional[float]]]:
        name_data = [f"{_PROFILE_LEVEL_SEPARATOR}"]
        start_data = [self.started]
        end_data = [self.ended]

        for stage_name, stage_data in self.substage.items():
            sub_name_data, sub_start_data, sub_end_data = stage_data.flatten()
            sub_name_data = [
                f"{_PROFILE_LEVEL_SEPARATOR}{stage_name}{name}"
                if name != f"{_PROFILE_LEVEL_SEPARATOR}"
                else f"{_PROFILE_LEVEL_SEPARATOR}{stage_name}"
                for name in sub_name_data
            ]
            name_data.extend(sub_name_data)
            start_data.extend(sub_start_data)
            end_data.extend(sub_end_data)
        return name_data, start_data, end_data


def find_time_data(time_data: TimeProfileData, stages: List[str]) -> TimeProfileData:
    parent_data = time_data
    for stage_name in stages:
        if not stage_name in parent_data.substage:
            parent_data.substage[stage_name] = TimeProfileData()
        parent_data = parent_data.substage[stage_name]
    return parent_data


@dataclass
class ProfileData:
    data: TimeProfileData = field(default_factory=TimeProfileData)
    current_stages: List[str] = field(default_factory=list)

    def tick_start(self, name: str):
        self.current_stages.append(name)
        if name == _PIPELINE_NAME_IN_PROFILE and len(self.current_stages) == 1:
            time_data = self.data
        else:
            time_data = find_time_data(self.data, self.current_stages)
        time_data.started = time.perf_counter()

    def tick_end(self):
        if len(self.current_stages) == 1:
            time_data = self.data
        else:
            time_data = find_time_data(self.data, self.current_stages)
        time_data.ended = time.perf_counter()
        self.current_stages.pop(-1)
