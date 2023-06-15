import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class TimeProfileData:
    started: Optional[float] = None
    ended: Optional[float] = None
    substage: Dict[str, "TimeProfileData"] = field(default_factory=dict)

    def flatten(self) -> Tuple[List[str], List[Optional[float]], List[Optional[float]]]:
        name_data = ["."]
        start_data = [self.started]
        end_data = [self.ended]

        for stage_name, stage_data in self.substage.items():
            sub_name_data, sub_start_data, sub_end_data = stage_data.flatten()
            sub_name_data = [f".{stage_name}.{name}" for name in sub_name_data]
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
        time_data = find_time_data(self.data, self.current_stages)
        time_data.started = time.perf_counter()

    def tick_end(self):
        time_data = find_time_data(self.data, self.current_stages)
        time_data.ended = time.perf_counter()
        self.current_stages.pop(-1)
