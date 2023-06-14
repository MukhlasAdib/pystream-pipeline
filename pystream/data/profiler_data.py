import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TimeProfileData:
    value: Optional[float] = None
    substage: Dict[str, "TimeProfileData"] = field(default_factory=dict)


def insert_time_data(time_data: TimeProfileData, stages: List[str]):
    parent_data = time_data
    for stage_name in stages:
        if not stage_name in parent_data.substage:
            parent_data.substage[stage_name] = TimeProfileData()
        parent_data = parent_data.substage[stage_name]
    parent_data.value= time.perf_counter()
    return time_data


@dataclass
class ProfileData:
    started: TimeProfileData = field(default_factory=TimeProfileData)
    ended: TimeProfileData = field(default_factory=TimeProfileData)
    current_stages: List[str] = field(default_factory=list)

    def tick_start(self, name: str):
        self.current_stages.append(name)
        insert_time_data(self.started, self.current_stages)

    def tick_end(self):
        insert_time_data(self.ended, self.current_stages)
        self.current_stages.pop(-1)