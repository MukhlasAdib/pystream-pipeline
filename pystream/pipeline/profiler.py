from dataclasses import replace
from typing import Dict

from pystream.data.pipeline_data import ProfileData


class ProfilerHandler:
    def __init__(self, max_history: int = 100):
        self.previous_data = ProfileData()
        self.latency_history = []
        self.throughput_history = []
        self.is_first = True

    def process_data(self, data: ProfileData) -> None:
        if self.is_first:
            self.previous_data = replace(data)
            self.is_first = False
            return

        latency = self.calculate_latency(data)
        throughput = self.calculate_throughput(data)
        self.previous_data = replace(data)

    def put_data(self, latency, throughput):
        pass

    def calculate_latency(self, data: ProfileData) -> Dict[str, float]:
        latency = {}
        for stage in data.ended.keys():
            if stage in data.started:
                latency[stage] = data.ended[stage] - data.started[stage]
        return latency

    def calculate_throughput(self, data: ProfileData) -> Dict[str, float]:
        throughput = {}
        for stage in data.ended.keys():
            if stage in self.previous_data.ended:
                throughput[stage] = (
                    data.ended[stage] - self.previous_data.started[stage]
                )
        return throughput
