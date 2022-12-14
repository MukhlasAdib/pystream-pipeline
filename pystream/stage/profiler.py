import time

from pystream.data.pipeline_data import ProfileData


def start_measurement(stage_name: str, profile_data: ProfileData) -> None:
    profile_data.started[stage_name] = time.perf_counter()


def end_measurement(stage_name: str, profile_data: ProfileData) -> None:
    if stage_name in profile_data.started:
        profile_data.latencies[stage_name] = (
            time.perf_counter() - profile_data.started[stage_name]
        )
