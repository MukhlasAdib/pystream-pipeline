from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ProfileData:
    started: Dict[str, float] = {}
    latencies: Dict[str, float] = {}


@dataclass
class PipelineData:
    data: Any = None
    profile: ProfileData = ProfileData()


class InputGeneratorRequest:
    pass


_request_generator = InputGeneratorRequest()
