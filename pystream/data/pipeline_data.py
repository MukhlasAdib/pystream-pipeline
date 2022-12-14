from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class ProfileData:
    started: Dict[str, float] = field(default_factory=dict)
    latencies: Dict[str, float] = field(default_factory=dict)


@dataclass
class PipelineData:
    data: Any = None
    profile: ProfileData = ProfileData()


class InputGeneratorRequest:
    pass


_request_generator = InputGeneratorRequest()
