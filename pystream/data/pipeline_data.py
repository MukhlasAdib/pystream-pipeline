from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class ProfileData:
    started: Dict[str, float] = field(default_factory=dict)
    ended: Dict[str, float] = field(default_factory=dict)


@dataclass
class PipelineData:
    data: Any = None
    profile: ProfileData = field(default_factory=ProfileData)


class InputGeneratorRequest:
    pass


_request_generator = InputGeneratorRequest()
