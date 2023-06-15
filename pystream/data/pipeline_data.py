from dataclasses import dataclass, field
from typing import Any

from pystream.data.profiler_data import ProfileData

@dataclass
class PipelineData:
    data: Any = None
    profile: ProfileData = field(default_factory=ProfileData)


class InputGeneratorRequest:
    pass


_request_generator = InputGeneratorRequest()
