from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class PipelineData:
    data: Optional[Any] = None


class InputGeneratorRequest:
    pass
