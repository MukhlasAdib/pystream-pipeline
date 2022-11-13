import time

import pytest

from pystream import Stage


class DummyStage(Stage):
    def __init__(self):
        self.val = None
        self.wait = 0.5

    def __call__(self, data: list) -> list:
        time.sleep(self.wait)
        data.append(self.val)
        return data

    def cleanup(self) -> None:
        self.val = None


@pytest.fixture
def dummy_stage():
    return DummyStage
