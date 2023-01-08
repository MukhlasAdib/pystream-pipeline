import sqlite3
import time
from pathlib import Path

import pandas as pd
import pytest

from pystream.data.pipeline_data import ProfileData
from pystream.utils.profiler import ProfileDBHandler, ProfilerHandler


def generate_test_profile_data(
    num_data: int = 3,
    num_stages: int = 5,
    latency: float = 0.5,
    throughput: float = 4,
    base_time: float = 100,
):
    current_base = base_time
    stages_name = [f"Stage{chr(i + 65)}" for i in range(num_stages)]
    data = []
    for _ in range(num_data):
        current_base += 1 / throughput
        current = current_base
        started = {}
        ended = {}
        for stage in stages_name:
            started[stage] = current
            current += latency
            ended[stage] = current
        data.append(ProfileData(started=started, ended=ended))
    return data


def test_profiler():
    profiler_handler = ProfilerHandler()
    data = generate_test_profile_data(3, 5, 0.5, 4)
    for d in data:
        profiler_handler.process_data(d)
    profiler_handler.summarize()


class TestProfileDBHandler:
    LATENCY_TABLE = "Latency"
    THROUGHPUT_TABLE = "Throughput"
    INDEX_COLUMN_NAME = "data_num"
    TEST_DB_NAME = "test_db.sqlite"

    @pytest.fixture(autouse=True)
    def _init_profiler_db(self, tmp_path: Path):
        self.db_path = tmp_path / self.TEST_DB_NAME
        self.profiler_db = ProfileDBHandler(str(self.db_path))

    def test_init(self):
        assert len(self.profiler_db.column_names) == 0
        assert self.LATENCY_TABLE == self.profiler_db.latency_table
        assert self.THROUGHPUT_TABLE == self.profiler_db.throughput_table

        with sqlite3.connect(self.db_path) as test_conn:
            for table_name in [self.LATENCY_TABLE, self.THROUGHPUT_TABLE]:
                table_df = pd.read_sql_query(
                    f"SELECT * FROM {table_name}", test_conn, dtype=float
                )
                assert table_df.shape[0] == 0
                assert table_df.columns == [self.INDEX_COLUMN_NAME]
