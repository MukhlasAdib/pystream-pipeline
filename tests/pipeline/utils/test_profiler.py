import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pytest

import pystream.pipeline.utils.profiler as _profiler
from pystream.data.profiler_data import ProfileData
from pystream.pipeline.utils.profiler import ProfileDBHandler, ProfilerHandler


def generate_test_profile_data(
    num_data: int = 3,
    num_stages: int = 5,
    latency: float = 0.5,
    throughput: float = 4,
    base_time: float = 100,
) -> List[ProfileData]:
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


def generate_test_latency_and_throughput_dict(
    num_data: int = 3,
    num_stages: int = 5,
    latency: float = 0.5,
    throughput: float = 4,
) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    latency_data = [
        {f"Stage{chr(i + 65)}": latency for i in range(num_stages)}
        for _ in range(num_data)
    ]
    throughput_data = [
        {f"Stage{chr(i + 65)}": throughput for i in range(num_stages)}
        for _ in range(num_data)
    ]
    return latency_data, throughput_data


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

    def test_put_data_and_summarize(self):
        latency = 0.53
        throughput = 10
        num_data = 4
        num_stages = 5
        latencies, throughputs = generate_test_latency_and_throughput_dict(
            num_data=num_data,
            num_stages=num_stages,
            latency=latency,
            throughput=throughput,
        )
        for lat, fps in zip(latencies, throughputs):
            self.profiler_db.put_data(lat, fps)

        with sqlite3.connect(self.db_path) as test_conn:
            for table_name in [self.LATENCY_TABLE, self.THROUGHPUT_TABLE]:
                table_df = pd.read_sql_query(
                    f"SELECT * FROM {table_name}", test_conn, dtype=float
                )
                assert table_df.shape[0] == num_data

        sum_lat, sum_fps = self.profiler_db.summarize("mean")
        for stage in latencies[0].keys():
            assert sum_lat[stage] == latency
            assert sum_fps[stage] == throughput

        sum_lat, sum_fps = self.profiler_db.summarize("median")
        for stage in latencies[0].keys():
            assert sum_lat[stage] == latency
            assert sum_fps[stage] == throughput


class TestProfilerHandler:
    @pytest.fixture(autouse=True)
    def _init_profiler(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(_profiler, "_PYSTREAM_DIR", str(tmp_path))
        self.max_history = 100
        self.db_path = os.path.join(str(tmp_path), "user_data", "last_profiles.sqlite")
        self.profiler_handler = ProfilerHandler(max_history=self.max_history)

    def test_init(self):
        assert self.profiler_handler.db_handler.db_path == self.db_path
        os.path.isfile(self.db_path)

    def test_process_data_and_summarize(self):
        num_data = 5
        num_stages = 7
        latency = 0.5
        throughput = 6.8
        data = generate_test_profile_data(
            num_data=num_data,
            num_stages=num_stages,
            latency=latency,
            throughput=throughput,
        )

        for d in data:
            self.profiler_handler.process_data(d)

        with sqlite3.connect(self.db_path) as test_conn:
            for table_name in [
                self.profiler_handler.db_handler.latency_table,
                self.profiler_handler.db_handler.throughput_table,
            ]:
                table_df = pd.read_sql_query(
                    f"SELECT * FROM {table_name}", test_conn, dtype=float
                )
                assert table_df.shape[0] == num_data - 1

        latencies, throughputs = self.profiler_handler.summarize()
        assert len(latencies) == num_stages
        assert len(throughputs) == num_stages
        for k in latencies.keys():
            assert pytest.approx(latencies[k], rel=0.001) == latency
            assert pytest.approx(throughputs[k], rel=0.001) == throughput
