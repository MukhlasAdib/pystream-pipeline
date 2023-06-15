import os
import sqlite3
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import pytest

import pystream.pipeline.utils.profiler as _profiler
from pystream.data.profiler_data import ProfileData, TimeProfileData
from pystream.pipeline.utils.profiler import ProfileDBHandler, ProfilerHandler
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE, _PROFILE_LEVEL_SEPARATOR


def generate_one_cycle_test_profile_data(
    main_time_data: TimeProfileData,
    num_stages: int = 5,
    latency: float = 0.5,
    base_time: float = 100,
    base_name: str = "",
    substage_idx: int = -1,
    num_substages: int = 3,
):
    current = base_time
    main_time_data.started = current
    for i in range(num_stages):
        stage_name = f"{base_name}{i}"
        time_data = TimeProfileData()
        time_data.started = current
        if i == substage_idx:
            _, current = generate_one_cycle_test_profile_data(
                time_data,
                num_stages=num_substages,
                latency=latency,
                base_time=current,
                base_name=f"{i}",
            )
        else:
            current += latency
        time_data.ended = current
        main_time_data.substage[stage_name] = time_data
    main_time_data.ended = current
    return main_time_data, current


def generate_test_profile_data(
    num_data: int = 3,
    num_stages: int = 5,
    latency: float = 0.5,
    throughput: float = 4,
    base_time: float = 100,
    substage_idx: int = 2,
    num_substages: int = 3,
) -> List[ProfileData]:
    current_base = base_time
    data = []
    for _ in range(num_data):
        current_base += 1 / throughput
        main_time_data = TimeProfileData(started=current_base)
        generate_one_cycle_test_profile_data(
            main_time_data=main_time_data,
            num_stages=num_stages,
            latency=latency,
            base_time=current_base,
            substage_idx=substage_idx,
            num_substages=num_substages,
        )
        data.append(ProfileData(main_time_data))
    return data


def generate_test_latency_and_throughput_dict(
    num_data: int = 3,
    num_stages: int = 5,
    latency: float = 0.5,
    throughput: float = 4,
) -> Tuple[List[List[str]], List[np.ndarray], List[np.ndarray]]:
    names_data = [f"{i}" for i in range(num_stages)]
    latency_data = np.array([latency for _ in range(num_stages)])
    throughput_data = np.array([throughput for _ in range(num_stages)])
    return (
        [names_data for _ in range(num_data)],
        [latency_data for _ in range(num_data)],
        [throughput_data for _ in range(num_data)],
    )


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

        names, latencies, throughputs = generate_test_latency_and_throughput_dict(
            num_data=num_data,
            num_stages=num_stages,
            latency=latency,
            throughput=throughput,
        )
        for i in range(len(names)):
            self.profiler_db.put_data(names[i], latencies[i], throughputs[i])

        with sqlite3.connect(self.db_path) as test_conn:
            for table_name in [self.LATENCY_TABLE, self.THROUGHPUT_TABLE]:
                table_df = pd.read_sql_query(
                    f"SELECT * FROM {table_name}", test_conn, dtype=float
                )
                assert table_df.shape[0] == num_data

        sum_lat, sum_fps = self.profiler_db.summarize("mean")
        for stage in names[0]:
            assert sum_lat[stage] == latency
            assert sum_fps[stage] == throughput

        sum_lat, sum_fps = self.profiler_db.summarize("median")
        for stage in names[0]:
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
        substage_idx = 2
        num_substages = 3

        data = generate_test_profile_data(
            num_data=num_data,
            num_stages=num_stages,
            latency=latency,
            throughput=throughput,
            substage_idx=substage_idx,
            num_substages=num_substages,
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
        assert len(latencies) == num_stages + 1 + num_substages
        assert len(throughputs) == num_stages + 1 + num_substages
        for k in latencies.keys():
            assert k.startswith(_PIPELINE_NAME_IN_PROFILE)
            if k == _PIPELINE_NAME_IN_PROFILE:
                assert pytest.approx(latencies[k], rel=0.001) == latency * (
                    num_stages + num_substages - 1
                )
            elif (
                k
                == f"{_PIPELINE_NAME_IN_PROFILE}{_PROFILE_LEVEL_SEPARATOR}{substage_idx}"
            ):
                assert pytest.approx(latencies[k], rel=0.001) == latency * num_substages
            else:
                assert pytest.approx(latencies[k], rel=0.001) == latency
            assert pytest.approx(throughputs[k], rel=0.001) == throughput
