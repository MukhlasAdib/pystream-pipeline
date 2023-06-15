import os
import sqlite3
from typing import Dict, List, Literal, Tuple

import numpy as np
import pandas as pd

from pystream.data.profiler_data import ProfileData, TimeProfileData
from pystream.utils.errors import ProfilingError
from pystream.utils.general import _PIPELINE_NAME_IN_PROFILE, _PYSTREAM_DIR


class ProfileDBHandler:
    _CREATE_TABLE_QUERY = """
        CREATE TABLE {} (
            data_num INTEGER PRIMARY KEY AUTOINCREMENT
        );
        """

    _ADD_COLUMN_QUERY = """
        ALTER TABLE {} 
        ADD COLUMN "{}" REAL;
        """

    _PUT_DATA_QUERY = """
        INSERT INTO {} ({})
        VALUES({});
        """

    def __init__(self, db_path: str) -> None:
        """Handler of the profile database

        Args:
            db_path (str): path to the SQLite DB
        """
        self.db_path = db_path
        self.latency_table = "Latency"
        self.throughput_table = "Throughput"

        if os.path.isfile(self.db_path):
            os.remove(self.db_path)
        self._create_tables()

        self.column_names = []

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(self._CREATE_TABLE_QUERY.format(self.latency_table))
        cur.execute(self._CREATE_TABLE_QUERY.format(self.throughput_table))

    def put_data(self, names: List[str], latency: np.ndarray, throughput: np.ndarray) -> None:
        conn = self.conn
        self._put_one_table(names, latency, self.latency_table, conn)
        self._put_one_table(names, throughput, self.throughput_table, conn)
        conn.commit()

    def _put_one_table(
        self, names: List[str], data: np.ndarray, table_name: str, conn: sqlite3.Connection
    ) -> None:
        if len(data) == 0:
            return

        for col in names:
            if col not in self.column_names:
                self._add_new_column(col, conn)

        names = [f"\"{name}\"" for name in names]
        columns = ",".join(names)
        values = ",".join([str(v) for v in data])
        cur = conn.cursor()
        cur.execute(self._PUT_DATA_QUERY.format(table_name, columns, values))

    def _add_new_column(self, column_name: str, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute(self._ADD_COLUMN_QUERY.format(self.latency_table, column_name))
        cur.execute(self._ADD_COLUMN_QUERY.format(self.throughput_table, column_name))
        self.column_names.append(column_name)

    def _summarize_table(
        self, table_name: str, stat: Literal["mean", "median"]
    ) -> Dict[str, float]:
        table_df = pd.read_sql_query(
            f"SELECT * FROM {table_name}", self.conn, dtype=float
        )
        if stat == "median":
            summary = table_df.median()
        else:
            summary = table_df.mean()
        summary = summary[1:]
        out = {}
        for col, val in zip(summary.index, summary):
            out[col] = val
        return out

    def summarize(
        self, stat: Literal["mean", "median"] = "mean"
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        latency = self._summarize_table(self.latency_table, stat)
        throughput = self._summarize_table(self.throughput_table, stat)
        return latency, throughput

    @property
    def conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)


class ProfilerHandler:
    def __init__(self, max_history: int = 100000) -> None:
        """Handler of pipeline profiler

        Args:
            max_history (int, optional): The maximum history to be saved.
                Defaults to 100000.
        """
        self.max_history = max_history

        self.previous_end_data = np.array([])
        self.is_first = True

        self.db_folder = "user_data"
        self.db_filename = "last_profiles.sqlite"
        os.makedirs(os.path.join(_PYSTREAM_DIR, self.db_folder), exist_ok=True)

        db_path = os.path.join(_PYSTREAM_DIR, self.db_folder, self.db_filename)
        if os.path.isfile(db_path):
            os.remove(db_path)
        self.db_handler = ProfileDBHandler(db_path)

    def process_data(self, data: ProfileData) -> None:
        """Process pipeline profile data, put them into the DB

        Args:
            data (ProfileData): the pipeline profile data
        """
        name_data, start_data, end_data = self.get_flatten_data(data.data)
        if self.is_first:
            self.previous_end_data = end_data.copy()
            self.is_first = False
            return
        
        latency = self._calculate_latency(start_data, end_data)
        throughput = self._calculate_throughput(end_data)
        self.db_handler.put_data(name_data, latency, throughput)

    def get_flatten_data(
        self, time_data: TimeProfileData
    ) -> Tuple[List[str], np.ndarray, np.ndarray]:
        name_data, start_data, end_data = time_data.flatten()
        if None in start_data:
            raise ProfilingError("Found a None in a profile start record")
        if None in end_data:
            raise ProfilingError("Found a None in a profile end record")
        name_data = [_PIPELINE_NAME_IN_PROFILE + name if name != "__" else _PIPELINE_NAME_IN_PROFILE for name in name_data]
        return name_data, np.array(start_data), np.array(end_data)

    def _calculate_latency(
        self, start_time: np.ndarray, end_time: np.ndarray
    ) -> np.ndarray:
        return np.subtract(end_time, start_time)

    def _calculate_throughput(self, end_data: np.ndarray) -> np.ndarray:
        throughput = np.subtract(end_data, self.previous_end_data)
        throughput = np.divide(1, throughput)
        self.previous_end_data = end_data.copy()
        return throughput

    def summarize(self) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Get the average latency and throughput

        Returns:
            Tuple[Dict[str, float], Dict[str, float]]: dictionary of the latency and
            throughput data respectively. The data is a dict where the key is the
            stage name
        """
        latency, throughput = self.db_handler.summarize()
        return latency, throughput
