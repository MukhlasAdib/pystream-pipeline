import sqlite3
from dataclasses import replace
from typing import Dict, Literal, Tuple

import os
import pandas as pd

from pystream.utils.general import _PYSTREAM_DIR
from pystream.data.pipeline_data import ProfileData


class ProfileDBHandler:
    _CREATE_TABLE_QUERY = """
        CREATE TABLE {} (
            data_num INTEGER PRIMARY KEY
        );
        """

    _ADD_COLUMN_QUERY = """
        ALTER TABLE {} 
        ADD COLUMN {} REAL;
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
        self.conn = sqlite3.connect(self.db_path)
        self._create_tables()

        self.column_names = []

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(self._CREATE_TABLE_QUERY.format(self.latency_table))
        cur.execute(self._CREATE_TABLE_QUERY.format(self.throughput_table))

    def put_data(self, latency: Dict[str, float], throughput: Dict[str, float]) -> None:
        self._put_one_table(latency, self.latency_table)
        self._put_one_table(throughput, self.throughput_table)
        self.conn.commit()

    def _put_one_table(self, data: Dict[str, float], table_name: str) -> None:
        for col in data.keys():
            if col not in self.column_names:
                self._add_new_column(col)

        columns = ",".join(data.keys())
        values = ",".join([str(v) for v in data.values()])
        cur = self.conn.cursor()
        cur.execute(self._PUT_DATA_QUERY.format(table_name, columns, values))

    def _add_new_column(self, column_name: str) -> None:
        cur = self.conn.cursor()
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

    def close(self) -> None:
        self.conn.close()


class ProfilerHandler:
    def __init__(self, max_history: int = 100000) -> None:
        """Handler of pipeline profiler

        Args:
            max_history (int, optional): The maximum history to be saved.
                Defaults to 100000.
        """
        self.max_history = max_history

        self.previous_data = ProfileData()
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
        if self.is_first:
            self.previous_data = replace(data)
            self.is_first = False
            return

        latency = self._calculate_latency(data)
        throughput = self._calculate_throughput(data)
        self.previous_data = replace(data)
        self.db_handler.put_data(latency, throughput)

    def _calculate_latency(self, data: ProfileData) -> Dict[str, float]:
        latency = {}
        for stage in data.ended.keys():
            if stage in data.started:
                latency[stage] = data.ended[stage] - data.started[stage]
        return latency

    def _calculate_throughput(self, data: ProfileData) -> Dict[str, float]:
        throughput = {}
        for stage in data.ended.keys():
            if stage in self.previous_data.ended:
                period = data.ended[stage] - self.previous_data.ended[stage]
                throughput[stage] = 1 / period
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

    def cleanup(self) -> None:
        self.db_handler.close()
