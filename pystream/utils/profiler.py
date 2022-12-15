from dataclasses import replace
import os
import sqlite3
from typing import Dict

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
        values = ",".join(data.keys())
        cur = self.conn.cursor()
        cur.execute(self._ADD_COLUMN_QUERY.format(table_name, columns, values))

    def _add_new_column(self, column_name: str) -> None:
        cur = self.conn.cursor()
        cur.execute(self._ADD_COLUMN_QUERY.format(self.latency_table, column_name))
        cur.execute(self._ADD_COLUMN_QUERY.format(self.throughput_table, column_name))
        self.column_names.append(column_name)

    def close(self) -> None:
        self.conn.close()


class ProfilerHandler:
    def __init__(self, max_history: int = 100000) -> None:
        self.max_history = max_history

        self.previous_data = ProfileData()
        self.latency_history = []
        self.throughput_history = []
        self.is_first = True

        db_path = os.path.join(_PYSTREAM_DIR, "user_data", "last_profiles.db")
        self.db_handler = ProfileDBHandler(db_path)

    def process_data(self, data: ProfileData) -> None:
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
                throughput[stage] = (
                    data.ended[stage] - self.previous_data.started[stage]
                )
        return throughput

    def cleanup(self):
        self.db_handler.close()
