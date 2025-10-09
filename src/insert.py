from pathlib import Path

from DbConnector import DbConnector
from shapely.geometry import LineString

import pandas as pd
import json


class Inserter:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def insert(self, path):
        # Load CSV
        df = pd.read_csv(path)
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

        for _, row in df.iterrows():
            # Convert POLYLINE JSON to LINESTRING
            coords = json.loads(row["POLYLINE"])
            if len(coords) < 2:
                continue
            line_wkt = LineString(coords).wkt

            # Insert into polylines
            self.cursor.execute(
                "INSERT INTO polylines (missing_data, line) VALUES (%s, ST_GeomFromText(%s, 4326))",
                (row["MISSING_DATA"], line_wkt)
            )
            polyline_id = self.cursor.lastrowid

            # Insert into trips
            self.cursor.execute(
                """
                INSERT INTO trips (call_type, origin_call, origin_stand, taxi_id, timestamp, polyline)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    row["CALL_TYPE"],
                    row["ORIGIN_CALL"] if not pd.isna(row["ORIGIN_CALL"]) else None,
                    row["ORIGIN_STAND"] if not pd.isna(row["ORIGIN_STAND"]) else None,
                    row["TAXI_ID"],
                    row["TIMESTAMP"],
                    polyline_id
                )
            )

        self.db_connection.commit()
        print("Finished inserting CSV data.")

    def close(self):
        self.connection.close_connection()


def main():
    inserter = Inserter()
    try:
        inserter.insert(Path(__file__).resolve().parent.parent / "dat" / "porto.csv")
    finally:
        inserter.close()


if __name__ == "__main__":
    main()
