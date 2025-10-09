from DbConnector import DbConnector
import pandas as pd
import numpy as np
import json
from scipy.spatial import cKDTree
import csv
import os
from multiprocessing import Pool, cpu_count

class TaxiProximityFinderParallel:
    def __init__(self, chunk_size=100_000, limit=1000, n_workers=None):
        self.connection = DbConnector()
        self.db = self.connection.db_connection
        self.cursor = self.connection.cursor

        self.chunk_size = chunk_size
        self.limit = limit
        self.distance_threshold = 5
        self.time_threshold = 5

        self.partial_file = "pairs_temp.csv"
        if os.path.exists(self.partial_file):
            os.remove(self.partial_file)

        self.output_file = "../sol/taxi_pairs.csv"

        self.n_workers = n_workers or max(1, cpu_count() - 1)

    # ---------- SQL ----------
    def fetch_trips_chunk(self, offset):
        query = f"""
        SELECT t.taxi_id, t.timestamp, ST_AsGeoJSON(p.line) AS line
        FROM trips t
        JOIN polylines p ON t.polyline = p.id
        LIMIT {self.limit} OFFSET {offset};
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        cols = [col[0] for col in self.cursor.description]
        return pd.DataFrame(rows, columns=cols)

    # ---------- Expand ----------
    @staticmethod
    def expand_to_points(df, sample_every=4):
        """Convert polylines to individual points (sampled)."""
        points = []
        for _, row in df.iterrows():
            try:
                line = json.loads(row["line"])
                coords = line["coordinates"]
                for i, (lon, lat) in enumerate(coords[::sample_every]):
                    timestamp = int(row["timestamp"].timestamp()) + i * (15 * sample_every)
                    points.append({
                        "taxi_id": row["taxi_id"],
                        "timestamp": timestamp,
                        "lat": lat,
                        "lon": lon
                    })
            except Exception:
                continue
        return pd.DataFrame(points)

    # ---------- KDTree Worker ----------
    @staticmethod
    def find_close_pairs_chunk(sub_df, distance_threshold, time_threshold):
        """Function executed in parallel to find taxi pairs."""
        R = 6371000
        lat_rad = np.radians(sub_df["lat"].to_numpy())
        lon_rad = np.radians(sub_df["lon"].to_numpy())
        timestamps = sub_df["timestamp"].to_numpy()
        taxi_ids = sub_df["taxi_id"].to_numpy()

        x = R * lon_rad * np.cos(lat_rad)
        y = R * lat_rad
        t_scaled = timestamps * distance_threshold

        pts = np.column_stack((x, y, t_scaled))
        tree = cKDTree(pts)
        neighbors = tree.query_ball_tree(tree, r=distance_threshold)

        found_pairs = set()
        for i, neigh_list in enumerate(neighbors):
            t1 = timestamps[i]
            id1 = taxi_ids[i]
            for j in neigh_list:
                id2 = taxi_ids[j]
                if id1 < id2 and abs(t1 - timestamps[j]) <= time_threshold:
                    found_pairs.add((id1, id2))
        return list(found_pairs)

    # ---------- Main Processing ----------
    def process_all_data(self):
        offset = 0
        chunk_idx = 1
        all_pairs = set()

        with Pool(self.n_workers) as pool:
            while True:
                df_chunk = self.fetch_trips_chunk(offset)
                if df_chunk.empty:
                    break

                print(f"Processing chunk {chunk_idx} (offset={offset}) ...")
                pts_df = self.expand_to_points(df_chunk)
                if pts_df.empty:
                    offset += self.limit
                    chunk_idx += 1
                    continue

                # Subdivide points and prepare tasks
                tasks = []
                for start in range(0, len(pts_df), self.chunk_size):
                    sub_df = pts_df.iloc[start:start + self.chunk_size]
                    tasks.append((sub_df, self.distance_threshold, self.time_threshold))

                # Run in parallel
                results = pool.starmap(TaxiProximityFinderParallel.find_close_pairs_chunk, tasks)

                # Save partial results
                with open(self.partial_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    for res in results:
                        writer.writerows(res)

                offset += self.limit
                chunk_idx += 1

        # Combine unique results
        if os.path.exists(self.partial_file):
            with open(self.partial_file, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    all_pairs.add(tuple(map(int, row)))

        # Save final CSV
        with open(self.output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["taxi_id_1", "taxi_id_2"])
            for pair in sorted(all_pairs):
                writer.writerow(pair)

        return all_pairs

    def close(self):
        self.connection.close_connection()


def main():
    program = TaxiProximityFinderParallel(chunk_size=100_000, limit=1000)
    try:
        pairs = program.process_all_data()
        print(f"Found {len(pairs)} taxi pairs within 5m and 5s.")
    except Exception as e:
        print("ERROR:", e)
    finally:

        program.close()

if __name__ == '__main__':
    main()
