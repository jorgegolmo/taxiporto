import pandas as pd
from pathlib import Path
from shapely import wkt, geometry

from DbConnector import DbConnector



connection = DbConnector().db_connection

query = """
SELECT trips.trip_id, ST_AsText(polylines.line) AS line
FROM trips
JOIN polylines ON trips.polyline = polylines.id;
"""

df = pd.read_sql(query, connection)

def passed_near(polyline, point):
    line = wkt.loads(polyline)
    return line.distance(point) * 111_000 <= 100 # Convert degrees to meters

cityhall = geometry.Point(-8.62911, 41.15794)
df_near = df[df['line'].apply(lambda lw: passed_near(lw, cityhall))]

df_near[['trip_id']].to_csv(Path(__file__).resolve().parent.parent / "sol" / "6.csv", index=False)
