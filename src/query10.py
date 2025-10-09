import pandas as pd
from pathlib import Path

from DbConnector import DbConnector



connection = DbConnector().db_connection

query = """
SELECT trip_id
FROM trips
JOIN polylines ON trips.polyline = polylines.id
WHERE ST_Distance_Sphere(
        ST_StartPoint(polylines.line),
        ST_EndPoint(polylines.line)
      ) <= 50;
"""

df = pd.read_sql(query, connection)

df.to_csv(Path(__file__).resolve().parent.parent / "sol" / "10.csv", index=False)
