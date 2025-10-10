from DbConnector import DbConnector
from tabulate import tabulate

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            SELECT
              t.taxi_id,
              ROUND(SUM((ST_NumPoints(p.line) - 1) * 15) / 3600, 4) AS total_hours,
              ROUND(SUM(ST_Length(p.line)), 2) AS total_distance
            FROM trips t
            JOIN polylines p ON t.polyline = p.id
            GROUP BY t.taxi_id
            ORDER BY total_hours DESC
            LIMIT 20;
        """)
        rows = cursor.fetchall()
        print("Query 5: Taxis with most total hours driven and total distance")
        print(tabulate(rows, headers=["taxi_id", "total_hours", "total_distance"]))
        print()

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
