from DbConnector import DbConnector
from tabulate import tabulate

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            SELECT
              t.call_type,
              ROUND(AVG((ST_NumPoints(p.line) - 1) * 15), 2) AS avg_duration_seconds,
              ROUND(AVG(ST_Length(p.line)), 2) AS avg_distance,
              ROUND(SUM(CASE WHEN HOUR(t.timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) / COUNT(*), 4) AS share_00_06,
              ROUND(SUM(CASE WHEN HOUR(t.timestamp) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) / COUNT(*), 4) AS share_06_12,
              ROUND(SUM(CASE WHEN HOUR(t.timestamp) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) / COUNT(*), 4) AS share_12_18,
              ROUND(SUM(CASE WHEN HOUR(t.timestamp) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) / COUNT(*), 4) AS share_18_24
            FROM trips t
            JOIN polylines p ON t.polyline = p.id
            GROUP BY t.call_type
            ORDER BY t.call_type;
        """)
        rows = cursor.fetchall()
        print("Query 4b: Avg duration (s), avg distance, share of trips by time band")
        print(tabulate(rows, headers=[
            "call_type", "avg_duration_s", "avg_distance",
            "share_00_06", "share_06_12", "share_12_18", "share_18_24"
        ]))
        print()

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
