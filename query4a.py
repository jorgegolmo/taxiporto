from DbConnector import DbConnector
from tabulate import tabulate

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            WITH counts AS (
              SELECT
                taxi_id,
                call_type,
                COUNT(*) AS trips,
                ROW_NUMBER() OVER (PARTITION BY taxi_id ORDER BY COUNT(*) DESC) AS rn
              FROM trips
              GROUP BY taxi_id, call_type
            )
            SELECT taxi_id, call_type AS most_used_call, trips
            FROM counts
            WHERE rn = 1
            ORDER BY taxi_id;
        """)
        rows = cursor.fetchall()
        print("Query 4a: Most used call type per taxi")
        print(tabulate(rows, headers=["taxi_id", "most_used_call", "trips"]))
        print()

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
