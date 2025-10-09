from DbConnector import DbConnector
from tabulate import tabulate

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            SELECT taxi_id, COUNT(*) AS trip_count
            FROM trips
            GROUP BY taxi_id
            ORDER BY trip_count DESC
            LIMIT 20;
        """)
        rows = cursor.fetchall()
        print("Query 3: Top 20 taxis with most trips")
        print(tabulate(rows, headers=["taxi_id", "trip_count"]))
        print()

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
