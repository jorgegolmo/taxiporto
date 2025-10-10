from DbConnector import DbConnector

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            SELECT AVG(trip_count) FROM (
                SELECT COUNT(*) AS trip_count
                FROM trips
                GROUP BY taxi_id
            ) AS sub;
        """)
        avg_trips = cursor.fetchone()[0]
        print("Query 2: Average number of trips per taxi")
        print(avg_trips, "\n")

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
