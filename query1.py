from DbConnector import DbConnector

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        # Total distinct taxis
        cursor.execute("SELECT COUNT(DISTINCT taxi_id) FROM trips;")
        total_taxis = cursor.fetchone()[0]

        # Total trips
        cursor.execute("SELECT COUNT(*) FROM trips;")
        total_trips = cursor.fetchone()[0]

        # Total GPS points
        cursor.execute("SELECT SUM(ST_NumPoints(line)) FROM polylines;")
        total_gps = cursor.fetchone()[0] or 0

        print("Query 1: Total taxis, trips, GPS points")
        print(f"Taxis: {total_taxis}, Trips: {total_trips}, GPS points: {total_gps}\n")

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
