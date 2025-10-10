from DbConnector import DbConnector

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM trips t
            JOIN polylines p ON t.polyline = p.id
            WHERE DATE(t.timestamp) <> DATE(
                  t.timestamp + INTERVAL (ST_NumPoints(p.line) - 1) * 15 SECOND
            );
        """)
        count = cursor.fetchone()[0]
        print("Query 9: Number of trips starting on one day and ending the next (midnight-crossers)")
        print(count, "\n")

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
