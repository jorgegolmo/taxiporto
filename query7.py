from DbConnector import DbConnector

def main():
    conn = DbConnector()
    cursor = conn.cursor

    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM trips t
            JOIN polylines p ON t.polyline = p.id
            WHERE ST_NumPoints(p.line) < 3;
        """)
        count = cursor.fetchone()[0]
        print("Query 7: Number of invalid trips (<3 GPS points)")
        print(count, "\n")

    finally:
        conn.close_connection()

if __name__ == "__main__":
    main()
