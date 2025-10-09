from DbConnector import DbConnector
import pandas as pd
from tabulate import tabulate

class IdleTimeAnalysis:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def compute_idle_times(self):
        # SQL base: calcula hora de inicio y hora estimada de fin por taxi
        query = """
        SELECT
            t.taxi_id,
            t.timestamp AS start_time,
            t.timestamp + INTERVAL (ST_NumPoints(p.line) - 1) * 15 SECOND AS end_time
        FROM trips t
        JOIN polylines p ON t.polyline = p.id
        WHERE ST_NumPoints(p.line) > 1;
        """

        # Leer directamente desde SQL a pandas
        df = pd.read_sql(query, self.db_connection)

        # Asegurar que los tiempos sean datetime
        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"] = pd.to_datetime(df["end_time"])

        # Ordenar por taxi y tiempo
        df = df.sort_values(["taxi_id", "start_time"])

        # Calcular fin del viaje anterior
        df["prev_end"] = df.groupby("taxi_id")["end_time"].shift(1)

        # Calcular tiempo inactivo en segundos
        df["idle_s"] = (df["start_time"] - df["prev_end"]).dt.total_seconds()

        # Filtrar valores válidos (mayor que 0 y no nulos)
        df = df[df["idle_s"].notnull() & (df["idle_s"] > 0)]

        # Calcular el promedio de tiempo inactivo por taxi
        avg_idle = (
            df.groupby("taxi_id")["idle_s"]
            .mean()
            .sort_values(ascending=False)
            .head(20)
            .reset_index()
        )

        # Convertir segundos a minutos
        avg_idle["avg_idle_minutes"] = avg_idle["idle_s"] / 60
        avg_idle = avg_idle[["taxi_id", "avg_idle_minutes"]]

        print("\nTop 20 taxis with highest average idle time:")
        print(tabulate(avg_idle, headers="keys", tablefmt="psql", showindex=False))

        return avg_idle

    def close(self):
        self.connection.close_connection()


def main():
    program = None
    try:
        program = IdleTimeAnalysis()
        result = program.compute_idle_times()

        # Guardar resultados a CSV
        result.to_csv("../sol/top20_idle_taxis.csv", index=False)
        print("\nResults saved to 'top20_idle_taxis.csv'")
    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.close()


if __name__ = '__main__':
    main()
