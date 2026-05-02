from pathlib import Path
from typing import Any, Dict, List

import duckdb


class DataEngine:
    """
    Paso 9 / Issue #6:
    Ejecuta SQL analitico con DuckDB directamente sobre Parquet local, sin
    cargar el dataset completo de forma persistente en RAM.
    """

    def __init__(self, database: str = ":memory:") -> None:
        self.database = database

    def execute_query(self, parquet_path: str, sql_query: str) -> List[Dict[str, Any]]:
        """
        Crea una vista temporal `data` sobre el Parquet y ejecuta la consulta.
        El caller valida que sea SELECT antes de llegar aqui.
        """
        path = Path(parquet_path)
        if not path.exists():
            raise FileNotFoundError(f"Archivo Parquet no encontrado: {parquet_path}")

        conn = duckdb.connect(self.database)
        safe_path = str(path.resolve()).replace("'", "''")

        try:
            conn.execute(
                "CREATE OR REPLACE VIEW data AS "
                f"SELECT * FROM parquet_scan('{safe_path}')"
            )
            conn.execute("CREATE OR REPLACE VIEW parquet_view AS SELECT * FROM data")

            relation = conn.execute(sql_query)
            try:
                df = relation.fetchdf()
                return df.to_dict(orient="records")
            except Exception:
                rows = relation.fetchall()
                columns = [column[0] for column in relation.description]
                return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()
