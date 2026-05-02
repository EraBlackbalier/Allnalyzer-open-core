import duckdb
from typing import Dict, Any
from pathlib import Path
import os


class DataAnalyzer:
    MAX_ROWS_FOR_MVP = int(os.getenv("MAX_ROWS_FOR_ANALYSIS", "1000000"))

    def __init__(self, parquet_path: str):
        p = Path(parquet_path)
        if not p.exists():
            raise FileNotFoundError(f"Archivo Parquet no encontrado: {parquet_path}")
        self.parquet_path = str(p.resolve())

    def get_basic_stats(self) -> Dict[str, Any]:
        try:
            conn = duckdb.connect(":memory:")
            conn.execute(f"CREATE VIEW data AS SELECT * FROM parquet_scan('{self.parquet_path}')")
            cols_result = conn.execute("DESCRIBE data").fetchall()
            columns = [col[0] for col in cols_result]
            dtypes = {col[0]: col[1] for col in cols_result}
            numeric_columns = [col for col, dtype in dtypes.items() if self._is_numeric_type(dtype)]
            text_columns = [col for col, dtype in dtypes.items() if self._is_text_type(dtype)]
            row_count_result = conn.execute(
                "SELECT COUNT(*) as cnt FROM data LIMIT ?",
                [self.MAX_ROWS_FOR_MVP + 1],
            ).fetchall()
            total_rows = row_count_result[0][0]
            rows_truncated = total_rows > self.MAX_ROWS_FOR_MVP
            if rows_truncated:
                total_rows = self.MAX_ROWS_FOR_MVP
            conn.close()
            return {
                "total_rows": total_rows,
                "rows_truncated": rows_truncated,
                "max_rows_limit": self.MAX_ROWS_FOR_MVP,
                "total_columns": len(columns),
                "columns": columns,
                "numeric_columns": numeric_columns,
                "text_columns": text_columns,
                "dtypes": dtypes,
            }
        except Exception as exc:
            raise RuntimeError(f"Error obteniendo estadísticas básicas: {str(exc)}") from exc

    def get_column_metrics(self, column_name: str) -> Dict[str, Any]:
        try:
            conn = duckdb.connect(":memory:")
            conn.execute(f"CREATE VIEW data AS SELECT * FROM parquet_scan('{self.parquet_path}')")
            dtypes_result = conn.execute("DESCRIBE data").fetchall()
            col_type = None
            for col_info in dtypes_result:
                col_name = col_info[0]
                col_dtype = col_info[1]
                if col_name.lower() == column_name.lower():
                    col_type = col_dtype
                    break

            if col_type is None:
                raise ValueError(f"Columna no encontrada: {column_name}")

            safe_col = f'"{column_name.replace(chr(34), chr(34) + chr(34))}"'

            if self._is_numeric_type(col_type):
                query = f"""
                SELECT 
                    COUNT(*) as count,
                    COUNT(CASE WHEN {safe_col} IS NULL THEN 1 END) as null_count,
                    AVG({safe_col}) as mean,
                    MIN({safe_col}) as min_val,
                    MAX({safe_col}) as max_val,
                    STDDEV({safe_col}) as std
                FROM data
                LIMIT ?
                """
                result = conn.execute(query, [self.MAX_ROWS_FOR_MVP]).fetchall()[0]
                conn.close()
                return {
                    "column": column_name,
                    "type": col_type,
                    "count": result[0],
                    "null_count": result[1],
                    "mean": float(result[2]) if result[2] is not None else None,
                    "min": float(result[3]) if result[3] is not None else None,
                    "max": float(result[4]) if result[4] is not None else None,
                    "std": float(result[5]) if result[5] is not None else None,
                }

            query = f"""
            SELECT 
                COUNT(*) as count,
                COUNT(CASE WHEN {safe_col} IS NULL THEN 1 END) as null_count,
                COUNT(DISTINCT {safe_col}) as unique_count
            FROM data
            LIMIT ?
            """
            result = conn.execute(query, [self.MAX_ROWS_FOR_MVP]).fetchall()[0]
            conn.close()
            return {
                "column": column_name,
                "type": col_type,
                "count": result[0],
                "null_count": result[1],
                "unique_count": result[2],
            }
        except Exception as exc:
            raise RuntimeError(f"Error obteniendo métricas de columna {column_name}: {str(exc)}") from exc

    def get_all_metrics(self) -> Dict[str, Any]:
        try:
            basic_stats = self.get_basic_stats()
            column_metrics = {}
            for col in basic_stats["columns"]:
                try:
                    column_metrics[col] = self.get_column_metrics(col)
                except Exception as exc:
                    column_metrics[col] = {"error": str(exc)}
            return {
                "summary": basic_stats,
                "columns": column_metrics,
            }
        except Exception as exc:
            raise RuntimeError(f"Error obteniendo métricas completas: {str(exc)}") from exc

    @staticmethod
    def _is_numeric_type(dtype_str: str) -> bool:
        dtype_lower = dtype_str.lower()
        numeric_keywords = ["int", "float", "double", "decimal", "bigint", "smallint", "numeric"]
        return any(keyword in dtype_lower for keyword in numeric_keywords)

    @staticmethod
    def _is_text_type(dtype_str: str) -> bool:
        dtype_lower = dtype_str.lower()
        text_keywords = ["varchar", "text", "string", "char"]
        return any(keyword in dtype_lower for keyword in text_keywords)