import gc
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


STORAGE_ROOT = Path("storage")
RAW_DIR = Path(os.getenv("RAW_STORAGE_FOLDER", str(STORAGE_ROOT / "raw")))
PARQUET_DIR = Path(os.getenv("PARQUET_STORAGE_FOLDER", str(STORAGE_ROOT / "parquet")))


def _json_safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convierte el preview de Pandas a JSON seguro para Vue."""
    safe_df = df.where(pd.notnull(df), None)
    return safe_df.to_dict(orient="records")


def process_and_convert_to_parquet(
    file_path: str,
    *,
    file_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Issue #5 / Paso 2:
    Lee CSV/XLSX con Pandas solo para ingesta, esquema y preview de 5 filas.

    Issue #9 / Paso 3:
    Convierte a Parquet en disco y libera referencias con gc.collect() para
    evitar mantener archivos masivos en RAM.
    """
    source_path = Path(file_path)
    token = file_token or uuid.uuid4().hex

    if not source_path.exists():
        return {"status": "error", "error": "Archivo no encontrado", "parquet_path": ""}

    try:
        if source_path.stat().st_size == 0:
            return {"status": "error", "error": "Archivo vacio", "parquet_path": ""}
    except OSError as exc:
        return {"status": "error", "error": f"No se pudo acceder al archivo: {exc}", "parquet_path": ""}

    ext = source_path.suffix.lower()
    if ext not in {".csv", ".xls", ".xlsx"}:
        return {"status": "error", "error": "Formato de archivo no soportado", "parquet_path": ""}

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = (PARQUET_DIR / f"{token}.parquet").resolve()

    sample_df: Optional[pd.DataFrame] = None
    writer: Optional[pq.ParquetWriter] = None

    try:
        if ext == ".csv":
            try:
                sample_df = pd.read_csv(source_path, nrows=100)
            except pd.errors.EmptyDataError:
                return {"status": "error", "error": "CSV vacio o corrupto", "parquet_path": ""}

            if sample_df.shape[1] == 0:
                return {"status": "error", "error": "Archivo sin columnas detectables", "parquet_path": ""}

            schema = {str(col): str(dtype) for col, dtype in sample_df.dtypes.items()}
            preview_data = _json_safe_records(sample_df.head(5))

            # CSV se procesa por chunks para no cargar el archivo completo.
            for chunk in pd.read_csv(source_path, chunksize=100_000):
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(str(parquet_path), table.schema)
                writer.write_table(table)
                del chunk
                del table
                gc.collect()

            if writer is not None:
                writer.close()
                writer = None

        else:
            try:
                sample_df = pd.read_excel(source_path, engine="openpyxl", nrows=100)
            except Exception:
                return {"status": "error", "error": "XLSX corrupto o en formato no soportado", "parquet_path": ""}

            if sample_df.shape[1] == 0:
                return {"status": "error", "error": "Archivo sin columnas detectables", "parquet_path": ""}

            schema = {str(col): str(dtype) for col, dtype in sample_df.dtypes.items()}
            preview_data = _json_safe_records(sample_df.head(5))

            # XLSX no ofrece chunks nativos en Pandas; se libera inmediatamente.
            df = pd.read_excel(source_path, engine="openpyxl")
            if df.shape[0] == 0:
                return {"status": "error", "error": "Archivo XLSX sin filas", "parquet_path": ""}

            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, str(parquet_path))
            del df
            del table
            gc.collect()

        del sample_df
        gc.collect()

        return {
            "status": "success",
            "file_token": token,
            "original_name": source_path.name,
            "raw_path": str(source_path.resolve()),
            "parquet_path": str(parquet_path),
            "schema": schema,
            "preview_data": preview_data,
        }

    except Exception as exc:
        if writer is not None:
            writer.close()
        if parquet_path.exists():
            try:
                parquet_path.unlink()
            except OSError:
                pass
        gc.collect()
        return {"status": "error", "error": str(exc), "parquet_path": ""}
