import os
import uuid
from pathlib import Path
from typing import Optional, Tuple


ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "100"))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


def validate_file_upload(file_path: str) -> Tuple[bool, Optional[str]]:
    path = Path(file_path)

    if not path.exists():
        return False, f"Archivo no encontrado: {file_path}"

    try:
        size = path.stat().st_size
    except OSError as exc:
        return False, f"No se pudo acceder al archivo: {exc}"

    if size == 0:
        return False, "Archivo vacio"

    if path.suffix.lower() not in ALLOWED_EXTENSIONS:
        allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
        return False, f"Formato no soportado: {path.suffix.lower()}. Solo se permiten {allowed}"

    if size > MAX_FILE_SIZE_BYTES:
        mb = size / 1024 / 1024
        return False, f"Archivo demasiado grande. Maximo {MAX_FILE_SIZE_MB}MB, recibido {mb:.1f}MB"

    if not os.access(path, os.R_OK):
        return False, "No hay permisos de lectura en el archivo"

    return True, None


def validate_column_name(column_name: str) -> Tuple[bool, Optional[str]]:
    if not column_name:
        return False, "Nombre de columna vacio"

    if len(column_name) > 255:
        return False, "Nombre de columna demasiado largo (max 255 caracteres)"

    return True, None


def validate_file_id(file_id: str) -> Tuple[bool, Optional[str]]:
    if not file_id:
        return False, "ID de archivo vacio"

    try:
        uuid.UUID(str(file_id))
        return True, None
    except ValueError:
        return False, "ID de archivo invalido (se esperaba UUID)"


def validate_user_id(user_id: int) -> Tuple[bool, Optional[str]]:
    if not isinstance(user_id, int) or user_id <= 0:
        return False, "ID de usuario invalido"

    return True, None
