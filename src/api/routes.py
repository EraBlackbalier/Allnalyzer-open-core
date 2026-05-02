import math
import os
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, current_app, request
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from src.core.analyzer import DataAnalyzer
from src.core.parser import RAW_DIR, process_and_convert_to_parquet
from src.database.config import db_config
from src.database.service import FileMetadataService
from src.utils.validators import validate_file_id, validate_file_upload

api_bp = Blueprint("api", __name__, url_prefix="/api")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            return str(value)
    return value


def _response(
    success: bool,
    data: Any = None,
    error: Optional[str] = None,
    code: Optional[str] = None,
    status_code: int = 200,
) -> Tuple[Dict[str, Any], int]:
    return {
        "success": success,
        "data": _json_safe(data),
        "error": error,
        "code": code,
    }, status_code


def _save_upload_to_raw(file: FileStorage) -> tuple[str, str]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    original_name = file.filename or "dataset"
    safe_name = secure_filename(original_name) or "dataset"
    ext = Path(safe_name).suffix.lower()
    token = uuid.uuid4().hex
    raw_name = f"{token}{ext}"
    raw_path = RAW_DIR / raw_name
    file.save(raw_path)
    return token, str(raw_path.resolve())


def _get_file_record(file_id: str):
    session = db_config.get_session()
    try:
        return FileMetadataService.get_file_by_id(session, file_id)
    finally:
        session.close()


@api_bp.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return _response(False, error="No se envio archivo", code="NO_FILE", status_code=400)

    file = request.files["file"]
    if not file.filename:
        return _response(False, error="Nombre de archivo vacio", code="EMPTY_FILENAME", status_code=400)

    token, raw_path = _save_upload_to_raw(file)

    is_valid, error_msg = validate_file_upload(raw_path)
    if not is_valid:
        return _response(False, error=error_msg, code="VALIDATION_ERROR", status_code=400)

    parser_result = process_and_convert_to_parquet(raw_path, file_token=token)
    if parser_result.get("status") != "success":
        return _response(
            False,
            error=parser_result.get("error", "Error desconocido al procesar archivo"),
            code="PARSER_ERROR",
            status_code=400,
        )

    parquet_path = parser_result["parquet_path"]
    schema = parser_result.get("schema", {})

    try:
        analyzer = DataAnalyzer(parquet_path)
        stats = analyzer.get_basic_stats()
    except Exception as exc:
        current_app.logger.warning("No se pudieron calcular estadisticas iniciales: %s", exc)
        stats = {
            "total_rows": None,
            "total_columns": len(schema),
            "columns": list(schema.keys()),
            "numeric_columns": [],
            "text_columns": [],
            "dtypes": schema,
        }

    try:
        session = db_config.get_session()
        try:
            file_record = FileMetadataService.register_file(
                session=session,
                user_id=1,
                name=parser_result.get("original_name", file.filename),
                local_path=parquet_path,
                schema_json=schema,
            )
        finally:
            session.close()
    except Exception as exc:
        current_app.logger.error("Error registrando metadata en Postgres: %s", exc)
        return _response(
            False,
            error="El Parquet se genero, pero no pudo registrarse en PostgreSQL local.",
            code="DATABASE_ERROR",
            status_code=500,
        )

    return _response(
        True,
        data={
            "file_id": str(file_record.id),
            "requires_schema_confirmation": True,
            "original_name": parser_result.get("original_name"),
            "raw_path": parser_result.get("raw_path"),
            "parquet_path": parquet_path,
            "schema": schema,
            "preview_data": parser_result.get("preview_data", []),
            "stats": stats,
        },
        status_code=201,
    )


@api_bp.route("/files", methods=["GET"])
def list_files():
    user_id = request.args.get("user_id", default=1, type=int)
    try:
        session = db_config.get_session()
        try:
            files = FileMetadataService.get_files_by_user(session, user_id)
            return _response(True, data=[file.to_dict() for file in files])
        finally:
            session.close()
    except Exception as exc:
        current_app.logger.error("Error listando archivos: %s", exc)
        return _response(False, error=str(exc), code="SERVER_ERROR", status_code=500)


@api_bp.route("/files/<file_id>", methods=["GET"])
def get_file_detail(file_id: str):
    is_valid, error_msg = validate_file_id(file_id)
    if not is_valid:
        return _response(False, error=error_msg, code="INVALID_FILE_ID", status_code=400)

    try:
        file_record = _get_file_record(file_id)
        if not file_record:
            return _response(False, error="Archivo no encontrado", code="FILE_NOT_FOUND", status_code=404)
        return _response(True, data=file_record.to_dict())
    except Exception as exc:
        current_app.logger.error("Error obteniendo archivo: %s", exc)
        return _response(False, error=str(exc), code="SERVER_ERROR", status_code=500)


@api_bp.route("/files/<file_id>/history", methods=["GET"])
def get_file_history(file_id: str):
    is_valid, error_msg = validate_file_id(file_id)
    if not is_valid:
        return _response(False, error=error_msg, code="INVALID_FILE_ID", status_code=400)

    try:
        session = db_config.get_session()
        try:
            file_record = FileMetadataService.get_file_by_id(session, file_id)
            if not file_record:
                return _response(False, error="Archivo no encontrado", code="FILE_NOT_FOUND", status_code=404)
            history = FileMetadataService.get_analysis_history(session, file_id)
            return _response(True, data=[item.to_dict() for item in history])
        finally:
            session.close()
    except Exception as exc:
        current_app.logger.error("Error obteniendo historial: %s", exc)
        return _response(False, error=str(exc), code="SERVER_ERROR", status_code=500)
