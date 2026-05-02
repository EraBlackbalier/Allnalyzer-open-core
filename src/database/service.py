import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from src.models import AnalysisHistory, FileMetadata


class FileMetadataService:
    """Servicio de metadata e historial sobre PostgreSQL local."""

    @staticmethod
    def register_file(
        session: Session,
        user_id: int,
        name: str,
        local_path: str,
        schema_json: Dict[str, str],
    ) -> FileMetadata:
        """Issue #2 / Paso 7: registra el Parquet y su esquema JSONB."""
        if not name or not local_path:
            raise ValueError("name y local_path no pueden estar vacios")

        try:
            file_record = FileMetadata(
                user_id=user_id,
                name=name,
                local_path=local_path,
                schema_json=schema_json,
            )
            session.add(file_record)
            session.commit()
            session.refresh(file_record)
            return file_record
        except Exception as exc:
            session.rollback()
            raise RuntimeError(f"Error al registrar archivo: {exc}") from exc

    @staticmethod
    def get_file_by_id(session: Session, file_id: str) -> Optional[FileMetadata]:
        """Paso 8: obtiene la ruta local del Parquet desde Postgres."""
        try:
            parsed_file_id = uuid.UUID(str(file_id))
            return session.query(FileMetadata).filter(FileMetadata.id == parsed_file_id).first()
        except ValueError:
            return None
        except Exception as exc:
            raise RuntimeError(f"Error al consultar archivo: {exc}") from exc

    @staticmethod
    def get_files_by_user(session: Session, user_id: int) -> List[FileMetadata]:
        """Lista archivos del usuario local mock."""
        try:
            return (
                session.query(FileMetadata)
                .filter(FileMetadata.user_id == user_id)
                .order_by(FileMetadata.created_at.desc())
                .all()
            )
        except Exception as exc:
            raise RuntimeError(f"Error al listar archivos del usuario: {exc}") from exc

    @staticmethod
    def update_file_stats(
        session: Session,
        file_id: str,
        schema_json: Dict[str, str],
    ) -> FileMetadata:
        """Actualiza esquema si una normalizacion posterior lo requiere."""
        try:
            parsed_file_id = uuid.UUID(str(file_id))
            file_record = session.query(FileMetadata).filter(FileMetadata.id == parsed_file_id).first()
            if not file_record:
                raise ValueError(f"Archivo con ID {file_id} no encontrado")

            file_record.schema_json = schema_json
            session.commit()
            session.refresh(file_record)
            return file_record
        except Exception as exc:
            session.rollback()
            raise RuntimeError(f"Error al actualizar estadisticas: {exc}") from exc

    @staticmethod
    def delete_file(session: Session, file_id: str) -> bool:
        """Elimina metadata de archivo y su historial asociado."""
        try:
            parsed_file_id = uuid.UUID(str(file_id))
            file_record = session.query(FileMetadata).filter(FileMetadata.id == parsed_file_id).first()
            if not file_record:
                return False
            session.delete(file_record)
            session.commit()
            return True
        except ValueError:
            return False
        except Exception as exc:
            session.rollback()
            raise RuntimeError(f"Error al eliminar archivo: {exc}") from exc

    @staticmethod
    def register_analysis_result(
        session: Session,
        file_id: str,
        user_id: int,
        metric_name: str,
        numeric_result: float,
        question: Optional[str] = None,
    ) -> AnalysisHistory:
        """Paso 11: guarda resultados numericos devueltos por DuckDB."""
        if not metric_name:
            raise ValueError("metric_name no puede estar vacio")

        try:
            parsed_file_id = uuid.UUID(str(file_id))
        except ValueError as exc:
            raise ValueError("file_id invalido para historial") from exc

        try:
            history_record = AnalysisHistory(
                file_id=parsed_file_id,
                user_id=user_id,
                metric_name=metric_name,
                numeric_result=float(numeric_result),
                question=question,
            )
            session.add(history_record)
            session.commit()
            session.refresh(history_record)
            return history_record
        except Exception as exc:
            session.rollback()
            raise RuntimeError(f"Error guardando historial de analisis: {exc}") from exc

    @staticmethod
    def get_analysis_history(session: Session, file_id: str) -> List[AnalysisHistory]:
        """Paso 11: lista el historial numerico asociado a un archivo."""
        try:
            parsed_file_id = uuid.UUID(str(file_id))
        except ValueError:
            return []

        try:
            return (
                session.query(AnalysisHistory)
                .filter(AnalysisHistory.file_id == parsed_file_id)
                .order_by(AnalysisHistory.created_at.desc())
                .all()
            )
        except Exception as exc:
            raise RuntimeError(f"Error consultando historial de analisis: {exc}") from exc
