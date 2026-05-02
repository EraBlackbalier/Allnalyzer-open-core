import uuid
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column

Base = declarative_base()


class FileMetadata(Base):
    __tablename__ = "file_metadata"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    user_id = Column(Integer, nullable=False, index=True, default=1)

    name = Column(String(255), nullable=False)

    local_path = Column(Text, nullable=False, unique=True)

    schema_json = Column(JSONB, nullable=False, default=dict)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    analysis_records = relationship(
        "AnalysisHistory",
        back_populates="file_metadata",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_file_metadata_user_created", "user_id", "created_at"),
        Index("idx_file_metadata_local_path", "local_path"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "user_id": self.user_id,
            "name": self.name,
            "local_path": self.local_path,
            "schema_json": self.schema_json,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class AnalysisHistory(Base):
    __tablename__ = "analysis_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    file_id = Column(
        UUID(as_uuid=True),
        ForeignKey("file_metadata.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    user_id = Column(Integer, nullable=False, index=True, default=1)

    metric_name = Column(String(120), nullable=False)

    numeric_result = Column(Float, nullable=False)

    question = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    file_metadata = relationship("FileMetadata", back_populates="analysis_records")

    __table_args__ = (
        Index("idx_analysis_history_file_created", "file_id", "created_at"),
        Index("idx_analysis_history_user_created", "user_id", "created_at"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "file_id": str(self.file_id),
            "user_id": self.user_id,
            "metric_name": self.metric_name,
            "numeric_result": self.numeric_result,
            "question": self.question,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
