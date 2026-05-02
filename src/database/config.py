import os
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker

load_dotenv()


class DatabaseConfig:
    """
    Gestiona la configuracion de PostgreSQL local.

    Issue #2 / Paso 7:
    PostgreSQL es el cerebro administrativo: guarda metadata, rutas locales
    de Parquet e historial de resultados. La app sigue siendo cloud-ready,
    pero en esta fase todo apunta a localhost.
    """

    def __init__(self) -> None:
        self.db_url: Optional[str] = os.getenv("DATABASE_URL")
        self.echo = os.getenv("DATABASE_ECHO", "false").lower() == "true"
        self._engine = None
        self._session_factory = None

    def get_database_url(self) -> str:
        """
        Retorna la URL de PostgreSQL desde variables de entorno.

        Prioridad:
        1. DATABASE_URL completo.
        2. Partes separadas para poder editar usuario/password en .env.
        """
        if self.db_url:
            return self.db_url

        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "Allnalyzer")

        return URL.create(
            "postgresql+psycopg2",
            username=db_user,
            password=db_password,
            host=db_host,
            port=int(db_port),
            database=db_name,
        ).render_as_string(hide_password=False)

    def init_engine(self):
        """Inicializa el engine SQLAlchemy para PostgreSQL local."""
        self._engine = create_engine(
            self.get_database_url(),
            echo=self.echo,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
        return self._engine

    def get_engine(self):
        """Obtiene el engine, inicializandolo si es necesario."""
        if self._engine is None:
            self.init_engine()
        return self._engine

    def init_session_factory(self):
        """Inicializa la factory de sesiones."""
        self._session_factory = sessionmaker(
            bind=self.get_engine(),
            expire_on_commit=False,
        )
        return self._session_factory

    def get_session_factory(self):
        """Obtiene la factory de sesiones."""
        if self._session_factory is None:
            self.init_session_factory()
        return self._session_factory

    def get_session(self) -> Session:
        """Crea una sesion nueva de base de datos."""
        return self.get_session_factory()()

    def create_all_tables(self) -> None:
        """Crea todas las tablas administrativas en PostgreSQL local."""
        from src.models import Base

        Base.metadata.create_all(self.get_engine())


db_config = DatabaseConfig()
