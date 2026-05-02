from src.database.config import db_config


def create_all() -> None:
    db_config.create_all_tables()


if __name__ == "__main__":
    create_all()
    print("Tablas creadas correctamente en PostgreSQL local.")
