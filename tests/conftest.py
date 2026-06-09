import os


def pytest_configure():
    # Assure l'import de DBManager à l'initialisation des tests sans dépendance réelle à PostgreSQL.
    os.environ.setdefault("DB_HOST", "127.0.0.1")
    os.environ.setdefault("DB_NAME", "test_db")
    os.environ.setdefault("DB_USER", "test_user")
