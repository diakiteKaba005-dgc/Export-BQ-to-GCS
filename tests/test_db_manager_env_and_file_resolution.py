import builtins
import io
import json
import os

from cloudSql.db_manager import DBManager


def test_dbmanager_prefers_env_variables(monkeypatch):
    monkeypatch.setenv("DB_HOST", "db.host")
    monkeypatch.setenv("DB_NAME", "env_db")
    monkeypatch.setenv("DB_USER", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_secret")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.delenv("DB_HOST_LOCAL", raising=False)

    manager = DBManager()

    assert "host=db.host" in manager.dsn
    assert "port=5433" in manager.dsn
    assert "dbname=env_db" in manager.dsn
    assert "user=env_user" in manager.dsn
    assert "password=env_secret" in manager.dsn


def test_dbmanager_reads_local_socket_config_with_override(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.setenv("DB_HOST_LOCAL", "localhost")

    fake_config = {
        "DB_HOST": "/cloudsql/project:region:instance",
        "DB_PORT": 5432,
        "DB_NAME": "local_db",
        "DB_USER": "local_user",
        "DB_PASSWORD": "local_secret"
    }

    monkeypatch.setattr(os.path, "exists", lambda path: True)
    monkeypatch.setattr(builtins, "open", lambda path, mode="r", *args, **kwargs: io.StringIO(json.dumps(fake_config)))

    manager = DBManager()

    assert "host=localhost" in manager.dsn
    assert "dbname=local_db" in manager.dsn
    assert "user=local_user" in manager.dsn
    assert "password=local_secret" in manager.dsn
