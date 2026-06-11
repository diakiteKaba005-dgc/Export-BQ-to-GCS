import os
import pytest

import main


def test_set_google_application_credentials_sets_env_when_config_present(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    config = {"credentials_path": "/tmp/fake-key.json"}

    result = main.set_google_application_credentials(config, env_name="prd")

    assert result is None
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/tmp/fake-key.json"


def test_set_google_application_credentials_uses_service_account(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    fake_source = object()
    monkeypatch.setattr(main.google.auth, "default", lambda: (fake_source, "my-project"))

    created = {}
    def fake_impersonated_credentials(source_credentials, target_principal, target_scopes, lifetime):
        created["args"] = (source_credentials, target_principal, target_scopes, lifetime)
        return "impersonated-creds"

    monkeypatch.setattr(main, "ImpersonatedCredentials", fake_impersonated_credentials)

    config = {"service_account": "sa@project.iam.gserviceaccount.com"}
    result = main.set_google_application_credentials(config, env_name="prd")

    assert result == "impersonated-creds"
    assert created["args"][0] is fake_source
    assert created["args"][1] == "sa@project.iam.gserviceaccount.com"
    assert created["args"][2] == ["https://www.googleapis.com/auth/cloud-platform"]
    assert created["args"][3] == 3600


def test_set_google_application_credentials_raises_in_prd_when_missing(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    with pytest.raises(RuntimeError, match="Missing Google credentials"):
        main.set_google_application_credentials({}, env_name="prd")


def test_set_google_application_credentials_allows_missing_in_dev(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setenv("APP_ENV", "dev")

    result = main.set_google_application_credentials({}, env_name="dev")

    assert result is None
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ


def test_get_dag_dir_uses_default_on_missing_config():
    expected = os.path.join(os.path.dirname(main.__file__), "dag")
    assert main.get_dag_dir({}) == expected


def test_get_dag_dir_uses_config_value_relative_path():
    config = {"dag_dir": "./custom_dags"}
    expected = os.path.abspath(os.path.join(os.path.dirname(main.__file__), "./custom_dags"))
    assert main.get_dag_dir(config) == expected


def test_get_dag_dir_uses_gcs_uri_unchanged():
    config = {"dag_dir": "gs://my-project-prd-dags"}
    assert main.get_dag_dir(config) == "gs://my-project-prd-dags"
