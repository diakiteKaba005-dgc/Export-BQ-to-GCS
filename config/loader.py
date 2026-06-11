import os
import yaml
import re
from typing import Any, Dict

ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_value(value: Any) -> Any:
    if isinstance(value, str):
        # Replace ${VAR} with environment variable, if present
        def _repl(match):
            name = match.group(1)
            return os.environ.get(name, match.group(0))

        s = ENV_VAR_PATTERN.sub(_repl, value)
        # Also expand environment variables like $HOME
        return os.path.expandvars(s)
    if isinstance(value, dict):
        return {k: _expand_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_value(v) for v in value]
    return value


def load_config(env: str | None = None, config_dir: str | None = None) -> Dict[str, Any]:
    """Charge la configuration YAML pour l'environnement donné et remplace
    les références d'environnement ${VAR} par leur valeur.

    Priorité de l'environnement à utiliser:
    1. Argument `env`
    2. Variable d'environnement `APP_ENV`
    3. Variable d'environnement `ENV`
    4. `dev` par défaut
    """
    if env is None:
        env = os.environ.get("APP_ENV") or os.environ.get("ENV") or "dev"
    if config_dir is None:
        # Chemin relatif au repo
        base = os.path.dirname(__file__)
        config_dir = os.path.join(base)

    path = os.path.join(config_dir, f"{env}.yml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    return _expand_value(raw)


if __name__ == "__main__":
    # Small CLI for quick checks
    import json
    cfg = load_config()
    print(json.dumps(cfg, indent=2, ensure_ascii=False))
