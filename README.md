Usage rapide pour le loader de configuration

- Définir l'environnement via la variable d'environnement `APP_ENV` (ex: `dev`, `rec`, `prd`).
- Les fichiers se trouvent dans le dossier `config/` et sont nommés `dev.yml`, `rec.yml`, `prd.yml`.

Secrets et chemins de clefs

- Vous pouvez référencer un secret stocké comme variable d'environnement via la syntaxe `${VAR}` dans les YAML.
- Exemple courant: définir `SA_KEY_PATH` (chemin local vers la clef JSON) et l'utiliser dans `credentials_path: "${SA_KEY_PATH}"`.
- En production, préférez renseigner `service_account` plutôt que `credentials_path`.
- Le loader remplace `${SA_KEY_PATH}` par la valeur de la variable d'environnement. `main.py` exporte automatiquement
	`GOOGLE_APPLICATION_CREDENTIALS` si `credentials_path` est présent dans la config.
- Pour PostgreSQL, ajoutez une section `postgres` avec `host`, `port`, `dbname`, `user`, et `password` ou `password_secret`.
- Le loader remplace aussi `${POSTGRES_APP_PASSWORD}` par la variable d'environnement correspondante.
- Si `password` n'est pas fourni et que `password_secret` est défini, `DBManager` va récupérer la valeur du secret depuis Google Secret Manager.
- Le répertoire des DAGs est maintenant configurable avec `dag_dir`.
- Vous pouvez définir `dag_dir: "gs://..."` pour des DAGs stockés dans un bucket Cloud Storage en production.

Exemple de bloc PostgreSQL dans `config/dev.yml`:

```yaml
postgres:
  host: "localhost"
  port: 5432
  dbname: "my_project_dev"
  user: "my_user"
  password_secret: "${POSTGRES_PASSWORD_SECRET}"
dag_dir: "./dag"
```

Exemple de configuration de production :

```yaml
project_id: "my-project-prd"
gcs_bucket: "my-project-prd-bucket"
bigquery_dataset: "dataset_prd"
dag_dir: "gs://my-project-prd-dags"
service_account: "my-service-account@my-project-prd.iam.gserviceaccount.com"
postgres:
  host: "my-prod-postgres-host"
  port: 5432
  dbname: "my_project_prd"
  user: "my_user"
  password_secret: "${POSTGRES_PASSWORD_SECRET}"
```

Exemple d'utilisation dans Python:

```python
from config.loader import load_config

cfg = load_config()  # charge l'environnement depuis APP_ENV ou 'dev'
print(cfg["project_id"])  # lit project_id
```

Les valeurs au format `${VAR}` seront remplacées par la variable d'environnement `VAR`.

Pour la production, préférez l'utilisation de Secret Manager ou variables d'environnement pour les secrets.
