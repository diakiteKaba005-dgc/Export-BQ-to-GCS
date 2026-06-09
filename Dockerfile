# 1. Utilisation d'une image de base Python légère (Slim)
FROM python:3.11-slim

# 2. Configuration des variables d'environnement Python
# PYTHONDONTWRITEBYTECODE=1 : Évite la création de fichiers .pyc (inutiles en conteneur)
# PYTHONUNBUFFERED=1 : Force l'affichage immédiat des logs dans Google Cloud Logging (sans buffer)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Définition du répertoire de travail dans le conteneur
WORKDIR /app

# 4. Installation des dépendances système nécessaires pour le driver PostgreSQL (psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 5. Copie et installation des dépendances Python (Mise en cache optimale)
COPY Requirements.txt .
RUN pip install --no-cache-dir -r Requirements.txt

# 6. Copie du reste du code source du projet
# (Les fichiers comme venv/ ou cloudSql/connexion.json seront ignorés grâce au .dockerignore)
COPY . .

# 7. Validation du code avant de finaliser l'image
RUN pytest -q tests

# 8. Information sur le port d'écoute (Cloud Run utilise par défaut le port 8080)
EXPOSE 8080

# 8. Commande de démarrage du serveur Uvicorn
# On utilise exec pour mapper dynamiquement la variable PORT injectée automatiquement par Cloud Run
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]