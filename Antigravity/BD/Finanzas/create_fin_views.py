import os
import sys
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
sys.path.append(str(Path(__file__).parent.parent.parent / "backend"))
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH

def create_views():
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    if os.path.exists(CREDENTIALS_PATH):
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)

    sql_path = Path(__file__).parent / "setup_financial_views.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        queries = f.read().split(";")

    print(f"Creating views in {BQ_PROJECT}.{BQ_DATASET}...")
    for query in queries:
        if query.strip():
            try:
                client.query(query).result()
                print(f"OK Executed query: {query.strip()[:50]}...")
            except Exception as e:
                print(f"Error in query: {e}")

if __name__ == "__main__":
    create_views()
