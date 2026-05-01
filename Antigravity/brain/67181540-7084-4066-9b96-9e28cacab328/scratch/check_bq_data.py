import os
import sys
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
sys.path.append(r"c:\Dashboard_Iconic\Antigravity\backend")
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH

def check_data():
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if os.path.exists(CREDENTIALS_PATH):
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)

    query = f"SELECT Ejercicio, COUNT(*) as count FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_movimientos_contabilidad` GROUP BY 1"
    print(f"Querying: {query}")
    results = client.query(query).result()
    for row in results:
        print(f"Año {row.Ejercicio}: {row.count} filas")

if __name__ == "__main__":
    check_data()
