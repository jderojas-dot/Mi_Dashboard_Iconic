import os
import sys
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuración
sys.path.append(r"c:\Dashboard_Iconic\Antigravity\backend")
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH

def fix_table():
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if os.path.exists(CREDENTIALS_PATH):
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.TB_movimientos_contabilidad"
    print(f"Updating table: {table_id}")
    
    table = client.get_table(table_id)
    external_config = table.external_data_configuration
    
    if external_config.source_format == "GOOGLE_SHEETS":
        external_config.options.skip_leading_rows = 1
        table.external_data_configuration = external_config
        client.update_table(table, ["external_data_configuration"])
        print("✅ Table updated successfully: skip_leading_rows = 1")
    else:
        print(f"❌ Table is not a Google Sheet: {external_config.source_format}")

if __name__ == "__main__":
    fix_table()
