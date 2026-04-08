import sys
from pathlib import Path
sys.path.append(str(Path(r"c:\Dashboard_Iconic\Antigravity\backend")))
from google.cloud import bigquery
from config import BQ_PROJECT, BQ_DATASET
from google.oauth2 import service_account

creds = service_account.Credentials.from_service_account_file(r"c:\Dashboard_Iconic\Antigravity\credentials.json")
bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)
for table in ["TB_mov_inventario", "VW_VENTAS_DASHBOARD", "TB_CACHE_VENTAS_DASHBOARD"]:
    query = f"SELECT column_name, data_type FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}'"
    print(f"\nTable {table}:")
    for r in bq.query(query).result(): print(f"  {r.column_name}: {r.data_type}")
