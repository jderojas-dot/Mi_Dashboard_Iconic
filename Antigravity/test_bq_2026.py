import sys
from pathlib import Path
sys.path.append(str(Path(r"c:\Dashboard_Iconic\Antigravity\backend")))
from google.cloud import bigquery
from config import BQ_PROJECT, BQ_DATASET
from google.oauth2 import service_account

creds = service_account.Credentials.from_service_account_file(r"c:\Dashboard_Iconic\Antigravity\credentials.json")
bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)

query1 = "SELECT * FROM `dashboard-iconic-terroirs.Mis_Tablas.TB_CACHE_TOP_CLIENTES_ANNO` WHERE anno = 2026 LIMIT 5"
try:
    print("Top Clientes 2026:")
    for r in bq.query(query1).result(): print(dict(r))
except Exception as e: print(e)

query2 = "SELECT * FROM `dashboard-iconic-terroirs.Mis_Tablas.TB_CACHE_KPI_YTD` WHERE anno = 2026"
try:
    print("KPI YTD 2026:")
    for r in bq.query(query2).result(): print(dict(r))
except Exception as e: print(e)
