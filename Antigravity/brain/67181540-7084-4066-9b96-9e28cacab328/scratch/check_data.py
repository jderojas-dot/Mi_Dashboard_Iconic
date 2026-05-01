
import os
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT = "dashboard-iconic-terroirs"
BQ_DATASET = "Mis_Tablas"
CRED_PATH = r"c:\Dashboard_Iconic\Antigravity\credentials.json"

def check_data():
    if os.path.exists(CRED_PATH):
        creds = service_account.Credentials.from_service_account_file(CRED_PATH)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)
        
    sql = f"SELECT Nro_Cta, Nombre_Cuenta, COUNT(*) as n FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_movimientos_contabilidad` GROUP BY 1, 2 LIMIT 20"
    rows = client.query(sql).result()
    for r in rows:
        print(f"Cta: {r.Nro_Cta} | Nom: {r.Nombre_Cuenta} | Count: {r.n}")

if __name__ == "__main__":
    check_data()
