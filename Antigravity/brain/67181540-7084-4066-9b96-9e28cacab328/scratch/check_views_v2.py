
import os
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT = "dashboard-iconic-terroirs"
BQ_DATASET = "Mis_Tablas"
CRED_PATH = r"c:\Dashboard_Iconic\Antigravity\credentials.json"

def check_views():
    if os.path.exists(CRED_PATH):
        creds = service_account.Credentials.from_service_account_file(CRED_PATH)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)
        
    views = ["VW_FIN_BALANCE", "VW_FIN_RESULTADOS", "VW_FIN_FLUJO"]
    
    for view_name in views:
        view_id = f"{BQ_PROJECT}.{BQ_DATASET}.{view_name}"
        try:
            view = client.get_table(view_id)
            print(f"✅ View {view_name} exists.")
        except Exception as e:
            print(f"❌ View {view_name} NOT FOUND: {e}")

if __name__ == "__main__":
    check_views()
