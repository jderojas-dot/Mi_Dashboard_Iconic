
import os
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT = "dashboard-iconic-terroirs"
BQ_DATASET = "Mis_Tablas"
CRED_PATH = r"c:\Dashboard_Iconic\Antigravity\credentials.json"

def get_sql():
    if os.path.exists(CRED_PATH):
        creds = service_account.Credentials.from_service_account_file(CRED_PATH)
        client = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        client = bigquery.Client(project=BQ_PROJECT)
        
    view_id = f"{BQ_PROJECT}.{BQ_DATASET}.VW_FIN_BALANCE"
    view = client.get_table(view_id)
    print(view.view_query)

if __name__ == "__main__":
    get_sql()
