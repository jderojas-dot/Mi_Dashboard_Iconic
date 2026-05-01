
import sys
import os
from pathlib import Path
from google.cloud import bigquery

# Import config
sys.path.append(str(Path(__file__).parent.parent / "backend"))
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH

def check_views():
    client = bigquery.Client(project=BQ_PROJECT)
    views = ["VW_FIN_BALANCE", "VW_FIN_RESULTADOS", "VW_FIN_FLUJO"]
    
    for view_name in views:
        view_id = f"{BQ_PROJECT}.{BQ_DATASET}.{view_name}"
        try:
            view = client.get_table(view_id)
            print(f"✅ View {view_name} exists.")
            # print(f"SQL: {view.view_query}")
        except Exception as e:
            print(f"❌ View {view_name} NOT FOUND or error: {e}")

if __name__ == "__main__":
    check_views()
