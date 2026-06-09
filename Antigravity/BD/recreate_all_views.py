import os
import sys
import re
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Import configuration from backend
sys.path.append(str(Path(__file__).parent.parent / "backend"))
try:
    from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH
except ImportError:
    BQ_PROJECT = "dashboard-iconic-terroirs"
    BQ_DATASET = "Mis_Tablas"
    CREDENTIALS_PATH = str(Path(__file__).parent.parent / "credentials.json")

def get_client():
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if CREDENTIALS_PATH and os.path.exists(CREDENTIALS_PATH):
        print(f"[INFO] Usando archivo de credenciales: {CREDENTIALS_PATH}")
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH, scopes=scopes
        )
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)
    return bigquery.Client(project=BQ_PROJECT)

def execute_sql_file(client, file_path):
    print(f"[READ] Leyendo y ejecutando consultas en: {file_path.name}...")
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
        
    # Limpiar comentarios multilínea /* ... */
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
    
    # Separar por ";" pero asegurando no romper strings
    queries = []
    raw_queries = content.split(";")
    for q in raw_queries:
        clean_q = q.strip()
        lines = [line for line in clean_q.split("\n") if not line.strip().startswith("--")]
        final_query = "\n".join(lines).strip()
        if final_query:
            queries.append(final_query)
            
    print(f"[INFO] Encontradas {len(queries)} consultas DDL en el archivo.")
    
    for idx, query in enumerate(queries):
        match = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+[`\"\']?([\w\.\-]+)[`\"\']?", query, re.IGNORECASE)
        name = match.group(1) if match else f"Consulta #{idx+1}"
        
        print(f"[RUN] Ejecutando: {name}...")
        try:
            client.query(query).result()
            print(f"[OK] Ejecutada con exito: {name}")
        except Exception as e:
            print(f"[ERROR] Al ejecutar {name}: {e}")

def main():
    print("[START] Iniciando recreacion de vistas de Looker y del Dashboard...")
    try:
        client = get_client()
    except Exception as e:
        print(f"[ERROR] Al conectar a BigQuery: {e}")
        return
        
    base_dir = Path(__file__).parent.parent
    
    # 1. Recrear vistas del dashboard
    dash_sql_path = base_dir / "setup_dashboard_views.sql"
    if dash_sql_path.exists():
        execute_sql_file(client, dash_sql_path)
    else:
        print(f"[WARN] No se encontro: {dash_sql_path}")
        
    # 2. Recrear vistas de Looker
    looker_sql_path = base_dir / "setup_looker_views.sql"
    if looker_sql_path.exists():
        execute_sql_file(client, looker_sql_path)
    else:
        print(f"[WARN] No se encontro: {looker_sql_path}")
        
    print("\n[END] Proceso de recreacion de vistas completado.")

if __name__ == "__main__":
    main()
