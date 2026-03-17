"""
╔══════════════════════════════════════════════════════════════════╗
║  ICONIC TERROIRS — Backend API (FastAPI + BigQuery)             ║
║  Ejecutar: uvicorn main:app --reload --port 8000                ║
╚══════════════════════════════════════════════════════════════════╝
"""
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
from google.cloud import bigquery
from google.oauth2 import service_account
import os, json
import uvicorn
from pathlib import Path
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH, ALLOWED_ORIGINS

# ── BigQuery client (inicializado una vez al arrancar) ──
bq: bigquery.Client | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bq
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON"):
        print("🔧 Iniciando BigQuery con credenciales de variable de entorno...")
        creds_json = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
        creds = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
        bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    elif CREDENTIALS_PATH and Path(CREDENTIALS_PATH).exists():
        print(f"🔧 Iniciando BigQuery con archivo: {CREDENTIALS_PATH}")
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        print("⚠️ Iniciando BigQuery sin credenciales explícitas (usando gcloud ADC)...")
        bq = bigquery.Client(project=BQ_PROJECT)
    print(f"✅ BigQuery conectado → proyecto: {BQ_PROJECT}")
    yield
    if bq: bq.close()

app = FastAPI(title="Iconic Terroirs API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

VIEW = f"`{BQ_PROJECT}.{BQ_DATASET}.VW_VENTAS_DASHBOARD`"

import time
import threading

_query_cache = {}
_bq_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutos de caché en memoria

def run(sql: str) -> list[dict]:
    """Ejecuta SQL y retorna lista de dicts con caché básico y claves en minúsculas."""
    now = time.time()
    
    # 1. Verificar caché
    if sql in _query_cache:
        result, timestamp = _query_cache[sql]
        if now - timestamp < CACHE_TTL:
            return result

    # 2. Consultar a BigQuery
    try:
        query_brief = sql.strip().split("\n")[0][:100] + "..."
        print(f"🔍 Ejecutando BigQuery: {query_brief}")
        start_t = time.time()
        
        query_job = bq.query(sql)
        rows = query_job.result(timeout=45) 
        
        # Transformamos las claves a minúsculas para consistencia con el frontend
        result = []
        for r in rows:
            d = {k.lower(): v for k, v in dict(r).items()}
            result.append(d)
        
        end_t = time.time()
        print(f"✅ Query completada en {end_t - start_t:.2f}s ({len(result)} filas)")
        
        # 3. Guardar en caché
        _query_cache[sql] = (result, now)
        return result
    except Exception as e:
        print(f"❌ ERROR en BigQuery: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════
# VENTAS — KPIs
# ══════════════════════════════════════════════════════════════════
@app.get("/api/kpis")
def get_kpis(anno: int | None = None):
    # Usamos la vista GLOBAL que ya tiene todo calculado
    if not anno:
        sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_KPI_GLOBAL`"
        data = run(sql)
        return {"current": data[0], "previous": None}
    
    # Si hay año, usamos la lógica de comparación dinámicamente
    sql_base = f"""
    SELECT
      ROUND(SUM(VENTA_NETA_MN), 0)                          AS venta_neta,
      ROUND(SUM(MARGEN_MN), 0)                               AS margen,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100, 1) AS pct_margen,
      SUM(CANTIDAD)                                          AS unidades,
      COUNT(DISTINCT CLIENTE)                                AS clientes,
      COUNT(DISTINCT COD_VENTA)                              AS pedidos,
      ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN),COUNT(DISTINCT COD_VENTA)), 0) AS pedido_promedio
    FROM {VIEW}
    WHERE CAST(ANNO AS INT64) = {{year}}
    """
    current = run(sql_base.format(year=anno))
    previous = run(sql_base.format(year=anno-1))
    
    return {
        "current": current[0] if current else {},
        "previous": previous[0] if previous else None
    }

# ══════════════════════════════════════════════════════════════════
# VENTAS — SERIE MENSUAL
# ══════════════════════════════════════════════════════════════════
@app.get("/api/serie-mensual")
def get_serie_mensual(anno: int | None = None):
    where = f"WHERE ANNO = {anno}" if anno else ""
    sql = f"""
    SELECT periodo, anno, mes, periodo_label AS mes_label,
           venta_neta, margen_neto AS margen, unidades, pedidos, aov AS pedido_promedio
    FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_SERIE_MENSUAL`
    {where}
    ORDER BY periodo
    """
    rows = run(sql)
    for r in rows: r["periodo"] = str(r["periodo"])
    return rows

# ══════════════════════════════════════════════════════════════════
# VENTAS — DIMENSIONES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/por-pais")
def get_por_pais(anno: int | None = None):
    sql = f"SELECT pais_origen_producto AS pais, venta_neta, margen_neto AS margen, unidades FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_DIMENSION_PAIS` LIMIT 10"
    return run(sql)

@app.get("/api/por-tipo")
def get_por_tipo(anno: int | None = None):
    sql = f"SELECT nombre_tipo_bebida AS tipo, venta_neta, unidades, pct_venta FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_DIMENSION_TIPO`"
    return run(sql)

@app.get("/api/por-marca")
def get_por_marca(anno: int | None = None):
    sql = f"SELECT marca_producto AS marca, venta_neta, margen_neto AS margen, unidades FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_DIMENSION_MARCA` LIMIT 10"
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — CLIENTES & PRODUCTOS
# ══════════════════════════════════════════════════════════════════
@app.get("/api/top-clientes")
def get_top_clientes(limit: int = 20):
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_TOP_CLIENTES` LIMIT {limit}"
    rows = run(sql)
    for r in rows: r["ultima_compra"] = str(r["ultima_compra"])
    return rows

@app.get("/api/top-productos")
def get_top_productos(limit: int = 20):
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_TOP_PRODUCTOS` LIMIT {limit}"
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — ADICIONALES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/vendedores")
def get_vendedores():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_VENDEDORES`"
    return run(sql)

@app.get("/api/segmentos-precio")
def get_segmentos_precio():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_SEGMENTO_PRECIO`"
    return run(sql)

@app.get("/api/margenes-marca")
def get_margenes_marca():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_MARGEN_MARCA`"
    return run(sql)

@app.get("/api/rfm")
def get_rfm_list():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_RFM` LIMIT 8"
    rows = run(sql)
    for r in rows: r["ultima_compra"] = str(r["ultima_compra"])
    return rows

@app.get("/api/estacionalidad")
def get_estacionalidad():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_LOOKER_ESTACIONALIDAD`"
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# FORECAST
# ══════════════════════════════════════════════════════════════════
@app.get("/api/forecast-total")
def get_forecast_total():
    try:
        sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_TOTAL` ORDER BY periodo"
        rows = run(sql)
        for r in rows: r["periodo"] = str(r["periodo"])
        return rows
    except Exception: return []

@app.get("/api/forecast-productos")
def get_forecast_productos():
    try:
        sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_PRODUCTOS` ORDER BY producto, periodo"
        rows = run(sql)
        for r in rows: r["periodo"] = str(r["periodo"])
        return rows
    except Exception: return []

# ══════════════════════════════════════════════════════════════════
# SERVIR FRONTEND
# ══════════════════════════════════════════════════════════════════
frontend_path = Path(__file__).parent.parent / "frontend" / "public"

@app.get("/{full_path:path}")
def serve_frontend(full_path: str):
    file_p = frontend_path / full_path
    if full_path == "" or not file_p.exists() or file_p.is_dir():
        # Fallback to index.html for SPA routing
        if (frontend_path / "index.html").exists():
            return FileResponse(str(frontend_path / "index.html"))
        return {"error": "Frontend not found"}
    return FileResponse(str(file_p))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
