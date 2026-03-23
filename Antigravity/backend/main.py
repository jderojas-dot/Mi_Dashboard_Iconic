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
import os, json, datetime, time
import uvicorn
from pathlib import Path
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH, ALLOWED_ORIGINS
from concurrent.futures import ThreadPoolExecutor

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


import time
import threading

_query_cache = {}
_bq_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutos de caché en memoria
USE_MATERIALIZED = True  # Usar tablas TB_CACHE para máximo rendimiento

def bq_t(name: str) -> str:
    """Helper para conmutar entre vistas y tablas materializadas."""
    if USE_MATERIALIZED:
        return name.replace("VW_LOOKER_", "TB_CACHE_")
    return name

VIEW = f"`{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_VENTAS_DASHBOARD')}`"

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
# DASHBOARD — CONSOLIDADO
# ══════════════════════════════════════════════════════════════════
@app.get("/api/dashboard-init")
def get_dashboard_init(anno: int = 2025):
    """Retorna todos los datos necesarios para la carga inicial en una sola petición."""
    start_t = time.time()
    print(f"🚀 Iniciando consolidado Dashboard para año {anno}...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        f_kpis    = executor.submit(get_kpis, anno)
        f_serie   = executor.submit(get_serie_mensual, anno)
        f_pais    = executor.submit(get_por_pais, anno)
        f_tipo    = executor.submit(get_por_tipo, anno)
        f_marca   = executor.submit(get_por_marca, anno)
        f_top_cli = executor.submit(get_top_clientes, 500)
        f_top_prd = executor.submit(get_top_productos, 6)
        f_vendedores = executor.submit(get_vendedores)
        f_segmentos  = executor.submit(get_segmentos_precio)
        f_margenes   = executor.submit(get_margenes_marca)
        f_rfm        = executor.submit(get_rfm_list)
        f_heat       = executor.submit(get_estacionalidad)
        
        res = {
            "kpis":       f_kpis.result(),
            "serie":      f_serie.result(),
            "pais":       f_pais.result(),
            "tipo":       f_tipo.result(),
            "marca":      f_marca.result(),
            "top_cli":    f_top_cli.result(),
            "top_prd":    f_top_prd.result(),
            "vendedores": f_vendedores.result(),
            "segmentos":  f_segmentos.result(),
            "margenes":   f_margenes.result(),
            "rfm":        f_rfm.result(),
            "heat":       f_heat.result(),
            "period":     anno
        }
    
    end_t = time.time()
    print(f"⌛ Consolidado completado en {end_t - start_t:.2f}s")
    return res

# ══════════════════════════════════════════════════════════════════
# VENTAS — KPIs
# ══════════════════════════════════════════════════════════════════
@app.get("/api/kpis")
def get_kpis(anno: int | None = None):
    # Usamos la vista GLOBAL que ya tiene todo calculado
    if not anno:
        sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_KPI_GLOBAL')}`"
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

@app.get("/api/kpis-ytd")
def get_kpis_ytd():
    """YTD KPIs vs mismo periodo año anterior. Si existe TB_CACHE_KPI_YTD la usa directamente."""
    today        = datetime.date.today()
    current_year = today.year
    prev_year    = current_year - 1
    md           = today.strftime("%m-%d")

    # Intentar leer de la tabla materializada (más rápido)
    cache_table = f"`{BQ_PROJECT}.{BQ_DATASET}.TB_CACHE_KPI_YTD`"
    try:
        cache = run(f"SELECT * FROM {cache_table} WHERE anno = {current_year} LIMIT 1")
        if cache:
            r = cache[0]
            return {
                "current":  {"venta_neta": r.get("venta_neta"), "margen": r.get("margen"),
                             "pct_margen": r.get("pct_margen"), "unidades": r.get("unidades"),
                             "clientes": r.get("clientes"), "pedidos": r.get("pedidos"),
                             "pedido_promedio": r.get("pedido_promedio")},
                "previous": {"venta_neta": r.get("venta_neta_prev"), "margen": r.get("margen_prev"),
                             "pct_margen": r.get("pct_margen_prev"), "unidades": r.get("unidades_prev"),
                             "clientes": r.get("clientes_prev"), "pedidos": r.get("pedidos_prev"),
                             "pedido_promedio": r.get("pedido_promedio_prev")},
                "up_to": str(r.get("fecha_corte", today))
            }
    except Exception:
        pass  # Tabla aún no existe, calcular en vivo

    # Fallback: cálculo en vivo
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
    WHERE ANNO = {{year}}
      AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
    """
    current  = run(sql_base.format(year=current_year))
    previous = run(sql_base.format(year=prev_year))
    return {
        "current":  current[0]  if current  else {},
        "previous": previous[0] if previous else None,
        "up_to":    f"{current_year}-{md}"
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
    FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_SERIE_MENSUAL')}`
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
    sql = f"SELECT pais_origen_producto AS pais, venta_neta, margen_neto AS margen, unidades FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_DIMENSION_PAIS')}` LIMIT 10"
    return run(sql)

@app.get("/api/por-tipo")
def get_por_tipo(anno: int | None = None):
    sql = f"SELECT nombre_tipo_bebida AS tipo, venta_neta, unidades, pct_venta FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_DIMENSION_TIPO')}`"
    return run(sql)

@app.get("/api/por-marca")
def get_por_marca(anno: int | None = None):
    sql = f"SELECT marca_producto AS marca, venta_neta, margen_neto AS margen, unidades FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_DIMENSION_MARCA')}` LIMIT 10"
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# NUEVOS INDICADORES DE ANÁLISIS DE VENTAS (PAGE 2)
# ══════════════════════════════════════════════════════════════════
@app.get("/api/rentabilidad-origen")
def get_rentabilidad_origen(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    SELECT 
      IFNULL(PAIS_ORIGEN_PRODUCTO, 'Sin Origen') AS pais,
      ROUND(SUM(VENTA_NETA_MN), 0) AS venta_neta,
      ROUND(SUM(MARGEN_MN), 0) AS margen_neto,
      SUM(CANTIDAD) AS unidades
    FROM {VIEW}
    {where}
    GROUP BY PAIS_ORIGEN_PRODUCTO
    HAVING venta_neta > 0
    ORDER BY margen_neto DESC
    """
    return run(sql)

@app.get("/api/market-share-tipo")
def get_market_share_tipo(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH totales_pais AS (
      SELECT IFNULL(PAIS_ORIGEN_PRODUCTO, 'Sin Origen') AS pais, SUM(VENTA_NETA_MN) as total_pais
      FROM {VIEW} {where}
      GROUP BY PAIS_ORIGEN_PRODUCTO
    )
    SELECT 
      IFNULL(v.PAIS_ORIGEN_PRODUCTO, 'Sin Origen') AS pais,
      IFNULL(v.NOMBRE_TIPO_BEBIDA, 'Desconocido') AS tipo,
      ROUND(SUM(v.VENTA_NETA_MN), 0) AS venta_neta,
      ROUND(SAFE_DIVIDE(SUM(v.VENTA_NETA_MN), ANY_VALUE(t.total_pais)) * 100, 1) AS share_pct
    FROM {VIEW} v
    JOIN totales_pais t ON IFNULL(v.PAIS_ORIGEN_PRODUCTO, 'Sin Origen') = t.pais
    {where}
    GROUP BY IFNULL(v.PAIS_ORIGEN_PRODUCTO, 'Sin Origen'), IFNULL(v.NOMBRE_TIPO_BEBIDA, 'Desconocido')
    HAVING venta_neta > 0
    ORDER BY pais, share_pct DESC
    """
    return run(sql)

@app.get("/api/dependencia-marca")
def get_dependencia_marca(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH marcas_por_pais AS (
      SELECT 
        IFNULL(PAIS_ORIGEN_PRODUCTO, 'Sin Origen') AS pais, 
        IFNULL(MARCA_PRODUCTO, 'Sin Marca') AS marca, 
        SUM(VENTA_NETA_MN) as venta_marca
      FROM {VIEW} {where}
      GROUP BY PAIS_ORIGEN_PRODUCTO, MARCA_PRODUCTO
      HAVING venta_marca > 0
    ),
    ranqueo AS (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY pais ORDER BY venta_marca DESC) as rn
      FROM marcas_por_pais
    ),
    totales_pais AS (
      SELECT pais, SUM(venta_marca) as total_pais
      FROM marcas_por_pais
      GROUP BY pais
    )
    SELECT 
      r.pais,
      r.marca AS marca_ancla,
      ROUND(r.venta_marca, 0) AS venta_ancla,
      ROUND(t.total_pais, 0) AS venta_total_pais,
      ROUND(SAFE_DIVIDE(r.venta_marca, t.total_pais) * 100, 1) AS dependencia_pct
    FROM ranqueo r
    JOIN totales_pais t ON r.pais = t.pais
    WHERE r.rn = 1 AND t.total_pais > 0
    ORDER BY dependencia_pct DESC
    """
    return run(sql)

@app.get("/api/brand-type-fit")
def get_brand_type_fit(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH top_marcas AS (
      SELECT IFNULL(MARCA_PRODUCTO, 'Sin Marca') AS marca, SUM(VENTA_NETA_MN) as total_marca
      FROM {VIEW} {where}
      GROUP BY MARCA_PRODUCTO
      HAVING total_marca > 0
      ORDER BY total_marca DESC
      LIMIT 15
    )
    SELECT 
      IFNULL(v.MARCA_PRODUCTO, 'Sin Marca') AS marca,
      IFNULL(v.NOMBRE_TIPO_BEBIDA, 'Desconocido') AS tipo,
      ROUND(SUM(v.VENTA_NETA_MN), 0) AS venta_tipo,
      ROUND(SAFE_DIVIDE(SUM(v.VENTA_NETA_MN), ANY_VALUE(t.total_marca)) * 100, 1) AS share_pct
    FROM {VIEW} v
    JOIN top_marcas t ON IFNULL(v.MARCA_PRODUCTO, 'Sin Marca') = t.marca
    {where}
    GROUP BY IFNULL(v.MARCA_PRODUCTO, 'Sin Marca'), IFNULL(v.NOMBRE_TIPO_BEBIDA, 'Desconocido')
    HAVING venta_tipo > 0
    ORDER BY marca, share_pct DESC
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — CLIENTES & PRODUCTOS
# ══════════════════════════════════════════════════════════════════
@app.get("/api/top-clientes")
def get_top_clientes(limit: int = 20):
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_TOP_CLIENTES')}` LIMIT {limit}"
    rows = run(sql)
    for r in rows: r["ultima_compra"] = str(r["ultima_compra"])
    return rows

@app.get("/api/top-clientes-year")
def get_top_clientes_year(anno: int = None, limit: int = 500):
    """Clientes activos en el año indicado. Usa TB_CACHE_TOP_CLIENTES_ANNO si existe."""
    today = datetime.date.today()
    if anno is None:
        anno = today.year

    # Intentar cache materializado (solo para el año actual)
    if anno == today.year:
        try:
            rows = run(f"""
                SELECT cliente, venta_neta, pedidos, unidades, dias_sin_compra, ultima_compra
                FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_CACHE_TOP_CLIENTES_ANNO`
                WHERE anno = {anno}
                ORDER BY venta_neta DESC
                LIMIT {limit}
            """)
            if rows:
                return rows
        except Exception:
            pass  # Tabla aún no existe

    # Fallback: cálculo en vivo
    sql = f"""
    SELECT
      CLIENTE                                           AS cliente,
      ROUND(SUM(VENTA_NETA_MN), 0)                     AS venta_neta,
      COUNT(DISTINCT COD_VENTA)                         AS pedidos,
      SUM(CANTIDAD)                                     AS unidades,
      DATE_DIFF(CURRENT_DATE(), MAX(FECHA), DAY)        AS dias_sin_compra,
      FORMAT_DATE('%Y-%m-%d', MAX(FECHA))               AS ultima_compra
    FROM {VIEW}
    WHERE CAST(ANNO AS INT64) = {anno}
    GROUP BY CLIENTE
    ORDER BY venta_neta DESC
    LIMIT {limit}
    """
    return run(sql)

@app.get("/api/top-productos")
def get_top_productos(limit: int = 20):
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_TOP_PRODUCTOS')}` LIMIT {limit}"
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — ADICIONALES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/vendedores")
def get_vendedores():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_VENDEDORES')}`"
    return run(sql)

@app.get("/api/segmentos-precio")
def get_segmentos_precio():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_SEGMENTO_PRECIO')}`"
    return run(sql)

@app.get("/api/margenes-marca")
def get_margenes_marca():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_MARGEN_MARCA')}`"
    return run(sql)

@app.get("/api/rfm")
def get_rfm_list():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_RFM')}` LIMIT 8"
    rows = run(sql)
    for r in rows: r["ultima_compra"] = str(r["ultima_compra"])
    return rows

@app.get("/api/estacionalidad")
def get_estacionalidad():
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{bq_t('VW_LOOKER_ESTACIONALIDAD')}`"
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
