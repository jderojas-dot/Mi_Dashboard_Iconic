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
import os, json, datetime, time, sys
import uvicorn
from pathlib import Path
from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH, ALLOWED_ORIGINS
from concurrent.futures import ThreadPoolExecutor

# ── Forzar UTF-8 en stdout para evitar errores de encoding en Windows ──
if hasattr(sys.stdout, 'reconfigure'):
    try: sys.stdout.reconfigure(encoding='utf-8')
    except Exception: pass

bq: bigquery.Client | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bq
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON"):
        print("[BQ] Iniciando BigQuery con credenciales de variable de entorno...")
        creds_json = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
        creds = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
        bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    elif CREDENTIALS_PATH and Path(CREDENTIALS_PATH).exists():
        print(f"[BQ] Iniciando BigQuery con archivo: {CREDENTIALS_PATH}")
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)
    else:
        print("[BQ] Iniciando BigQuery sin credenciales explicitas (usando gcloud ADC)...")
        bq = bigquery.Client(project=BQ_PROJECT)
    print(f"[BQ] BigQuery conectado -> proyecto: {BQ_PROJECT}")
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

VIEW_SOURCE = f"`{BQ_PROJECT}.{BQ_DATASET}.TB_CACHE_VENTAS_DASHBOARD`" if USE_MATERIALIZED else f"`{BQ_PROJECT}.{BQ_DATASET}.VW_VENTAS_DASHBOARD`"
VIEW = f"(SELECT * FROM {VIEW_SOURCE} WHERE PAIS_ORIGEN_PRODUCTO IS NOT NULL AND PAIS_ORIGEN_PRODUCTO != '')"

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
        print(f"[BQ] Ejecutando BigQuery: {query_brief}")
        start_t = time.time()
        
        query_job = bq.query(sql)
        rows = query_job.result(timeout=45) 
        
        # Transformamos las claves a minúsculas para consistencia con el frontend
        result = []
        for r in rows:
            d = {k.lower(): v for k, v in dict(r).items()}
            result.append(d)
        
        end_t = time.time()
        print(f"[OK] Query completada en {end_t - start_t:.2f}s ({len(result)} filas)")
        
        # 3. Guardar en caché
        _query_cache[sql] = (result, now)
        return result
    except Exception as e:
        print(f"[ERROR] ERROR en BigQuery: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════
# DASHBOARD — CONSOLIDADO
# ══════════════════════════════════════════════════════════════════
@app.get("/api/dashboard-init")
def get_dashboard_init(anno: int = 2025):
    """Retorna todos los datos necesarios para la carga inicial en una sola petición."""
    start_t = time.time()
    print(f"[INIT] Iniciando consolidado Dashboard para año {anno}...")
    
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
    print(f"[DONE] Consolidado completado en {end_t - start_t:.2f}s")
    return res

# ══════════════════════════════════════════════════════════════════
# VENTAS — KPIs
# ══════════════════════════════════════════════════════════════════
@app.get("/api/kpis")
def get_kpis(anno: int | None = None):
    """
    Retorna los KPIs principales para el año seleccionado.
    Si el año es el actual (2026), aplica filtro YTD (Like-for-Like) para la comparación.
    """
    today = datetime.date.today()
    current_year = today.year
    md = today.strftime("%m-%d")

    # Caso Histórico Total (sin año)
    if not anno or anno == 0:
        sql = f"""
        SELECT
          ROUND(SUM(VENTA_NETA_MN), 0)                          AS venta_neta,
          ROUND(SUM(MARGEN_MN), 0)                               AS margen,
          ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100, 1) AS pct_margen,
          SUM(CANTIDAD)                                          AS unidades,
          COUNT(DISTINCT CLIENTE)                                AS clientes,
          COUNT(DISTINCT COD_VENTA)                              AS pedidos,
          ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN),COUNT(DISTINCT COD_VENTA)), 0) AS pedido_promedio
        FROM {VIEW}
        """
        data = run(sql)
        return {"current": data[0] if data else {}, "previous": None}
    
    # Filtro YTD solo si el año seleccionado es el año actual en curso
    ytd_filter = f"AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'" if anno == current_year else ""

    sql_template = f"""
    SELECT
      ROUND(SUM(VENTA_NETA_MN), 0)                          AS venta_neta,
      ROUND(SUM(MARGEN_MN), 0)                               AS margen,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100, 1) AS pct_margen,
      SUM(CANTIDAD)                                          AS unidades,
      COUNT(DISTINCT CLIENTE)                                AS clientes,
      COUNT(DISTINCT COD_VENTA)                              AS pedidos,
      ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN),COUNT(DISTINCT COD_VENTA)), 0) AS pedido_promedio
    FROM {{view}}
    WHERE CAST(ANNO AS INT64) = {{year}}
    {{extra_filter}}
    """
    
    current = run(sql_template.format(view=VIEW, year=anno, extra_filter=ytd_filter))
    previous = run(sql_template.format(view=VIEW, year=anno-1, extra_filter=ytd_filter))
    
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
    """Clientes activos en el año indicado o histórico total si anno=0."""
    today = datetime.date.today()
    if anno is None:
        anno = today.year

    where_clause = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno > 0 else ""

    # SQL con CTE para cálculo de Share % y Total de Clientes
    sql = f"""
    WITH stats AS (
      SELECT
        CLIENTE                                           AS cliente,
        ROUND(SUM(VENTA_NETA_MN), 0)                     AS venta_neta,
        COUNT(DISTINCT COD_VENTA)                         AS pedidos,
        SUM(CANTIDAD)                                     AS unidades,
        DATE_DIFF(CURRENT_DATE(), MAX(FECHA), DAY)        AS dias_sin_compra,
        FORMAT_DATE('%Y-%m-%d', MAX(FECHA))               AS ultima_compra
      FROM {VIEW}
      {where_clause}
      GROUP BY CLIENTE
    ),
    totales AS (
      SELECT 
        SUM(venta_neta) as gran_total,
        COUNT(*) as total_clientes
      FROM stats
    )
    SELECT 
      s.*,
      t.total_clientes,
      ROUND(SAFE_DIVIDE(s.venta_neta, t.gran_total) * 100, 1) AS share_pct
    FROM stats s, totales t
    ORDER BY venta_neta DESC
    LIMIT {limit if limit > 0 else 1000}
    """
    return run(sql)

@app.get("/api/top-productos-year")
def get_top_productos_year(anno: int = None, limit: int = 500):
    """Productos vendidos en el año o histórico total si anno=0."""
    if anno is None:
        anno = datetime.date.today().year
    
    where_clause = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno > 0 else ""
    
    sql = f"""
    WITH stats AS (
      SELECT 
        PRODUCTO                                           AS producto,
        IFNULL(MARCA_PRODUCTO, 'Sin Marca')               AS marca,
        IFNULL(NOMBRE_TIPO_BEBIDA, 'Desconocido')         AS tipo,
        ROUND(SUM(VENTA_NETA_MN), 0)                     AS venta_neta,
        ROUND(SUM(MARGEN_MN), 0)                           AS margen,
        ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN))*100, 1) AS pct_margen
      FROM {VIEW}
      {where_clause}
      GROUP BY 1, 2, 3
    ),
    totales AS (
      SELECT COUNT(*) as total_productos FROM stats
    )
    SELECT 
      s.*, 
      t.total_productos 
    FROM stats s, totales t
    ORDER BY venta_neta DESC
    LIMIT {limit if limit > 0 else 1000}
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

@app.get("/api/retencion")
def get_retencion():
    sql = f"""
    SELECT CLIENTE, CAST(ANNO AS INT64) AS anno_num, SUM(VENTA_NETA_MN) as venta
    FROM {VIEW}
    WHERE CAST(ANNO AS INT64) >= 2022
    GROUP BY 1, 2
    ORDER BY 2
    """
    try:
        rows = run(sql)
        from collections import defaultdict
        clientes = defaultdict(dict)
        for r in rows:
            clientes[r['anno_num']][r['cliente']] = r['venta']
            
        anios = sorted(list(clientes.keys()))
        res = []
        
        for i, a in enumerate(anios):
            act = clientes[a]
            if i == 0:
                continue
                
            prv = clientes[anios[i-1]]
            retenidos = sum(1 for c in act if c in prv)
            nuevos = sum(1 for c in act if c not in prv)
            perdidos_cli = [c for c in prv if c not in act]
            perdidos = len(perdidos_cli)
            perdida_usd = sum(prv[c] for c in perdidos_cli)
            
            res.append({
                "anno": f"{anios[i-1]}→{a}",
                "clientes_actuales": len(act),
                "retenidos": retenidos,
                "nuevos": nuevos,
                "perdidos": perdidos,
                "perdida_usd": perdida_usd
            })
            
        if anios:
            ultimo_ano = anios[-1]
            act = clientes.get(ultimo_ano, {})
            prv = clientes.get(ultimo_ano - 1, {})
            if ultimo_ano == 2026:
                perdidos_cli = [c for c in prv if c not in act]
                res[-1]["perdidos"] = len(perdidos_cli)
                res[-1]["perdida_usd"] = sum(prv[c] for c in perdidos_cli)
                
        return res
    except Exception as e:
        print("Error en retencion:", e)
        return []

@app.get("/api/retencion-listado")
def get_retencion_listado(p1: int, p2: int, tipo: str):
    """Retorna el listado de clientes para un periodo y tipo (nuevos, retenidos, perdidos)."""
    sql = f"""
    SELECT CLIENTE, CAST(ANNO AS INT64) AS anno_num, SUM(VENTA_NETA_MN) as venta
    FROM {VIEW}
    WHERE CAST(ANNO AS INT64) IN ({p1}, {p2})
    GROUP BY 1, 2
    """
    try:
        rows = run(sql)
        from collections import defaultdict
        clientes = defaultdict(dict)
        for r in rows:
            clientes[r['anno_num']][r['cliente']] = r['venta']
            
        c1 = clientes.get(p1, {})
        c2 = clientes.get(p2, {})
        
        res = []
        if tipo == 'nuevos':
            listado = [c for c in c2 if c not in c1]
            for c in listado:
                res.append({"cliente": c, "venta": c2[c]})
        elif tipo == 'retenidos':
            listado = [c for c in c2 if c in c1]
            for c in listado:
                res.append({"cliente": c, "venta": c2[c], "venta_anterior": c1[c]})
        elif tipo == 'perdidos':
            listado = [c for c in c1 if c not in c2]
            for c in listado:
                res.append({"cliente": c, "venta": c1[c]}) # Venta que se perdió (era la del año anterior)
                
        # Ordenar por venta descendente
        res.sort(key=lambda x: x['venta'], reverse=True)
        return res
    except Exception as e:
        print("Error en listado retencion:", e)
        return []

@app.get("/api/frecuencia-compra")
def get_frecuencia_compra(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH frec AS (
      SELECT CLIENTE, COUNT(DISTINCT COD_VENTA) as pedidos
      FROM {VIEW}
      {where}
      GROUP BY CLIENTE
    )
    SELECT 
      CASE 
        WHEN pedidos = 1 THEN '1 compra'
        WHEN pedidos BETWEEN 2 AND 3 THEN '2-3'
        WHEN pedidos BETWEEN 4 AND 10 THEN '4-10'
        WHEN pedidos BETWEEN 11 AND 50 THEN '11-50'
        ELSE '>50'
      END as rango_frecuencia,
      COUNT(CLIENTE) as cantidad_clientes
    FROM frec
    GROUP BY rango_frecuencia
    ORDER BY 
      CASE rango_frecuencia 
        WHEN '1 compra' THEN 1 WHEN '2-3' THEN 2 
        WHEN '4-10' THEN 3 WHEN '11-50' THEN 4 ELSE 5 END
    """
    return run(sql)

@app.get("/api/rfm-stats")
def get_rfm_stats(anno: int | None = None):
    """Retorna estadísticas por segmento RFM (N° clientes y Valor Soles)."""
    where_clause = f"WHERE SAFE_CAST(v.ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH base AS (
      SELECT 
        v.CLIENTE,
        v.FECHA,
        v.COD_VENTA,
        v.VENTA_NETA_MN,
        IFNULL(SAFE_CAST(c.TIPO_CAMBIO_VENTA AS FLOAT64), 3.75) as tc
      FROM {VIEW} v
      LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.TB_Ventas_Cabecera` c ON v.COD_VENTA = c.COD_VENTA
      {where_clause}
    ),
    customer_metrics AS (
      SELECT
        CLIENTE,
        MAX(FECHA) as ultima_compra,
        COUNT(DISTINCT COD_VENTA) as frecuencia,
        SUM(VENTA_NETA_MN) as total_pen
      FROM base
      GROUP BY CLIENTE
    ),
    segmented AS (
      SELECT
        *,
        CASE
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 60 AND frecuencia >= 10 AND total_pen >= 50000 THEN 'Champions'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 90 AND (frecuencia >= 5 OR total_pen >= 20000) THEN 'Leales'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 60 AND frecuencia < 5 THEN 'Prometedores'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) > 90 AND DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 180 THEN 'En Riesgo'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) > 180 THEN 'Perdidos'
          ELSE 'En Desarrollo'
        END as segmento
      FROM customer_metrics
    )
    SELECT
      segmento,
      COUNT(CLIENTE) as n_clientes,
      ROUND(SUM(total_pen), 0) as valor_pen
    FROM segmented
    GROUP BY segmento
    ORDER BY valor_pen DESC
    """
    return run(sql)

@app.get("/api/rfm-cross-sell")
def get_rfm_cross_sell(anno: int | None = None):
    """Analiza el mix de compra (Cross-sell) Tinto vs Blanco/Espumoso."""
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH client_mix AS (
      SELECT 
        CLIENTE,
        LOGICAL_OR(UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%TINTO%') as has_tinto,
        LOGICAL_OR(UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%BLANCO%') as has_blanco,
        LOGICAL_OR(UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%ESPUMOS%' OR UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%ESPUMANTE%' OR UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%CAVA%' OR UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%CHAMPAGNE%') as has_espumoso,
        SUM(VENTA_NETA_MN) as total_pen
      FROM {VIEW}
      {where}
      GROUP BY CLIENTE
    ),
    mix_categorized AS (
      SELECT
        *,
        CASE
          WHEN has_tinto AND NOT has_blanco AND NOT has_espumoso THEN 'Solo Tinto'
          WHEN has_tinto AND has_blanco AND NOT has_espumoso THEN 'Tinto+Blanco'
          WHEN has_tinto AND NOT has_blanco AND has_espumoso THEN 'Tinto+Espumoso'
          WHEN has_tinto AND has_blanco AND has_espumoso THEN 'Full Portfolio'
          WHEN NOT has_tinto AND (has_blanco OR has_espumoso) THEN 'Blanco/Esp s.Tinto'
          ELSE 'Otros'
        END as mix_type
      FROM client_mix
    )
    SELECT
      mix_type,
      COUNT(CLIENTE) as n_clientes,
      ROUND(AVG(total_pen), 0) as avg_pen,
      SUM(total_pen) as total_pen
    FROM mix_categorized
    GROUP BY mix_type
    ORDER BY n_clientes DESC
    """
    return run(sql)

@app.get("/api/rfm-details")
def get_rfm_details(anno: int | None = None):
    """Retorna la lista detallada de clientes con sus métricas y segmento RFM."""
    where_clause = f"WHERE SAFE_CAST(v.ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH base AS (
      SELECT 
        v.CLIENTE,
        v.FECHA,
        v.COD_VENTA,
        v.VENTA_NETA_MN,
        IFNULL(SAFE_CAST(c.TIPO_CAMBIO_VENTA AS FLOAT64), 3.75) as tc
      FROM {VIEW} v
      LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.TB_Ventas_Cabecera` c ON v.COD_VENTA = c.COD_VENTA
      {where_clause}
    ),
    customer_metrics AS (
      SELECT
        CLIENTE,
        MAX(FECHA) as ultima_compra,
        COUNT(DISTINCT COD_VENTA) as frecuencia,
        SUM(VENTA_NETA_MN) as total_pen
      FROM base
      GROUP BY CLIENTE
    )
    SELECT
      *,
      DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) as dias_sin_compra,
      CASE
        WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 60 AND frecuencia >= 10 AND total_pen >= 50000 THEN 'Champions'
        WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 90 AND (frecuencia >= 5 OR total_pen >= 20000) THEN 'Leales'
        WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 60 AND frecuencia < 5 THEN 'Prometedores'
        WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) > 90 AND DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 180 THEN 'En Riesgo'
        WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) > 180 THEN 'Perdidos'
        ELSE 'En Desarrollo'
      END as segmento
    FROM customer_metrics
    ORDER BY total_pen DESC
    """
    return run(sql)

@app.get("/api/rfm-mix-details")
def get_rfm_mix_details(anno: int | None = None):
    """Retorna la mezcla de tipos de producto (Tinto/Blanco/Esp) por cliente."""
    where_clause = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH rfm_base AS (
      SELECT 
        CLIENTE,
        SUM(VENTA_NETA_MN) as total_pen,
        MAX(FECHA) as ultima_compra,
        COUNT(DISTINCT COD_VENTA) as frecuencia,
        SUM(CASE WHEN UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%TINTO%' THEN VENTA_NETA_MN ELSE 0 END) as tinto_pen,
        SUM(CASE WHEN UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%BLANCO%' THEN VENTA_NETA_MN ELSE 0 END) as blanco_pen,
        SUM(CASE WHEN UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%ESPUMOS%' OR UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%ESPUMANTE%' OR UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%CAVA%' OR UPPER(NOMBRE_TIPO_BEBIDA) LIKE '%CHAMPAGNE%' THEN VENTA_NETA_MN ELSE 0 END) as espumoso_pen
      FROM {VIEW}
      {where_clause}
      GROUP BY CLIENTE
    ),
    segmented_mix AS (
      SELECT
        *,
        CASE
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 60 AND frecuencia >= 10 AND total_pen >= 50000 THEN 'Champions'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 90 AND (frecuencia >= 5 OR total_pen >= 20000) THEN 'Leales'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 60 AND frecuencia < 5 THEN 'Prometedores'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) > 90 AND DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) <= 180 THEN 'En Riesgo'
          WHEN DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) > 180 THEN 'Perdidos'
          ELSE 'En Desarrollo'
        END as segmento,
        ROUND((tinto_pen / NULLIF(total_pen, 0)) * 100, 1) as pct_tinto,
        ROUND((blanco_pen / NULLIF(total_pen, 0)) * 100, 1) as pct_blanco,
        ROUND((espumoso_pen / NULLIF(total_pen, 0)) * 100, 1) as pct_espumoso
      FROM rfm_base
    ),
    mix_categorized AS (
      SELECT
        *,
        CASE
          WHEN tinto_pen > 0 AND blanco_pen = 0 AND espumoso_pen = 0 THEN 'Solo Tinto'
          WHEN tinto_pen > 0 AND blanco_pen > 0 AND espumoso_pen = 0 THEN 'Tinto+Blanco'
          WHEN tinto_pen > 0 AND blanco_pen = 0 AND espumoso_pen > 0 THEN 'Tinto+Espumoso'
          WHEN tinto_pen > 0 AND blanco_pen > 0 AND espumoso_pen > 0 THEN 'Full Portfolio'
          WHEN tinto_pen = 0 AND (blanco_pen > 0 OR espumoso_pen > 0) THEN 'Blanco/Esp s.Tinto'
          ELSE 'Otros'
        END as mix_type
      FROM segmented_mix
    )
    SELECT * FROM mix_categorized ORDER BY total_pen DESC
    """
    return run(sql)

@app.get("/api/estrategias-ia")
def get_estrategias_ia(anno: int = 2026):
    try:
        kpis = run(f"SELECT SUM(VENTA_NETA_MN) as total, COUNT(DISTINCT CLIENTE) as clientes FROM {VIEW} WHERE CAST(ANNO AS INT64) = {anno}")
        total_venta = kpis[0]['total'] if kpis and kpis[0]['total'] else 1
        tinto = run(f"SELECT COUNT(DISTINCT CLIENTE) as cli FROM {VIEW} WHERE CAST(ANNO AS INT64) = {anno} AND NOMBRE_TIPO_BEBIDA LIKE '%Tinto%' AND CLIENTE NOT IN (SELECT CLIENTE FROM {VIEW} WHERE CAST(ANNO AS INT64) = {anno} AND NOMBRE_TIPO_BEBIDA LIKE '%Blanco%')")
        cli_solo_tinto = tinto[0]['cli'] if tinto else 59
    except Exception as e:
        print("Error en IA dynamica:", e)
        total_venta = 1000000
        cli_solo_tinto = 59
    potencial_tinto = f"S/ {int(cli_solo_tinto * (total_venta/10000))}K" if total_venta > 1 else "S/ 99K"
    return {
        "competencia": [
            {"competidor": "GW Yichang & Cía", "enfoque": "Volumen masivo, todas las gamas", "fortaleza": "~20% del mercado importado, red supermercados", "amenaza": "Alta", "color": "red"},
            {"competidor": "Perufarma", "enfoque": "Distribución amplia, medio-alto", "fortaleza": "Red HoReCa y retail consolidada", "amenaza": "Alta", "color": "red"},
            {"competidor": "Best Brands", "enfoque": "Exclusividad, alta gama", "fortaleza": "Marcas exclusivas, posicionamiento premium", "amenaza": "Media", "color": "amber"},
            {"competidor": "LC Group / KC Trading", "enfoque": "Importación selectiva", "fortaleza": "Mix europeo curado", "amenaza": "Media", "color": "amber"},
            {"competidor": "Vino Tinto SAC", "enfoque": "Vinos franceses", "fortaleza": "Especialización Francia, sommelier", "amenaza": "Baja", "color": "blue"},
            {"competidor": "Chile (ProChile)", "enfoque": "Vino chileno masivo-premium", "fortaleza": "+32% crecimiento, precio competitivo", "amenaza": "Alta", "color": "red"},
            {"competidor": "Argentina (Malbec)", "enfoque": "Vinos argentinos masivos", "fortaleza": "Dominio segmento volumen Perú", "amenaza": "Media", "color": "amber"}
        ],
        "estrategias": [
            {"id":1, "nombre": "Cross-sell Tinto → Blanco", "desc": "para clientes que solo compran tinto", "palanca": f"{cli_solo_tinto} clientes identificados, potencial {potencial_tinto}", "impacto": "+ S/ 50-90K", "color": "green"},
            {"id":2, "nombre": "Programa Champions", "desc": "wine tastings privados, acceso anticipado", "palanca": "Top 15 Champions = alto revenue total", "impacto": "Retención crítica", "color": "green"},
            {"id":3, "nombre": "Reactivación 'En riesgo'", "desc": "clientes inactivos con compras altas pasadas", "palanca": ">50 clientes en riesgo identif.", "impacto": "+ S/ 30-70K", "color": "amber"},
            {"id":4, "nombre": "Potenciar Alemania y China", "desc": "márgenes altos (>68%) pero portafolio pequeño", "palanca": "Alemania/China como diversificación", "impacto": "+3-5% margen mix", "color": "green"},
            {"id":5, "nombre": "Convertir mono-compra", "desc": "secuencia de nurturing y 2da compra", "palanca": "Identificar clientes de 1 solo pedido", "impacto": "+ S/ 20-40K", "color": "amber"},
            {"id":6, "nombre": "Desarrollar canal retail D2C", "desc": "via web — enfocado en consumidor final", "palanca": "Brecha en canal directo D2C", "impacto": "Nuevo canal", "color": "blue"},
            {"id":7, "nombre": "Contrarrestar Chile/Arg", "desc": "con narrativa de terroir único italiano", "palanca": "Terroir vs Volumen", "impacto": "Defensa mercado", "color": "amber"},
            {"id":8, "nombre": "Revisar márgenes Francia", "desc": "ajustar PVP o negociar mejor costo", "palanca": "Francia tiene el menor margen", "impacto": "+2-4% margen", "color": "amber"},
            {"id":9, "nombre": "Estacionalidad anticipada", "desc": "pre-cargar stock para meses pico", "palanca": "Enero y Noviembre concentran demanda", "impacto": "Eficiencia logíst.", "color": "blue"},
            {"id":10, "nombre": "Riesgo de concentración", "desc": "dependencia de un solo vendedor top", "palanca": "Vendedor top lidera share >50%", "impacto": "Riesgo operativo", "color": "red"}
        ]
    }

@app.get("/api/forecast-total")
def get_forecast_total():
    try:
        sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_TOTAL` ORDER BY periodo"
        rows = run(sql)
        for r in rows: r["periodo"] = str(r["periodo"])
        return rows
    except Exception: return []

@app.get("/api/forecast-productos")
def get_forecast_productos(producto: str = None):
    """Retorna datos de pronóstico por producto. Acepta filtro opcional por nombre de producto."""
    try:
        if producto:
            # Sanitizar: escapar comillas simples
            safe = producto.replace("'", "''")
            where = f"WHERE producto = '{safe}'"
        else:
            where = ""
        sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_PRODUCTOS` {where} ORDER BY producto, periodo"
        rows = run(sql)
        for r in rows: r["periodo"] = str(r["periodo"])
        return rows
    except Exception as e:
        print(f"Error en forecast-productos: {e}")
        return []

@app.get("/api/forecast-catalogo")
def get_forecast_catalogo():
    """Retorna lista de marca+producto unicos para popular los filtros del dashboard.
    Compatible con la tabla antigua (sin columna marca) y la nueva (con marca).
    """
    # Intentar con columna marca (tabla nueva generada por Chronos 2)
    try:
        sql = f"""
        SELECT DISTINCT
          IFNULL(marca, 'Sin Marca') AS marca,
          producto
        FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_PRODUCTOS`
        WHERE tipo = 'pronostico' OR tipo = 'pron\u00f3stico'
        ORDER BY marca, producto
        """
        rows = run(sql)
        if rows:
            return rows
    except Exception:
        pass
    # Fallback: tabla antigua sin columna marca
    try:
        sql2 = f"""
        SELECT DISTINCT
          'Sin Marca' AS marca,
          producto
        FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_PRODUCTOS`
        ORDER BY producto
        """
        return run(sql2)
    except Exception as e:
        print(f"Error en forecast-catalogo: {str(e)[:200]}")
        return []

@app.get("/api/productos-mix-pais")
def get_productos_mix_pais(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    SELECT 
      IFNULL(PAIS_ORIGEN_PRODUCTO, 'Sin Origen') AS pais,
      ROUND(SUM(VENTA_NETA_MN), 0) AS venta_neta,
      ROUND(SUM(COSTO_MN_TOTAL), 0) AS costo_total,
      ROUND(SUM(MARGEN_MN), 0) AS margen_neto,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN))*100, 1) AS pct_margen,
      COUNT(DISTINCT NOMBRE_TIPO_BEBIDA) AS lineas,
      COUNT(DISTINCT PRODUCTO) AS skus
    FROM {VIEW}
    {where}
    GROUP BY PAIS_ORIGEN_PRODUCTO
    HAVING venta_neta > 0
    ORDER BY venta_neta DESC
    """
    rows = run(sql)
    for r in rows:
        # Lógica de 'Acción'
        if r['pct_margen'] > 40 and r['venta_neta'] > 100000:
            r['accion'] = "Estrella principal"
        elif r['pct_margen'] > 30 and r['venta_neta'] > 50000:
            r['accion'] = "Consolidar"
        elif r['pct_margen'] < 20 and r['venta_neta'] > 50000:
            r['accion'] = "Revisar precios"
        elif r['pct_margen'] > 40 and r['venta_neta'] < 50000:
            r['accion'] = "Ampliar impulso"
        elif r['skus'] < 5 and r['pct_margen'] > 30:
            r['accion'] = "Ampliar portafolio"
        elif r['pct_margen'] < 15:
            r['accion'] = "Reestructurar"
        else:
            r['accion'] = "Mantener"
    return rows

@app.get("/api/productos-all")
def get_productos_all(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH stats AS (
      SELECT 
        PRODUCTO AS producto,
        IFNULL(MARCA_PRODUCTO, 'Sin Marca') AS marca,
        ROUND(SUM(VENTA_NETA_MN), 0) AS venta_neta
      FROM {VIEW}
      {where}
      GROUP BY 1, 2
      HAVING venta_neta > 0
    ),
    totales AS (
      SELECT SUM(venta_neta) as gran_total FROM stats
    )
    SELECT 
      s.*, 
      ROUND(SAFE_DIVIDE(s.venta_neta, t.gran_total) * 100, 1) AS share_pct
    FROM stats s, totales t
    ORDER BY venta_neta DESC
    """
    return run(sql)

@app.get("/api/marcas-all")
def get_marcas_all(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH stats AS (
      SELECT 
        IFNULL(MARCA_PRODUCTO, 'Sin Marca') AS marca,
        ROUND(SUM(VENTA_NETA_MN), 0) AS venta_neta
      FROM {VIEW}
      {where}
      GROUP BY 1
      HAVING venta_neta > 0
    ),
    totales AS (
      SELECT SUM(venta_neta) as gran_total FROM stats
    )
    SELECT 
      s.*, 
      ROUND(SAFE_DIVIDE(s.venta_neta, t.gran_total) * 100, 1) AS share_pct
    FROM stats s, totales t
    ORDER BY venta_neta DESC
    """
    return run(sql)

@app.get("/api/segmentos-precio-year")
def get_segmentos_precio_year(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno and anno != 0 else ""
    sql = f"""
    WITH base AS (
      SELECT 
        VENTA_NETA_MN, 
        CANTIDAD,
        SAFE_DIVIDE(VENTA_NETA_MN, CANTIDAD) as precio_unitario
      FROM {VIEW}
      {where}
    )
    SELECT
      CASE 
        WHEN precio_unitario < 69 THEN '<S/ 69'
        WHEN precio_unitario BETWEEN 69 AND 149.99 THEN 'S/ 69-150'
        WHEN precio_unitario BETWEEN 150 AND 299.99 THEN 'S/ 150-300'
        WHEN precio_unitario BETWEEN 300 AND 600 THEN 'S/ 300-600'
        ELSE '>S/ 600'
      END as segmento,
      CASE 
        WHEN precio_unitario < 69 THEN 1
        WHEN precio_unitario BETWEEN 69 AND 149.99 THEN 2
        WHEN precio_unitario BETWEEN 150 AND 299.99 THEN 3
        WHEN precio_unitario BETWEEN 300 AND 600 THEN 4
        ELSE 5
      END as orden,
      ROUND(SUM(VENTA_NETA_MN), 0) as venta_neta,
      SUM(CANTIDAD) as botellas
    FROM base
    WHERE precio_unitario IS NOT NULL
    GROUP BY 1, 2
    ORDER BY orden
    """
    return run(sql)

@app.get("/api/analisis-experto-clientes")
def get_analisis_experto():
    """Retorna los 6 segmentos estratégicos para recuperación y crecimiento."""
    sql = f"""
    WITH base AS (
      SELECT 
        v.CLIENTE,
        v.ANNO,
        v.NOMBRE_TIPO_BEBIDA,
        SUM(v.VENTA_NETA_MN) as venta,
        SUM(v.MARGEN_MN) as margen,
        COUNT(DISTINCT v.COD_VENTA) as pedidos,
        MAX(v.FECHA) as ultima_fecha
      FROM {VIEW} v
      GROUP BY 1, 2, 3
    ),
    customer_summary AS (
      SELECT 
        CLIENTE,
        SUM(venta) as total_historico,
        SUM(pedidos) as total_pedidos,
        MAX(ultima_fecha) as ultima_compra,
        COUNT(DISTINCT NOMBRE_TIPO_BEBIDA) as categorias_distintas,
        MAX(CASE WHEN CAST(ANNO AS INT64) = 2026 THEN venta ELSE 0 END) as venta_2026,
        MAX(CASE WHEN CAST(ANNO AS INT64) = 2025 THEN venta ELSE 0 END) as venta_2025,
        SAFE_DIVIDE(SUM(margen), SUM(venta)) as pct_margen_avg
      FROM base
      GROUP BY CLIENTE
    ),
    metrics AS (
      SELECT
        *,
        DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY) as dias_inactivo
      FROM customer_summary
    )
    SELECT
      'VIPs en Fuga (Inactivos 2026)' as segmento,
      COUNTIF(venta_2026 = 0 AND venta_2025 > 10000 AND total_pedidos > 5) as n_clientes,
      'Llamada de Gerencia + Invitación a cata privada de nuevas añadas' as accion,
      'Alta' as prioridad
    FROM metrics
    UNION ALL
    SELECT
      'Ballenas Perdidas (Histórico >10K)' as segmento,
      COUNTIF(total_historico > 10000 AND dias_inactivo > 180) as n_clientes,
      'Visita comercial presencial con oferta de relanzamiento de cuenta' as accion,
      'Alta' as prioridad
    FROM metrics
    UNION ALL
    SELECT
      'Deriva de Frecuencia' as segmento,
      COUNTIF(venta_2025 > 0 AND dias_inactivo BETWEEN 90 AND 180) as n_clientes,
      'Email "Te extrañamos" con recomendación personalizada basada en historial' as accion,
      'Media' as prioridad
    FROM metrics
    UNION ALL
    SELECT
      'Potencial Cross-sell (Monocultivo)' as segmento,
      COUNTIF(venta_2026 > 0 AND categorias_distintas = 1 AND total_pedidos > 3) as n_clientes,
      'Envío de muestras de otras categorías (Blancos/Espumosos) + cupón combo' as accion,
      'Media' as prioridad
    FROM metrics
    UNION ALL
    SELECT
      'Reactivación (1 sola compra)' as segmento,
      COUNTIF(total_pedidos = 1) as n_clientes,
      'Descuento en 2da compra + Guía de maridaje digital interactiva' as accion,
      'Normal' as prioridad
    FROM metrics
    UNION ALL
    SELECT
      'Optimización de Margen' as segmento,
      COUNTIF(venta_2026 > 0 AND pct_margen_avg < 0.20) as n_clientes,
      'Asesoría para migración guiada a etiquetas Premium de mayor rentabilidad' as accion,
      'Media' as prioridad
    FROM metrics
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# MÓDULO FINANCIERO — PERÚ PCGE
# ══════════════════════════════════════════════════════════════════

@app.get("/api/fin-balance")
def get_fin_balance(anno: int = 2026):
    """Reporte de Situación Patrimonial (Balance General) mensualizado."""
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_FIN_BALANCE` WHERE ejercicio = {anno}"
    rows = run(sql)
    return {"rows": rows, "metadata": {"generacion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}

@app.get("/api/fin-resultados")
def get_fin_resultados(anno: int = 2026):
    """Estado de Resultados por Naturaleza mensualizado."""
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_FIN_RESULTADOS` WHERE ejercicio = {anno}"
    rows = run(sql)
    return {"rows": rows, "metadata": {"generacion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}

@app.get("/api/fin-flujo")
def get_fin_flujo(anno: int = 2026):
    """Estado de Flujo de Efectivo (Método Indirecto - Movimientos Cuenta 10)."""
    sql = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.VW_FIN_FLUJO` WHERE ejercicio = {anno}"
    rows = run(sql)
    return {"rows": rows, "metadata": {"generacion": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}

@app.get("/api/fin-detalle")
def get_fin_detalle(anno: int, cta: str, periodo: int | None = None):
    """Drill-down: Detalle de transacciones para una cuenta y periodo."""
    where_periodo = f"AND Periodo = {periodo}" if periodo is not None else ""
    sql = f"""
    SELECT 
      Fec_Mov as fecha,
      Reg_Ctb as registro,
      Glosa as glosa,
      Tip_Doc as doc_tipo,
      Nro_Doc as doc_numero,
      IFNULL(Cliente, Proveedor) as entidad,
      Mto_Debe as debe,
      Mto_Haber as haber,
      Mda_Origen as moneda
    FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_movimientos_contabilidad`
    WHERE Ejercicio = {anno}
      AND Nro_Cta = '{cta}'
      {where_periodo}
    ORDER BY Fec_Mov
    LIMIT 1000
    """
    return run(sql)

frontend_path = Path(__file__).parent.parent / "frontend" / "public"

# Servir index.html en la raíz
@app.get("/")
def read_root():
    return FileResponse(str(frontend_path / "index.html"))

# Montar archivos estáticos (CSS, JS, imágenes, etc.) bajo /static
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
