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
    """Ejecuta SQL y retorna lista de dicts con caché básico."""
    now = time.time()
    
    # 1. Verificar caché
    if sql in _query_cache:
        result, timestamp = _query_cache[sql]
        if now - timestamp < CACHE_TTL:
            return result

    # 2. Consultar a BigQuery (sin Lock global para no bloquear todos los endpoints)
    try:
        query_brief = sql.strip().split("\n")[0][:100] + "..."
        print(f"🔍 Ejecutando BigQuery: {query_brief}")
        start_t = time.time()
        
        query_job = bq.query(sql)
        rows = query_job.result(timeout=30)  # Timeout de 30 segundos
        result = [dict(r) for r in rows]
        
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
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    SELECT
      ROUND(SUM(VENTA_NETA_MN), 2)                          AS venta_neta,
      ROUND(SUM(MARGEN_MN), 2)                               AS margen,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100, 2) AS pct_margen,
      SUM(CANTIDAD)                                          AS unidades,
      COUNT(DISTINCT CLIENTE)                                AS clientes,
      COUNT(DISTINCT COD_VENTA)                              AS pedidos,
      ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN),COUNT(DISTINCT COD_VENTA)), 2) AS pedido_promedio
    FROM {VIEW} {where}
    """
    data = run(sql)
    # También traer año anterior para delta
    if anno:
        prev = run(sql.replace(f"= {anno}", f"= {anno-1}"))
        return {"current": data[0], "previous": prev[0] if prev else None}
    return {"current": data[0], "previous": None}

# ══════════════════════════════════════════════════════════════════
# VENTAS — SERIE MENSUAL
# ══════════════════════════════════════════════════════════════════
@app.get("/api/serie-mensual")
def get_serie_mensual(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    SELECT
      DATE(CAST(ANNO AS INT64), CAST(MES AS INT64), 1)      AS periodo,
      CAST(ANNO AS INT64)                                    AS anno,
      CAST(MES AS INT64)                                     AS mes,
      ROUND(SUM(VENTA_NETA_MN), 2)                          AS venta_neta,
      ROUND(SUM(MARGEN_MN), 2)                               AS margen,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100, 2) AS pct_margen,
      SUM(CANTIDAD)                                          AS unidades,
      COUNT(DISTINCT COD_VENTA)                              AS pedidos,
      ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN),COUNT(DISTINCT COD_VENTA)), 2) AS pedido_promedio
    FROM {VIEW} {where}
    GROUP BY 1,2,3
    ORDER BY 1
    """
    rows = run(sql)
    meses = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun", 7:"Jul", 8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}
    for r in rows:
        r["periodo"] = str(r["periodo"])
        r["mes_label"] = f"{meses.get(r['mes'], '')} '{str(r['anno'])[-2:]}"
    return rows

# ══════════════════════════════════════════════════════════════════
# VENTAS — DIMENSIONES (País, Tipo, Marca)
# ══════════════════════════════════════════════════════════════════
@app.get("/api/por-pais")
def get_por_pais(anno: int | None = None):
    where = f"WHERE CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    SELECT PAIS_ORIGEN_PRODUCTO AS pais,
      ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
      ROUND(SUM(MARGEN_MN),2) AS margen,
      SUM(CANTIDAD) AS unidades
    FROM {VIEW} {where}
    WHERE PAIS_ORIGEN_PRODUCTO IS NOT NULL
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """
    # fix WHERE duplication
    sql = sql.replace(f"FROM {VIEW} {where}\n    WHERE", f"FROM {VIEW}\n    {'AND' if where else 'WHERE'} PAIS_ORIGEN_PRODUCTO IS NOT NULL")
    if anno:
        sql = f"""
        SELECT PAIS_ORIGEN_PRODUCTO AS pais,
          ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
          ROUND(SUM(MARGEN_MN),2) AS margen,
          SUM(CANTIDAD) AS unidades
        FROM {VIEW}
        WHERE CAST(ANNO AS INT64)={anno} AND PAIS_ORIGEN_PRODUCTO IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
    else:
        sql = f"""
        SELECT PAIS_ORIGEN_PRODUCTO AS pais,
          ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
          ROUND(SUM(MARGEN_MN),2) AS margen,
          SUM(CANTIDAD) AS unidades
        FROM {VIEW}
        WHERE PAIS_ORIGEN_PRODUCTO IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
    return run(sql)

@app.get("/api/por-tipo")
def get_por_tipo(anno: int | None = None):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    SELECT NOMBRE_TIPO_BEBIDA AS tipo,
      ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
      ROUND(SUM(MARGEN_MN),2) AS margen,
      SUM(CANTIDAD) AS unidades
    FROM {VIEW}
    WHERE NOMBRE_TIPO_BEBIDA IS NOT NULL {where}
    GROUP BY 1 ORDER BY 2 DESC
    """
    return run(sql)

@app.get("/api/por-marca")
def get_por_marca(anno: int | None = None):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    SELECT MARCA_PRODUCTO AS marca,
      ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
      ROUND(SUM(MARGEN_MN),2) AS margen,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100,2) AS pct_margen,
      SUM(CANTIDAD) AS unidades
    FROM {VIEW}
    WHERE MARCA_PRODUCTO IS NOT NULL {where}
    GROUP BY 1 ORDER BY 2 DESC
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — CLIENTES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/top-clientes")
def get_top_clientes(anno: int | None = None, limit: int = Query(default=20, le=200)):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    WITH base AS (
      SELECT CLIENTE,
        COUNT(DISTINCT COD_VENTA) AS pedidos,
        SUM(CANTIDAD) AS unidades,
        ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
        ROUND(SUM(MARGEN_MN),2) AS margen,
        ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100,2) AS pct_margen,
        ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN),COUNT(DISTINCT COD_VENTA)),2) AS ticket_promedio,
        MAX(FECHA) AS ultima_compra
      FROM {VIEW}
      WHERE CLIENTE IS NOT NULL {where}
      GROUP BY 1
    ),
    total AS (SELECT SUM(venta_neta) AS t FROM base)
    SELECT b.*,
      ROUND(SAFE_DIVIDE(b.venta_neta, t.t)*100, 2) AS share_pct,
      DATE_DIFF(CURRENT_DATE(), b.ultima_compra, DAY) AS dias_inactivo,
      CASE
        WHEN DATE_DIFF(CURRENT_DATE(), b.ultima_compra, DAY) <= 60 AND pedidos >= 5 THEN 'VIP'
        WHEN DATE_DIFF(CURRENT_DATE(), b.ultima_compra, DAY) <= 90 AND pedidos >= 3 THEN 'Leal'
        WHEN DATE_DIFF(CURRENT_DATE(), b.ultima_compra, DAY) <= 90 THEN 'Activo'
        WHEN DATE_DIFF(CURRENT_DATE(), b.ultima_compra, DAY) <= 180 THEN 'En riesgo'
        ELSE 'Inactivo'
      END AS segmento_rfm
    FROM base b, total t
    ORDER BY venta_neta DESC
    """
    rows = run(sql)
    for r in rows:
        r["ultima_compra"] = str(r["ultima_compra"])
    return rows

# ══════════════════════════════════════════════════════════════════
# VENTAS — PRODUCTOS
# ══════════════════════════════════════════════════════════════════
@app.get("/api/top-productos")
def get_top_productos(anno: int | None = None, limit: int = Query(default=20, le=200)):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    WITH base AS (
      SELECT PRODUCTO, NOMBRE_TIPO_BEBIDA, PAIS_ORIGEN_PRODUCTO, MARCA_PRODUCTO,
        SUM(CANTIDAD) AS unidades,
        ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
        ROUND(SUM(MARGEN_MN),2) AS margen,
        ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100,2) AS pct_margen
      FROM {VIEW}
      WHERE PRODUCTO IS NOT NULL {where}
      GROUP BY 1,2,3,4
    ),
    total AS (SELECT SUM(unidades) AS t FROM base)
    SELECT b.*,
      ROUND(SAFE_DIVIDE(b.unidades, t.t)*100, 2) AS pct_uds_total
    FROM base b, total t
    ORDER BY unidades DESC
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — VENDEDORES
# ══════════════════════════════════════════════════════════════════
@app.get("/api/vendedores")
def get_vendedores(anno: int | None = None):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    WITH base AS (
      SELECT VENDEDOR,
        COUNT(DISTINCT COD_VENTA) AS pedidos,
        COUNT(DISTINCT CLIENTE) AS clientes,
        SUM(CANTIDAD) AS unidades,
        ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
        ROUND(SUM(MARGEN_MN),2) AS margen
      FROM {VIEW}
      WHERE VENDEDOR IS NOT NULL {where}
      GROUP BY 1
    ),
    total AS (SELECT SUM(venta_neta) AS t FROM base)
    SELECT b.*,
      ROUND(SAFE_DIVIDE(b.venta_neta, t.t)*100, 2) AS share_pct,
      ROUND(SAFE_DIVIDE(b.venta_neta, b.pedidos), 2) AS ticket_promedio
    FROM base b, total t
    ORDER BY venta_neta DESC
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — MÁRGENES POR MARCA
# ══════════════════════════════════════════════════════════════════
@app.get("/api/margenes-marca")
def get_margenes_marca(anno: int | None = None):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    SELECT MARCA_PRODUCTO AS marca,
      ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
      ROUND(SUM(COSTO_MN_TOTAL),2) AS costo,
      ROUND(SUM(MARGEN_MN),2) AS margen,
      ROUND(SAFE_DIVIDE(SUM(MARGEN_MN),SUM(VENTA_NETA_MN))*100,2) AS pct_margen,
      SUM(CANTIDAD) AS unidades
    FROM {VIEW}
    WHERE MARCA_PRODUCTO IS NOT NULL AND COSTO_MN_TOTAL > 0 {where}
    GROUP BY 1 ORDER BY pct_margen DESC
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — SEGMENTOS DE PRECIO
# ══════════════════════════════════════════════════════════════════
@app.get("/api/segmentos-precio")
def get_segmentos_precio(anno: int | None = None):
    where = f"AND CAST(ANNO AS INT64) = {anno}" if anno else ""
    sql = f"""
    WITH seg AS (
      SELECT
        CASE
          WHEN SAFE_DIVIDE(VENTA_NETA_ME, CANTIDAD) < 20  THEN 'Económico (<US$20)'
          WHEN SAFE_DIVIDE(VENTA_NETA_ME, CANTIDAD) < 50  THEN 'Medio (US$20-50)'
          WHEN SAFE_DIVIDE(VENTA_NETA_ME, CANTIDAD) < 130 THEN 'Premium (US$50-130)'
          ELSE 'Ultra Premium (>US$130)'
        END AS segmento,
        SUM(CANTIDAD) AS botellas,
        ROUND(SUM(VENTA_NETA_MN),2) AS venta_neta,
        ROUND(SUM(MARGEN_MN),2) AS margen
      FROM {VIEW}
      WHERE CANTIDAD > 0 {where}
      GROUP BY 1
    ),
    total AS (SELECT SUM(botellas) AS tb, SUM(venta_neta) AS tv FROM seg)
    SELECT s.*,
      ROUND(SAFE_DIVIDE(s.botellas, t.tb)*100, 2) AS pct_botellas,
      ROUND(SAFE_DIVIDE(s.venta_neta, t.tv)*100, 2) AS pct_revenue,
      ROUND(SAFE_DIVIDE(s.margen, s.venta_neta)*100, 2) AS pct_margen
    FROM seg s, total t
    ORDER BY s.venta_neta DESC
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# VENTAS — ESTACIONALIDAD (heatmap)
# ══════════════════════════════════════════════════════════════════
@app.get("/api/estacionalidad")
def get_estacionalidad():
    sql = f"""
    WITH mensual AS (
      SELECT CAST(ANNO AS INT64) AS anno, CAST(MES AS INT64) AS mes,
        SUM(VENTA_NETA_MN) AS venta
      FROM {VIEW} GROUP BY 1,2
    ),
    anual AS (
      SELECT anno, AVG(venta) AS prom FROM mensual GROUP BY 1
    )
    SELECT m.anno, m.mes, ROUND(m.venta,2) AS venta,
      ROUND(SAFE_DIVIDE(m.venta, a.prom)*100, 1) AS indice
    FROM mensual m JOIN anual a ON m.anno = a.anno
    ORDER BY 1,2
    """
    return run(sql)

# ══════════════════════════════════════════════════════════════════
# FORECAST — Serie total
# ══════════════════════════════════════════════════════════════════
@app.get("/api/forecast-total")
def get_forecast_total():
    try:
        sql = f"""
        SELECT periodo, anno, mes, venta_real, forecast, fc_lo, fc_hi, tipo
        FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_TOTAL`
        ORDER BY periodo
        """
        rows = run(sql)
        for r in rows:
            r["periodo"] = str(r["periodo"])
        return rows
    except Exception:
        # Fallback: datos hardcoded si la tabla aún no existe
        return _forecast_fallback()

def _forecast_fallback():
    hist = [22815,36233,46570,42278,29454,64074,54835,28853,47026,67181,44872,68327,
            39064,52930,61432,79556,60549,67730,84916,78664,95091,76539,49738,68354,
            75865,89990,91242,80278,113097,112285,67892,64767,94805,90460,66676,63136,
            78671,91076,83614,95628,87580,111184,78963,98692,90459,103587,81652,49369]
    from datetime import date
    from dateutil.relativedelta import relativedelta
    start = date(2022, 3, 1)
    rows = []
    for i, v in enumerate(hist):
        p = start + relativedelta(months=i)
        rows.append({"periodo": str(p), "anno": p.year, "mes": p.month,
                     "venta_real": v, "forecast": None, "fc_lo": None, "fc_hi": None, "tipo": "histórico"})
    fc = [(date(2026,3,1),79176,61172,97180),(date(2026,4,1),99097,81093,117101),(date(2026,5,1),98116,80112,116120)]
    for p,f,lo,hi in fc:
        rows.append({"periodo": str(p), "anno": p.year, "mes": p.month,
                     "venta_real": None, "forecast": f, "fc_lo": lo, "fc_hi": hi, "tipo": "pronóstico"})
    return rows

# ══════════════════════════════════════════════════════════════════
# FORECAST — Por producto
# ══════════════════════════════════════════════════════════════════
@app.get("/api/forecast-productos")
def get_forecast_productos():
    try:
        sql = f"""
        SELECT producto, periodo, anno, mes, 
               CAST(ROUND(uds_real) AS INT64) AS uds_real, 
               CAST(ROUND(fc_uds) AS INT64) AS fc_uds, 
               CAST(ROUND(fc_lo) AS INT64) AS fc_lo, 
               CAST(ROUND(fc_hi) AS INT64) AS fc_hi, 
               mes_label, tipo
        FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_FORECAST_PRODUCTOS`
        ORDER BY producto, periodo
        """
        rows = run(sql)
        for r in rows:
            r["periodo"] = str(r["periodo"])
        return rows
    except Exception:
        return []

# ══════════════════════════════════════════════════════════════════
# SIRVE EL FRONTEND (archivos estáticos)
# ══════════════════════════════════════════════════════════════════
frontend_path = Path(__file__).parent.parent / "frontend" / "public"
if frontend_path.exists():
    # Si tienes archivos estáticos extra (imágenes, css separados), crea la carpeta assets y descomenta esto:
    # app.mount("/assets", StaticFiles(directory=str(frontend_path / "assets")), name="assets")

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str):
        return FileResponse(str(frontend_path / "index.html"))

# ══════════════════════════════════════════════════════════════════
# ARRANQUE PROGRAMÁTICO (Para Render)
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    # Render asigna el puerto dinámicamente en la variable de entorno PORT
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
