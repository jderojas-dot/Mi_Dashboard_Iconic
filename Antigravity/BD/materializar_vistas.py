"""
╔══════════════════════════════════════════════════════════════════════╗
║   ICONIC TERROIRS — Script de Materialización de Vistas             ║
║   Convierte vistas lentas (Views) en tablas rápidas (Tables)        ║
╚══════════════════════════════════════════════════════════════════════╝
Uso: python materializar_vistas.py
"""

import os
import sys
import time
import datetime
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Importar config del backend
sys.path.append(str(Path(__file__).parent.parent / "backend"))
try:
    from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH
except ImportError:
    print("❌ No se pudo cargar config.py. Usando valores por defecto.")
    BQ_PROJECT = "dashboard-iconic-terroirs"
    BQ_DATASET = "Mis_Tablas"
    CREDENTIALS_PATH = str(Path(__file__).parent.parent / "credentials.json")

# ── Tabla base (ya materializada) ────────────────────────────────────────────
BASE_VIEW = f"`{BQ_PROJECT}.{BQ_DATASET}.TB_CACHE_VENTAS_DASHBOARD`"

# ── 1. Vistas estándar → TB_CACHE_*  ──────────────────────────────────────────
VISTAS_A_MATERIALIZAR = [
    "VW_VENTAS_DASHBOARD",       # Base de todas las consultas
    "VW_LOOKER_KPI_GLOBAL",
    "VW_LOOKER_SERIE_MENSUAL",
    "VW_LOOKER_DIMENSION_PAIS",
    "VW_LOOKER_DIMENSION_TIPO",
    "VW_LOOKER_DIMENSION_MARCA",
    "VW_LOOKER_TOP_CLIENTES",
    "VW_LOOKER_TOP_PRODUCTOS",
    "VW_LOOKER_VENDEDORES",
    "VW_LOOKER_SEGMENTO_PRECIO",
    "VW_LOOKER_MARGEN_MARCA",
    "VW_LOOKER_RFM",
    "VW_LOOKER_ESTACIONALIDAD"
]

# ── 2. Consultas personalizadas → nuevas tablas TB_CACHE_*  ───────────────────
# Se calculan dinámicamente con la fecha de hoy.
# ⚠️  AGREGAR AQUÍ cada nuevo endpoint que use SQL directo sobre la vista base.
def build_custom_queries():
    today        = datetime.date.today()
    current_year = today.year
    md           = today.strftime("%m-%d")          # "03-21"
    prev_year    = current_year - 1

    return [

        # ── KPIs YTD (Year-to-Date vs mismo periodo año anterior)  ──────────
        # Alimenta: /api/kpis-ytd
        {
            "tabla": "TB_CACHE_KPI_YTD",
            "descripcion": f"KPIs YTD {current_year} vs {prev_year} (corte {today})",
            "sql": f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.TB_CACHE_KPI_YTD` AS
            SELECT
              {current_year}                                                       AS anno,
              '{today.isoformat()}'                                                AS fecha_corte,
              -- Año actual
              ROUND(SUM(CASE WHEN CAST(ANNO AS INT64) = {current_year}
                               AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                             THEN VENTA_NETA_MN END), 0)                          AS venta_neta,
              ROUND(SUM(CASE WHEN CAST(ANNO AS INT64) = {current_year}
                               AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                             THEN MARGEN_MN END), 0)                              AS margen,
              ROUND(SAFE_DIVIDE(
                SUM(CASE WHEN CAST(ANNO AS INT64) = {current_year}
                           AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN MARGEN_MN END),
                SUM(CASE WHEN CAST(ANNO AS INT64) = {current_year}
                           AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN VENTA_NETA_MN END)
              ) * 100, 1)                                                          AS pct_margen,
              SUM(CASE WHEN CAST(ANNO AS INT64) = {current_year}
                         AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                       THEN CANTIDAD END)                                          AS unidades,
              COUNT(DISTINCT CASE WHEN CAST(ANNO AS INT64) = {current_year}
                                    AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                                  THEN CLIENTE END)                                AS clientes,
              COUNT(DISTINCT CASE WHEN CAST(ANNO AS INT64) = {current_year}
                                    AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                                  THEN COD_VENTA END)                              AS pedidos,
              ROUND(SAFE_DIVIDE(
                SUM(CASE WHEN CAST(ANNO AS INT64) = {current_year}
                           AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN VENTA_NETA_MN END),
                COUNT(DISTINCT CASE WHEN CAST(ANNO AS INT64) = {current_year}
                                      AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN COD_VENTA END)
              ), 0)                                                                AS pedido_promedio,
              -- Año anterior (mismo corte)
              ROUND(SUM(CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                               AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                             THEN VENTA_NETA_MN END), 0)                          AS venta_neta_prev,
              ROUND(SUM(CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                               AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                             THEN MARGEN_MN END), 0)                              AS margen_prev,
              ROUND(SAFE_DIVIDE(
                SUM(CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                           AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN MARGEN_MN END),
                SUM(CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                           AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN VENTA_NETA_MN END)
              ) * 100, 1)                                                          AS pct_margen_prev,
              SUM(CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                         AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                       THEN CANTIDAD END)                                          AS unidades_prev,
              COUNT(DISTINCT CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                                    AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                                  THEN CLIENTE END)                                AS clientes_prev,
              COUNT(DISTINCT CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                                    AND FORMAT_DATE('%m-%d', FECHA) <= '{md}'
                                  THEN COD_VENTA END)                              AS pedidos_prev,
              ROUND(SAFE_DIVIDE(
                SUM(CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                           AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN VENTA_NETA_MN END),
                COUNT(DISTINCT CASE WHEN CAST(ANNO AS INT64) = {prev_year}
                                      AND FORMAT_DATE('%m-%d', FECHA) <= '{md}' THEN COD_VENTA END)
              ), 0)                                                                AS pedido_promedio_prev
            FROM {BASE_VIEW}
            """
        },

        # ── Top Clientes del año en curso  ───────────────────────────────────
        # Alimenta: /api/top-clientes-year
        {
            "tabla": "TB_CACHE_TOP_CLIENTES_ANNO",
            "descripcion": f"Top clientes {current_year} con campos RFM",
            "sql": f"""
            CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.TB_CACHE_TOP_CLIENTES_ANNO` AS
            SELECT
              CLIENTE                                                  AS cliente,
              ROUND(SUM(VENTA_NETA_MN), 0)                           AS venta_neta,
              COUNT(DISTINCT COD_VENTA)                               AS pedidos,
              SUM(CANTIDAD)                                            AS unidades,
              DATE_DIFF(CURRENT_DATE(), MAX(FECHA), DAY)              AS dias_sin_compra,
              FORMAT_DATE('%Y-%m-%d', MAX(FECHA))                     AS ultima_compra,
              {current_year}                                           AS anno
            FROM {BASE_VIEW}
            WHERE CAST(ANNO AS INT64) = {current_year}
            GROUP BY CLIENTE
            ORDER BY venta_neta DESC
            LIMIT 500
            """
        },

    ]


def get_client():
    """Obtener cliente de BigQuery con manejo robusto de credenciales."""
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if CREDENTIALS_PATH and os.path.exists(CREDENTIALS_PATH):
        print(f"🔑 Usando archivo de credenciales: {CREDENTIALS_PATH}")
        try:
            creds = service_account.Credentials.from_service_account_file(
                CREDENTIALS_PATH, scopes=scopes
            )
            return bigquery.Client(project=BQ_PROJECT, credentials=creds)
        except Exception as e:
            print(f"⚠️ Error al cargar JSON ({e}). Intentando GCloud ADC...")

    print("☁️ Usando Google Cloud Application Default Credentials (ADC)...")
    return bigquery.Client(project=BQ_PROJECT)


def materializar():
    try:
        client = get_client()
        client.list_datasets(max_results=1)
        print(f"✅ Conexión establecida con éxito.")
    except Exception as e:
        print(f"❌ FALLO CRÍTICO DE CONEXIÓN: {e}")
        print("\n💡 Sugerencia: ejecuta:  gcloud auth application-default login")
        return

    print(f"🚀 Iniciando materialización en {BQ_PROJECT}.{BQ_DATASET}...")
    start_total = time.time()

    # ── 1. Vistas estándar ────────────────────────────────────────────────────
    print("\n📦 [1/2] Materializando vistas estándar...")
    for vista in VISTAS_A_MATERIALIZAR:
        tabla = vista.replace("VW_LOOKER_", "TB_CACHE_").replace("VW_VENTAS_", "TB_CACHE_VENTAS_")
        query = (
            f"CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.{tabla}` "
            f"AS SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{vista}`"
        )
        print(f"   ⏳ {vista} → {tabla}...", end=" ", flush=True)
        t0 = time.time()
        try:
            client.query(query).result()
            print(f"✅ ({time.time() - t0:.2f}s)")
        except Exception as e:
            print(f"❌ Error: {e}")

    # ── 2. Consultas personalizadas ───────────────────────────────────────────
    print("\n🔧 [2/2] Materializando consultas personalizadas (fecha de hoy)...")
    for item in build_custom_queries():
        print(f"   ⏳ {item['descripcion']} → {item['tabla']}...", end=" ", flush=True)
        t0 = time.time()
        try:
            client.query(item["sql"]).result()
            print(f"✅ ({time.time() - t0:.2f}s)")
        except Exception as e:
            print(f"❌ Error: {e}")

    print(f"\n✨ Materialización completada en {time.time() - start_total:.2f}s")
    print("👉 El dashboard ahora cargará instantáneamente usando estas tablas.")


if __name__ == "__main__":
    materializar()
