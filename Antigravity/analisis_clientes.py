
import os, json, datetime
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT   = "dashboard-iconic-terroirs"
BQ_DATASET   = "Mis_Tablas"
CREDENTIALS_PATH = "c:/Dashboard_Iconic/Antigravity/credentials.json"

scopes = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive"
]
creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
bq = bigquery.Client(project=BQ_PROJECT, credentials=creds)

view = f"`{BQ_PROJECT}.{BQ_DATASET}.VW_VENTAS_DASHBOARD`"

sql = f"""
WITH base AS (
  SELECT 
    v.CLIENTE,
    v.ANNO,
    v.NOMBRE_TIPO_BEBIDA,
    v.MARCA_PRODUCTO,
    SUM(v.VENTA_NETA_MN) as venta,
    SUM(v.MARGEN_MN) as margen,
    COUNT(DISTINCT v.COD_VENTA) as pedidos,
    MAX(v.FECHA) as ultima_fecha
  FROM {view} v
  GROUP BY 1, 2, 3, 4
),
customer_summary AS (
  SELECT 
    CLIENTE,
    SUM(venta) as total_historico,
    SUM(margen) as margen_historico,
    SUM(pedidos) as total_pedidos,
    MAX(ultima_fecha) as ultima_compra,
    COUNT(DISTINCT NOMBRE_TIPO_BEBIDA) as categorias_distintas,
    COUNT(DISTINCT MARCA_PRODUCTO) as marcas_distintas,
    MAX(CASE WHEN ANNO = 2026 THEN venta ELSE 0 END) as venta_2026,
    MAX(CASE WHEN ANNO = 2025 THEN venta ELSE 0 END) as venta_2025,
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
  -- 1. Champions Inactivos (VIPs a recuperar hoy)
  COUNTIF(venta_2026 = 0 AND venta_2025 > 20000 AND total_pedidos > 5) as vip_inactivo_2026,
  
  -- 2. Monocultivo (Cross-sell: Solo compran 1 categoría)
  COUNTIF(venta_2026 > 0 AND categorias_distintas = 1 AND total_pedidos > 3) as potencial_cross_sell,
  
  -- 3. Drifters (Frecuencia activa 2025, desaparecidos > 90 días)
  COUNTIF(venta_2025 > 0 AND dias_inactivo BETWEEN 90 AND 180) as frecuencia_en_caida,
  
  -- 4. High LTV Churned (Perdidos >$10K histórico)
  COUNTIF(total_historico > 10000 AND dias_inactivo > 180) as ballenas_perdidas,
  
  -- 5. Compradores de Margen Bajo (Volumen pero poca rentabilidad)
  COUNTIF(venta_2026 > 0 AND pct_margen_avg < 0.20) as volumen_bajo_margen,
  
  -- 6. One-Hit Wonders (1 sola compra histórica)
  COUNTIF(total_pedidos = 1) as una_sola_compra
FROM metrics
"""

query_job = bq.query(sql)
results = query_job.result()
for row in results:
    print(dict(row))
