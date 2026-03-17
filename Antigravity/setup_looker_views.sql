-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║   ICONIC TERROIRS — Vistas BigQuery para Looker Studio              ║
-- ║   Proyecto: dashboard-iconic-terroirs                                ║
-- ║   Dataset:  Mis_Tablas                                               ║
-- ║   Fuente:   VW_VENTAS_DASHBOARD                                      ║
-- ╠══════════════════════════════════════════════════════════════════════╣
-- ║   Ejecutar este script completo en BigQuery Console para crear       ║
-- ║   todas las vistas que alimentarán el dashboard de Looker Studio.    ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- ═══════════════════════════════════════════════════════════════════════
-- 1. VW_LOOKER_KPI_GLOBAL
--    Alimenta: KPI Strip (7 scorecards en Looker Studio)
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_KPI_GLOBAL` AS
WITH base AS (
  SELECT
    ANNO,
    MES,
    VENTA_NETA_MN,
    MARGEN_MN,
    CANTIDAD,
    COD_VENTA,
    CLIENTE
  FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
),
metricas AS (
  SELECT
    ROUND(SUM(VENTA_NETA_MN), 0)         AS venta_neta_total,
    ROUND(SUM(MARGEN_MN), 0)              AS margen_neto_total,
    ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN)) * 100, 1)
                                           AS pct_margen_bruto,
    SUM(CANTIDAD)                          AS unidades_total,
    COUNT(DISTINCT CLIENTE)                AS clientes_activos,
    COUNT(DISTINCT COD_VENTA)              AS num_pedidos,
    ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN), COUNT(DISTINCT COD_VENTA)), 0)
                                           AS aov
  FROM base
)
SELECT * FROM metricas;


-- ═══════════════════════════════════════════════════════════════════════
-- 2. VW_LOOKER_SERIE_MENSUAL
--    Alimenta: Gráfico de líneas "Venta Neta por Mes" con toggle de
--    métricas (Venta, Margen, Unidades, Pedidos, AOV)
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_SERIE_MENSUAL` AS
SELECT
  DATE(ANNO, MES, 1)                                           AS periodo,
  ANNO,
  MES,
  FORMAT_DATE('%b %Y', DATE(ANNO, MES, 1))                     AS periodo_label,
  ROUND(SUM(VENTA_NETA_MN), 2)                                 AS venta_neta,
  ROUND(SUM(MARGEN_MN), 2)                                     AS margen_neto,
  SUM(CANTIDAD)                                                 AS unidades,
  COUNT(DISTINCT COD_VENTA)                                     AS pedidos,
  ROUND(SAFE_DIVIDE(SUM(VENTA_NETA_MN), COUNT(DISTINCT COD_VENTA)), 2)
                                                                AS aov
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1, 2, 3, 4
ORDER BY 1;


-- ═══════════════════════════════════════════════════════════════════════
-- 3. VW_LOOKER_DIMENSION_PAIS
--    Alimenta: Gráfico de barras horizontales "Venta Neta por País"
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_DIMENSION_PAIS` AS
SELECT
  PAIS_ORIGEN_PRODUCTO,
  ROUND(SUM(VENTA_NETA_MN), 0)       AS venta_neta,
  ROUND(SUM(MARGEN_MN), 0)           AS margen_neto,
  SUM(CANTIDAD)                       AS unidades,
  COUNT(DISTINCT COD_VENTA)           AS pedidos,
  COUNT(DISTINCT CLIENTE)             AS clientes
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1
ORDER BY 2 DESC;


-- ═══════════════════════════════════════════════════════════════════════
-- 4. VW_LOOKER_DIMENSION_TIPO
--    Alimenta: Donut chart "Venta por Tipo de Bebida"
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_DIMENSION_TIPO` AS
SELECT
  NOMBRE_TIPO_BEBIDA,
  ROUND(SUM(VENTA_NETA_MN), 0)       AS venta_neta,
  SUM(CANTIDAD)                       AS unidades,
  ROUND(SUM(VENTA_NETA_MN) * 100.0 /
    SUM(SUM(VENTA_NETA_MN)) OVER (), 1) AS pct_venta
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1
ORDER BY 2 DESC;


-- ═══════════════════════════════════════════════════════════════════════
-- 5. VW_LOOKER_DIMENSION_MARCA
--    Alimenta: Barras horizontales "Venta Neta por Marca" (Top 8)
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_DIMENSION_MARCA` AS
SELECT
  MARCA_PRODUCTO,
  ROUND(SUM(VENTA_NETA_MN), 0)       AS venta_neta,
  ROUND(SUM(MARGEN_MN), 0)           AS margen_neto,
  SUM(CANTIDAD)                       AS unidades
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 8;


-- ═══════════════════════════════════════════════════════════════════════
-- 6. VW_LOOKER_TOP_CLIENTES
--    Alimenta: Tabla "Top Clientes por Revenue" con segmento RFM
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_TOP_CLIENTES` AS
WITH cliente_stats AS (
  SELECT
    CLIENTE,
    COUNT(DISTINCT COD_VENTA)                          AS pedidos,
    ROUND(SUM(VENTA_NETA_MN), 0)                      AS venta_neta,
    MAX(DATE(ANNO, MES, 1))                            AS ultima_compra,
    COUNT(DISTINCT FORMAT('%d-%02d', ANNO, MES))       AS meses_activos
  FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
  GROUP BY 1
),
ranked AS (
  SELECT
    *,
    ROUND(venta_neta * 100.0 / SUM(venta_neta) OVER(), 1) AS share_pct,
    DATE_DIFF(CURRENT_DATE(), ultima_compra, DAY)          AS dias_sin_compra,
    ROW_NUMBER() OVER (ORDER BY venta_neta DESC)           AS ranking
  FROM cliente_stats
),
rfm AS (
  SELECT
    *,
    CASE
      WHEN dias_sin_compra <= 60 AND venta_neta > 150000  THEN '🏆 VIP'
      WHEN dias_sin_compra <= 90 AND pedidos >= 80         THEN '⭐ Leal'
      WHEN dias_sin_compra <= 120                          THEN '🟢 Activo'
      WHEN dias_sin_compra <= 180                          THEN '🟡 En riesgo'
      ELSE '🔴 Inactivo'
    END AS segmento_rfm
  FROM ranked
)
SELECT
  ranking,
  CLIENTE              AS cliente,
  pedidos,
  venta_neta,
  share_pct,
  segmento_rfm,
  dias_sin_compra,
  ultima_compra
FROM rfm
ORDER BY ranking;


-- ═══════════════════════════════════════════════════════════════════════
-- 7. VW_LOOKER_TOP_PRODUCTOS
--    Alimenta: Tabla "Productos Más Vendidos"
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_TOP_PRODUCTOS` AS
WITH prod_stats AS (
  SELECT
    PRODUCTO,
    NOMBRE_TIPO_BEBIDA,
    SUM(CANTIDAD)                                      AS unidades,
    ROUND(SUM(VENTA_NETA_MN), 0)                      AS venta_neta,
    ROUND(SUM(MARGEN_MN), 0)                           AS margen_neto,
    ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN)) * 100, 1)
                                                       AS margen_pct
  FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    *,
    ROUND(unidades * 100.0 / SUM(unidades) OVER(), 1) AS pct_total_uds,
    ROW_NUMBER() OVER (ORDER BY unidades DESC)         AS ranking
  FROM prod_stats
)
SELECT * FROM ranked
ORDER BY ranking;


-- ═══════════════════════════════════════════════════════════════════════
-- 8. VW_LOOKER_VENDEDORES
--    Alimenta: Tabla "Vendedores"
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_VENDEDORES` AS
SELECT
  VENDEDOR,
  ROUND(SUM(VENTA_NETA_MN), 0)                          AS venta_neta,
  COUNT(DISTINCT COD_VENTA)                              AS pedidos,
  ROUND(SUM(VENTA_NETA_MN) * 100.0 /
    SUM(SUM(VENTA_NETA_MN)) OVER(), 1)                  AS share_pct
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1
ORDER BY 2 DESC;


-- ═══════════════════════════════════════════════════════════════════════
-- 9. VW_LOOKER_SEGMENTO_PRECIO
--    Alimenta: Barras "Segmentos por Precio/Botella"
--    Nota: Se usa COSTO_MN_TOTAL como proxy del costo FOB por unidad.
--    Ajusta los rangos de precio si tu lógica es diferente.
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_SEGMENTO_PRECIO` AS
WITH con_precio AS (
  SELECT
    *,
    SAFE_DIVIDE(COSTO_MN_TOTAL, CANTIDAD) AS precio_unitario_costo
  FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
  WHERE CANTIDAD > 0
),
segmentado AS (
  SELECT
    CASE
      WHEN precio_unitario_costo < 75    THEN '1. Económico (<US$20)'
      WHEN precio_unitario_costo < 185   THEN '2. Medio (US$20–50)'
      WHEN precio_unitario_costo < 480   THEN '3. Premium (US$50–130)'
      ELSE '4. Ultra Premium (>US$130)'
    END AS segmento,
    CANTIDAD,
    VENTA_NETA_MN,
    MARGEN_MN
  FROM con_precio
)
SELECT
  segmento,
  SUM(CANTIDAD)                                          AS unidades,
  ROUND(SUM(CANTIDAD) * 100.0 /
    SUM(SUM(CANTIDAD)) OVER(), 1)                        AS pct_volumen,
  ROUND(SUM(VENTA_NETA_MN), 0)                          AS venta_neta,
  ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN)) * 100, 1)
                                                         AS margen_pct
FROM segmentado
GROUP BY 1
ORDER BY 1;


-- ═══════════════════════════════════════════════════════════════════════
-- 10. VW_LOOKER_MARGEN_MARCA
--     Alimenta: Barras horizontales "Margen % por Marca"
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_MARGEN_MARCA` AS
SELECT
  MARCA_PRODUCTO,
  ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN)) * 100, 1)
                                              AS margen_pct,
  ROUND(SUM(VENTA_NETA_MN), 0)               AS venta_neta,
  SUM(CANTIDAD)                               AS unidades
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1
HAVING SUM(VENTA_NETA_MN) > 50000
ORDER BY 2 DESC
LIMIT 8;


-- ═══════════════════════════════════════════════════════════════════════
-- 11. VW_LOOKER_RFM
--     Alimenta: Lista "Segmentación RFM de Clientes"
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_RFM` AS
WITH cliente_rfm AS (
  SELECT
    CLIENTE,
    MAX(DATE(ANNO, MES, 1))                            AS ultima_compra,
    COUNT(DISTINCT COD_VENTA)                          AS frecuencia,
    ROUND(SUM(VENTA_NETA_MN), 0)                      AS valor_monetario,
    DATE_DIFF(CURRENT_DATE(), MAX(DATE(ANNO, MES, 1)), DAY) AS dias_sin_compra
  FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
  GROUP BY 1
),
segmentado AS (
  SELECT
    *,
    CASE
      WHEN dias_sin_compra <= 60 AND valor_monetario > 150000  THEN '🏆 VIP'
      WHEN dias_sin_compra <= 90 AND frecuencia >= 80           THEN '⭐ Leal'
      WHEN dias_sin_compra <= 120                               THEN '🟢 Activo'
      WHEN dias_sin_compra <= 180                               THEN '🟡 En riesgo'
      ELSE '🔴 Inactivo'
    END AS segmento_rfm
  FROM cliente_rfm
)
SELECT * FROM segmentado
ORDER BY valor_monetario DESC;


-- ═══════════════════════════════════════════════════════════════════════
-- 12. VW_LOOKER_ESTACIONALIDAD
--     Alimenta: Heatmap "Índice de Estacionalidad"
--     Calcula ratio mensual vs promedio anual
-- ═══════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_LOOKER_ESTACIONALIDAD` AS
WITH mensual AS (
  SELECT
    ANNO,
    MES,
    ROUND(SUM(VENTA_NETA_MN), 0) AS venta_mes
  FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
  GROUP BY 1, 2
),
promedio_anual AS (
  SELECT
    ANNO,
    AVG(venta_mes) AS avg_anual
  FROM mensual
  GROUP BY 1
)
SELECT
  m.ANNO,
  m.MES,
  FORMAT_DATE('%b', DATE(m.ANNO, m.MES, 1))              AS mes_label,
  m.venta_mes,
  ROUND(m.venta_mes / p.avg_anual * 100, 0)              AS indice_estacional,
  CASE
    WHEN m.venta_mes / p.avg_anual > 1.15 THEN 'Muy alto'
    WHEN m.venta_mes / p.avg_anual > 1.00 THEN 'Alto'
    WHEN m.venta_mes / p.avg_anual > 0.85 THEN 'Normal'
    ELSE 'Bajo'
  END                                                     AS nivel
FROM mensual m
  JOIN promedio_anual p ON m.ANNO = p.ANNO
ORDER BY m.ANNO, m.MES;


-- ═══════════════════════════════════════════════════════════════════════
-- 13. TB_FORECAST_TOTAL (Tabla, no vista)
--     Se crea vacía — el script actualizar_forecast_bigquery.py la llena
--     Alimenta: Gráfico "Serie Histórica + Pronóstico"
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `dashboard-iconic-terroirs.Mis_Tablas.TB_FORECAST_TOTAL` (
  periodo     DATE,
  anno        INT64,
  mes         INT64,
  venta_real  FLOAT64,
  forecast    FLOAT64,
  fc_lo       FLOAT64,
  fc_hi       FLOAT64,
  tipo        STRING
);


-- ═══════════════════════════════════════════════════════════════════════
-- 14. TB_FORECAST_PRODUCTOS (Tabla, no vista)
--     Se crea vacía — el script actualizar_forecast_bigquery.py la llena
--     Alimenta: Gráfico "Pronóstico por Producto"
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS `dashboard-iconic-terroirs.Mis_Tablas.TB_FORECAST_PRODUCTOS` (
  producto    STRING,
  periodo     DATE,
  anno        INT64,
  mes         INT64,
  uds_real    FLOAT64,
  fc_uds      FLOAT64,
  fc_lo       FLOAT64,
  fc_hi       FLOAT64,
  mes_label   STRING,
  tipo        STRING
);


-- ═══════════════════════════════════════════════════════════════════════
-- ✅ FIN DEL SCRIPT
--    12 vistas + 2 tablas creadas en dashboard-iconic-terroirs.Mis_Tablas
--
--    Siguiente paso:
--    1. Ejecutar actualizar_forecast_bigquery.py para llenar las tablas
--       de forecast
--    2. Abrir Looker Studio y conectar cada vista como data source
--    3. Seguir la guía en guia_looker_studio.md
-- ═══════════════════════════════════════════════════════════════════════
