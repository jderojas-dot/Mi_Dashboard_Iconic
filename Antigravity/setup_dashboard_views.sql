-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║   ICONIC TERROIRS — Vistas y Tablas para el Dashboard                 ║
-- ║   Archivo: setup_dashboard_views.sql                                  ║
-- ╚══════════════════════════════════════════════════════════════════════╝

-- Nota: La base del proyecto FastAPI requiere las tablas de pronóstico.
-- Las vistas adicionales aquí incluidas son útiles si vas a conectar 
-- la base a otros sistemas (como Looker Studio) o para futuras versiones.

-- ═══════════════════════════════════════════════════════════════════════
-- 1. TABLAS DE FORECAST (Requeridas por FastAPI y Python)
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
-- 2. VISTAS ADICIONALES (Para análisis profundo y compatibilidad Looker)
-- ═══════════════════════════════════════════════════════════════════════

-- Vista: Ventas del Año en Curso vs Año Anterior
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_DASHBOARD_VENTAS_ANNO_CURSO` AS
SELECT 
  *,
  CASE WHEN CAST(ANNO AS INT64) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN 'Current'
       WHEN CAST(ANNO AS INT64) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN 'Previous'
       ELSE 'Older' END as temporalidad
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
WHERE CAST(ANNO AS INT64) >= EXTRACT(YEAR FROM CURRENT_DATE()) - 1;

-- Vista: Detalle Avanzado de Clientes (Extiende la vista de RFM)
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_DASHBOARD_CLIENTES_DETALLE` AS
SELECT
  CLIENTE,
  MIN(DATE(ANNO, MES, 1)) AS primera_compra,
  MAX(DATE(ANNO, MES, 1)) AS ultima_compra,
  COUNT(DISTINCT COD_VENTA) AS frecuencia,
  COUNT(DISTINCT FORMAT('%d-%02d', ANNO, MES)) AS meses_activos,
  ROUND(SUM(VENTA_NETA_MN), 0) AS valor_monetario,
  ROUND(SUM(MARGEN_MN), 0) AS margen_total
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1;

-- Vista: Detalle Avanzado de Productos
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_DASHBOARD_PRODUCTOS_DETALLE` AS
SELECT
  PRODUCTO,
  NOMBRE_TIPO_BEBIDA,
  MARCA_PRODUCTO,
  SUM(CANTIDAD) AS unidades,
  ROUND(SUM(VENTA_NETA_MN), 0) AS venta_neta,
  ROUND(SUM(MARGEN_MN), 0) AS margen,
  ROUND(SAFE_DIVIDE(SUM(MARGEN_MN), SUM(VENTA_NETA_MN)) * 100, 1) AS margen_pct
FROM `dashboard-iconic-terroirs.Mis_Tablas.VW_VENTAS_DASHBOARD`
GROUP BY 1, 2, 3;

-- ═══════════════════════════════════════════════════════════════════════
-- ✅ FIN DEL SCRIPT
-- ═══════════════════════════════════════════════════════════════════════
