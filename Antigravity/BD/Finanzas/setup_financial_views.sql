-- 1. Vista: Balance General (Reporte de Situacion Patrimonial)
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_FIN_BALANCE` AS
SELECT 
  Nro_Cta as cuenta,
  Nombre_Cuenta as nombre,
  Ejercicio as ejercicio,
  Periodo as periodo,
  SUM(Mto_Debe) as debe,
  SUM(Mto_Haber) as haber
FROM `dashboard-iconic-terroirs.Mis_Tablas.TB_movimientos_contabilidad`
WHERE (Nro_Cta LIKE '1%' OR Nro_Cta LIKE '2%' OR Nro_Cta LIKE '3%' OR Nro_Cta LIKE '4%' OR Nro_Cta LIKE '5%')
GROUP BY 1, 2, 3, 4;

-- 2. Vista: Estado de Resultados por Naturaleza
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_FIN_RESULTADOS` AS
SELECT 
  Nro_Cta as cuenta,
  Nombre_Cuenta as nombre,
  Ejercicio as ejercicio,
  Periodo as periodo,
  SUM(Mto_Debe) as debe,
  SUM(Mto_Haber) as haber
FROM `dashboard-iconic-terroirs.Mis_Tablas.TB_movimientos_contabilidad`
WHERE (Nro_Cta LIKE '6%' OR Nro_Cta LIKE '7%')
  AND Periodo != 0
GROUP BY 1, 2, 3, 4;

-- 3. Vista: Flujo de Efectivo (Movimientos de Efectivo)
CREATE OR REPLACE VIEW `dashboard-iconic-terroirs.Mis_Tablas.VW_FIN_FLUJO` AS
SELECT 
  Ejercicio as ejercicio,
  Periodo as periodo,
  SUM(Mto_Debe) as ingresos,
  SUM(Mto_Haber) as egresos,
  SUM(Mto_Debe - Mto_Haber) as flujo_neto
FROM `dashboard-iconic-terroirs.Mis_Tablas.TB_movimientos_contabilidad`
WHERE Nro_Cta LIKE '10%'
GROUP BY 1, 2;
