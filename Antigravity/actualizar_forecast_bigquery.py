"""
╔══════════════════════════════════════════════════════════════════════╗
║   ICONIC TERROIRS — Script de Actualización de Pronósticos          ║
║   BigQuery: TB_FORECAST_TOTAL + TB_FORECAST_PRODUCTOS               ║
╠══════════════════════════════════════════════════════════════════════╣
║   Método: Fourier (ciclos dominantes) + AR(2) + Tendencia Lineal    ║
║   Horizonte: 3 meses hacia adelante desde el último mes con datos   ║
║   Banda de confianza: ±1.5 × RMSE (últimos 12 meses)               ║
╠══════════════════════════════════════════════════════════════════════╣
║   INSTALACIÓN DE DEPENDENCIAS:                                      ║
║     pip install google-cloud-bigquery pandas numpy scipy db-dtypes  ║
║                                                                     ║
║   AUTENTICACIÓN:                                                    ║
║     Opción A (recomendada): Service Account JSON                    ║
║       → Editar CREDENCIALES más abajo con la ruta a tu archivo .json║
║     Opción B: gcloud CLI                                            ║
║       → Ejecutar: gcloud auth application-default login             ║
║       → Dejar RUTA_CREDENCIALES = None                              ║
║                                                                     ║
║   USO:                                                              ║
║     python actualizar_forecast_bigquery.py                          ║
║     python actualizar_forecast_bigquery.py --dry-run   (sin subir)  ║
║     python actualizar_forecast_bigquery.py --meses 6   (6 meses)   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import argparse
import sys
import numpy as np
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from scipy.fft import fft, ifft

# ─────────────────────────────────────────────
# ① CONFIGURACIÓN — EDITAR AQUÍ
# ─────────────────────────────────────────────
PROYECTO        = "dashboard-iconic-terroirs"
DATASET         = "Mis_Tablas"
VISTA_VENTAS    = "VW_VENTAS_DASHBOARD"
TABLA_FORECAST  = "TB_FORECAST_TOTAL"
TABLA_PROD_FC   = "TB_FORECAST_PRODUCTOS"
TOP_N_PRODUCTOS = 5          # Número de productos top para pronosticar
MESES_FORECAST  = 3          # Horizonte de pronóstico en meses
HARMONICOS      = 4          # Número de armónicos Fourier a usar
MESES_RMSE      = 12         # Ventana para calcular RMSE (banda de confianza)
CI_FACTOR       = 1.5        # Factor de intervalo de confianza (×RMSE)
# Ruta al archivo JSON de la cuenta de servicio.
# Si usas gcloud auth, dejar en None
RUTA_CREDENCIALES = r"C:\Users\JAVIER\OneDrive\Documentos\Dashboard_Iconic\BD\credentials.json"
# RUTA_CREDENCIALES = None


# ─────────────────────────────────────────────
# ② DEPENDENCIAS
# ─────────────────────────────────────────────
def verificar_dependencias():
    faltantes = []
    for pkg in ['google.cloud.bigquery', 'pandas', 'numpy', 'scipy', 'dateutil']:
        try:
            __import__(pkg.replace('.', '/').replace('/', '.'))
        except ImportError:
            faltantes.append(pkg.split('.')[0])
    if faltantes:
        print(f"❌ Dependencias faltantes: {', '.join(faltantes)}")
        print("   Instalar con: pip install google-cloud-bigquery pandas numpy scipy python-dateutil db-dtypes")
        sys.exit(1)


# ─────────────────────────────────────────────
# ③ CLIENTE BIGQUERY
# ─────────────────────────────────────────────
def get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    if RUTA_CREDENCIALES:
        creds = service_account.Credentials.from_service_account_file(
            RUTA_CREDENCIALES,
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive.readonly",
            ],
        )
        client = bigquery.Client(project=PROYECTO, credentials=creds)
    else:
        client = bigquery.Client(project=PROYECTO)
    return client


# ─────────────────────────────────────────────
# ④ LECTURA DE DATOS HISTÓRICOS DESDE BIGQUERY
# ─────────────────────────────────────────────
def leer_serie_total(client):
    """Lee la serie mensual total de ventas desde VW_VENTAS_DASHBOARD."""
    sql = f"""
    SELECT
      DATE(ANNO, MES, 1)                              AS periodo,
      ANNO,
      MES,
      ROUND(SUM(VENTA_NETA_MN), 2)                   AS venta_neta,
      ROUND(SUM(MARGEN_MN), 2)                        AS margen_neto,
      SUM(CANTIDAD)                                   AS unidades,
      COUNT(DISTINCT COD_VENTA)                       AS pedidos
    FROM `{PROYECTO}.{DATASET}.{VISTA_VENTAS}`
    GROUP BY 1, 2, 3
    ORDER BY 1
    """
    print("   📥 Leyendo serie mensual total desde BigQuery...")
    df = client.query(sql).to_dataframe()
    df['periodo'] = pd.to_datetime(df['periodo'])
    print(f"   ✅ {len(df)} meses cargados ({df['periodo'].min().strftime('%b %Y')} → {df['periodo'].max().strftime('%b %Y')})")
    return df


def leer_serie_productos(client, top_n=5):
    """Lee las series mensuales de los top N productos."""
    sql = f"""
    WITH totales AS (
      SELECT
        PRODUCTO,
        SUM(CANTIDAD) AS total_uds
      FROM `{PROYECTO}.{DATASET}.{VISTA_VENTAS}`
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT {top_n}
    )
    SELECT
      v.PRODUCTO,
      DATE(v.ANNO, v.MES, 1)                         AS periodo,
      v.ANNO,
      v.MES,
      SUM(v.CANTIDAD)                                 AS unidades
    FROM `{PROYECTO}.{DATASET}.{VISTA_VENTAS}` v
    INNER JOIN totales t ON v.PRODUCTO = t.PRODUCTO
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2
    """
    print(f"   📥 Leyendo series de top {top_n} productos...")
    df = client.query(sql).to_dataframe()
    df['periodo'] = pd.to_datetime(df['periodo'])
    productos = df['PRODUCTO'].unique()
    print(f"   ✅ {len(productos)} productos cargados:")
    for p in productos:
        n = df[df['PRODUCTO']==p]['unidades'].sum()
        print(f"      • {p[:50]}: {n:.0f} uds históricas")
    return df


# ─────────────────────────────────────────────
# ⑤ MODELO DE PRONÓSTICO
#    Tendencia Lineal + Fourier (armónicos) + AR(2)
# ─────────────────────────────────────────────
def pronosticar(serie, meses_fc=3, harmonicos=4, meses_rmse=12, ci_factor=1.5):
    """
    Aplica el modelo combinado sobre una serie temporal mensual.

    Parámetros
    ----------
    serie       : array-like con los valores mensuales históricos
    meses_fc    : número de meses a pronosticar
    harmonicos  : número de armónicos Fourier dominantes a conservar
    meses_rmse  : ventana para calcular RMSE (band de confianza)
    ci_factor   : multiplicador del RMSE para la banda

    Retorna
    -------
    dict con claves:
        fitted      : valores ajustados del modelo (len = n)
        forecast    : pronóstico (len = meses_fc)
        fc_lo       : límite inferior (len = meses_fc)
        fc_hi       : límite superior (len = meses_fc)
        rmse        : RMSE sobre últimos meses_rmse meses
        slope       : pendiente de la tendencia (S/ o uds por mes)
        intercept   : intercepto de la tendencia
    """
    y = np.array(serie, dtype=float)
    n = len(y)
    x = np.arange(n)

    # — Componente 1: Tendencia lineal (mínimos cuadrados) —
    A = np.vstack([x, np.ones(n)]).T
    slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
    trend = slope * x + intercept
    detrended = y - trend

    # — Componente 2: Fourier — seleccionar armónicos dominantes —
    Y = fft(detrended)
    freqs = np.fft.fftfreq(n)
    power = np.abs(Y) ** 2

    pos_idx = np.where(freqs > 0)[0]
    dominant = pos_idx[np.argsort(power[pos_idx])[::-1][:harmonicos]]

    Y_filtered = np.zeros(n, dtype=complex)
    for i in dominant:
        Y_filtered[i] = Y[i]
        Y_filtered[n - i] = Y[n - i]
    seasonal = np.real(ifft(Y_filtered))

    fitted = trend + seasonal
    residuals = y - fitted

    # — Componente 3: AR(2) sobre residuos (Yule-Walker) —
    r0 = np.mean(residuals ** 2)
    r1 = np.mean(residuals[:-1] * residuals[1:])
    r2 = np.mean(residuals[:-2] * residuals[2:])

    try:
        R_mat = np.array([[r0, r1], [r1, r0]])
        r_vec = np.array([r1, r2])
        phi = np.linalg.solve(R_mat, r_vec)
    except np.linalg.LinAlgError:
        phi = np.zeros(2)

    # — Proyección —
    x_fc = np.arange(n, n + meses_fc)
    trend_fc = slope * x_fc + intercept

    # Fourier: proyectar cada armónico dominante
    seasonal_fc = np.zeros(meses_fc)
    for i in dominant:
        freq  = freqs[i]
        amp   = np.abs(Y[i]) * 2 / n
        phase = np.angle(Y[i])
        for j, xf in enumerate(x_fc):
            seasonal_fc[j] += amp * np.cos(2 * np.pi * freq * xf + phase)

    # AR(2): corrección de residuos
    res_fc = np.zeros(meses_fc)
    res_fc[0] = phi[0] * residuals[-1] + phi[1] * residuals[-2]
    if meses_fc > 1:
        res_fc[1] = phi[0] * res_fc[0] + phi[1] * residuals[-1]
    # Para horizontes > 2 el AR(2) decae a 0

    forecast_raw = trend_fc + seasonal_fc + res_fc

    # No permitir valores negativos (ventas no pueden ser < 0)
    forecast = np.maximum(0, forecast_raw)

    # — Banda de confianza —
    n_rmse = min(meses_rmse, n)
    rmse = np.sqrt(np.mean((y[-n_rmse:] - fitted[-n_rmse:]) ** 2))
    ci = ci_factor * rmse

    fc_lo = np.maximum(0, forecast - ci)
    fc_hi = forecast + ci

    return {
        'fitted':    fitted,
        'forecast':  forecast,
        'fc_lo':     fc_lo,
        'fc_hi':     fc_hi,
        'rmse':      rmse,
        'slope':     slope,
        'intercept': intercept,
    }


# ─────────────────────────────────────────────
# ⑥ CONSTRUCCIÓN DE DATAFRAMES DE RESULTADOS
# ─────────────────────────────────────────────
def construir_df_forecast_total(df_hist, resultado, meses_fc):
    """
    Une el histórico real con las filas de pronóstico
    en el formato de TB_FORECAST_TOTAL.
    """
    # — Filas históricas —
    rows_hist = []
    for _, r in df_hist.iterrows():
        rows_hist.append({
            'periodo':    r['periodo'].date(),
            'anno':       int(r['ANNO']),
            'mes':        int(r['MES']),
            'venta_real': float(r['venta_neta']),
            'forecast':   None,
            'fc_lo':      None,
            'fc_hi':      None,
            'tipo':       'histórico',
        })

    # — Filas de pronóstico —
    ultimo = df_hist['periodo'].max()
    rows_fc = []
    for i in range(meses_fc):
        periodo_fc = (ultimo + relativedelta(months=i+1)).replace(day=1)
        rows_fc.append({
            'periodo':    periodo_fc.date(),
            'anno':       periodo_fc.year,
            'mes':        periodo_fc.month,
            'venta_real': None,
            'forecast':   round(float(resultado['forecast'][i]), 2),
            'fc_lo':      round(float(resultado['fc_lo'][i]), 2),
            'fc_hi':      round(float(resultado['fc_hi'][i]), 2),
            'tipo':       'pronóstico',
        })

    df_out = pd.DataFrame(rows_hist + rows_fc)
    df_out['periodo'] = pd.to_datetime(df_out['periodo'])
    return df_out


def construir_df_forecast_productos(df_hist_prods, resultados_prods, meses_fc):
    """
    Construye las filas de pronóstico por producto
    en el formato de TB_FORECAST_PRODUCTOS.
    """
    rows = []
    nombres_meses = ['Ene','Feb','Mar','Abr','May','Jun',
                     'Jul','Ago','Sep','Oct','Nov','Dic']

    for producto, res in resultados_prods.items():
        df_p = df_hist_prods[df_hist_prods['PRODUCTO'] == producto]
        ultimo = df_p['periodo'].max()

        # Histórico del producto
        for _, r in df_p.iterrows():
            rows.append({
                'producto':   producto,
                'periodo':    r['periodo'].date(),
                'anno':       int(r['ANNO']),
                'mes':        int(r['MES']),
                'uds_real':   float(r['unidades']),
                'fc_uds':     None,
                'fc_lo':      None,
                'fc_hi':      None,
                'mes_label':  f"{nombres_meses[int(r['MES'])-1]} {int(r['ANNO'])}",
                'tipo':       'histórico',
            })

        # Pronóstico del producto
        for i in range(meses_fc):
            periodo_fc = (pd.Timestamp(ultimo) + relativedelta(months=i+1)).replace(day=1)
            rows.append({
                'producto':  producto,
                'periodo':   periodo_fc.date(),
                'anno':      periodo_fc.year,
                'mes':       periodo_fc.month,
                'uds_real':  None,
                'fc_uds':    round(float(res['forecast'][i]), 1),
                'fc_lo':     round(float(res['fc_lo'][i]), 1),
                'fc_hi':     round(float(res['fc_hi'][i]), 1),
                'mes_label': f"{nombres_meses[periodo_fc.month-1]} {periodo_fc.year}",
                'tipo':      'pronóstico',
            })

    df_out = pd.DataFrame(rows)
    df_out['periodo'] = pd.to_datetime(df_out['periodo'])
    return df_out


# ─────────────────────────────────────────────
# ⑦ SUBIDA A BIGQUERY
# ─────────────────────────────────────────────
def subir_a_bigquery(client, df, tabla, descripcion):
    """Reemplaza la tabla en BigQuery con los nuevos datos."""
    from google.cloud import bigquery

    tabla_full = f"{PROYECTO}.{DATASET}.{tabla}"
    print(f"   📤 Subiendo {len(df)} filas a {tabla_full}...")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Truncate = reemplaza completamente la tabla cada vez
    )

    job = client.load_table_from_dataframe(df, tabla_full, job_config=job_config)
    job.result()  # Esperar a que termine

    tabla_bq = client.get_table(tabla_full)
    print(f"   ✅ {tabla}: {tabla_bq.num_rows} filas en BigQuery")


# ─────────────────────────────────────────────
# ⑧ REPORTE EN CONSOLA
# ─────────────────────────────────────────────
def imprimir_reporte(df_total, resultados_total, df_prods, resultados_prods):
    """Imprime un resumen del pronóstico en consola."""
    sep = "─" * 65

    print(f"\n{'═'*65}")
    print(f"  REPORTE DE PRONÓSTICO — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'═'*65}")

    # — Pronóstico total —
    print(f"\n📊 PRONÓSTICO TOTAL (Ventas Netas en S/)")
    print(sep)
    print(f"  {'Período':<14} {'Pronóstico':>14} {'Límite Inf':>14} {'Límite Sup':>14}")
    print(sep)
    fc_rows = df_total[df_total['tipo'] == 'pronóstico']
    for _, r in fc_rows.iterrows():
        per = r['periodo'].strftime('%b %Y')
        print(f"  {per:<14} S/ {r['forecast']:>11,.2f}   S/ {r['fc_lo']:>11,.2f}   S/ {r['fc_hi']:>11,.2f}")

    print(f"\n  📈 Tendencia: +S/ {resultados_total['slope']:,.2f} por mes")
    print(f"  📏 RMSE modelo: S/ {resultados_total['rmse']:,.2f}")
    print(f"  ⬅ Último mes histórico: {df_total[df_total['tipo']=='histórico']['periodo'].max().strftime('%b %Y')}")

    # — Pronóstico por producto —
    print(f"\n📦 PRONÓSTICO POR PRODUCTO (Unidades)")
    print(sep)
    for prod, res in resultados_prods.items():
        print(f"\n  {prod[:55]}")
        print(f"  {'Mes':<14} {'Pronóstico':>12} {'Límite Inf':>12} {'Límite Sup':>12}")
        print(f"  {'─'*54}")
        for i, v in enumerate(res['forecast']):
            print(f"  {'Mes +' + str(i+1):<14} {v:>12.1f}   {res['fc_lo'][i]:>12.1f}   {res['fc_hi'][i]:>12.1f}")
        print(f"  Tendencia: {'↑ Creciente' if res['slope'] > 0 else '↓ Decreciente'} ({res['slope']:+.2f} uds/mes)  |  RMSE: {res['rmse']:.1f}")

    print(f"\n{'═'*65}\n")


# ─────────────────────────────────────────────
# ⑨ PROGRAMA PRINCIPAL
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Actualiza los pronósticos de ventas en BigQuery.'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Calcular y mostrar pronóstico sin subir a BigQuery'
    )
    parser.add_argument(
        '--meses', type=int, default=MESES_FORECAST,
        help=f'Meses a pronosticar (default: {MESES_FORECAST})'
    )
    parser.add_argument(
        '--top-productos', type=int, default=TOP_N_PRODUCTOS,
        help=f'Número de productos top a pronosticar (default: {TOP_N_PRODUCTOS})'
    )
    args = parser.parse_args()

    print("\n" + "═"*65)
    print("  ICONIC TERROIRS — Actualización de Pronósticos")
    print("  Método: Fourier + AR(2) + Tendencia Lineal")
    print("═"*65)

    # Verificar dependencias
    verificar_dependencias()

    # Conectar a BigQuery
    if not args.dry_run:
        print("\n🔌 Conectando a BigQuery...")
        try:
            client = get_bq_client()
            print(f"   ✅ Conectado al proyecto: {PROYECTO}")
        except Exception as e:
            print(f"   ❌ Error de conexión: {e}")
            print("   Verificar credenciales o ejecutar: gcloud auth application-default login")
            sys.exit(1)
    else:
        print("\n⚠️  Modo DRY RUN — no se subirá nada a BigQuery")
        client = None

    # ── SERIE TOTAL ──
    print("\n① Procesando serie total de ventas...")
    if args.dry_run:
        # Usar datos históricos locales para dry-run
        ventas_locales = [
            22814.62,36232.87,46570.29,42278.28,29454.02,64073.96,
            54834.69,28853.49,47025.84,67180.56,44871.97,68326.71,
            39063.55,52930.06,61431.73,79555.84,60549.35,67729.93,
            84916.34,78663.62,95090.74,76538.57,49737.88,68354.22,
            75865.26,89989.53,91242.01,80277.78,113097.06,112284.78,
            67891.87,64767.14,94804.88,90459.69,66676.47,63136.07,
            78671.49,91076.21,83613.66,95628.17,87579.65,111183.82,
            78963.26,98691.83,90459.44,103587.12,81651.89,49368.73,
        ]
        periodos = pd.date_range(start='2022-03-01', periods=len(ventas_locales), freq='MS')
        df_hist = pd.DataFrame({
            'periodo': periodos,
            'ANNO': [p.year for p in periodos],
            'MES':  [p.month for p in periodos],
            'venta_neta': ventas_locales,
            'margen_neto': [v*0.45 for v in ventas_locales],
            'unidades': [int(v/20) for v in ventas_locales],
            'pedidos': [int(v/900) for v in ventas_locales],
        })
        print(f"   ✅ Usando {len(df_hist)} meses de datos locales (modo dry-run)")
    else:
        df_hist = leer_serie_total(client)

    # Calcular pronóstico total
    resultado_total = pronosticar(
        serie      = df_hist['venta_neta'].values,
        meses_fc   = args.meses,
        harmonicos = HARMONICOS,
        meses_rmse = MESES_RMSE,
        ci_factor  = CI_FACTOR,
    )
    df_fc_total = construir_df_forecast_total(df_hist, resultado_total, args.meses)

    # ── SERIE POR PRODUCTO ──
    print(f"\n② Procesando top {args.top_productos} productos...")
    if args.dry_run:
        # Datos locales de ejemplo para dry-run
        prod_data = {
            'CANTINE DEI ROSSO DI MONTEPULCIANO': [
                5,8,12,10,7,15,13,6,11,16,10,16,9,12,14,18,14,16,
                20,18,22,18,11,16,18,21,21,19,27,26,16,15,22,21,15,
                15,18,21,19,22,20,26,18,23,21,24,19,11
            ],
            'TALIS FRIULI PINOT GRIGIO DOC': [
                10,15,18,16,12,25,22,11,19,27,18,27,16,21,25,32,24,
                27,34,31,38,31,20,27,30,36,37,32,45,45,27,26,38,36,
                27,25,31,37,33,38,35,44,32,39,36,41,33,20
            ],
        }
        periodos_p = pd.date_range(start='2022-03-01', periods=48, freq='MS')
        rows_p = []
        for prod, uds_list in prod_data.items():
            for i, u in enumerate(uds_list):
                rows_p.append({
                    'PRODUCTO': prod,
                    'periodo': periodos_p[i],
                    'ANNO': periodos_p[i].year,
                    'MES': periodos_p[i].month,
                    'unidades': u,
                })
        df_hist_prods = pd.DataFrame(rows_p)
        print(f"   ✅ Usando datos locales de {len(prod_data)} productos (modo dry-run)")
    else:
        df_hist_prods = leer_serie_productos(client, top_n=args.top_productos)

    # Calcular pronóstico por producto
    resultados_prods = {}
    for producto in df_hist_prods['PRODUCTO'].unique():
        df_p = df_hist_prods[df_hist_prods['PRODUCTO'] == producto].copy()
        # Rellenar todos los meses del rango con 0 si faltan
        rango = pd.date_range(
            start=df_hist['periodo'].min(),
            end=df_hist['periodo'].max(),
            freq='MS'
        )
        serie_completa = (
            df_p.set_index('periodo')['unidades']
            .reindex(rango, fill_value=0)
            .values
        )
        res = pronosticar(
            serie      = serie_completa,
            meses_fc   = args.meses,
            harmonicos = min(HARMONICOS, len(serie_completa)//4),
            meses_rmse = MESES_RMSE,
            ci_factor  = CI_FACTOR,
        )
        resultados_prods[producto] = res

    df_fc_prods = construir_df_forecast_productos(
        df_hist_prods, resultados_prods, args.meses
    )

    # ── REPORTE ──
    imprimir_reporte(df_fc_total, resultado_total, df_fc_prods, resultados_prods)

    # ── SUBIR A BIGQUERY ──
    if not args.dry_run:
        print("③ Subiendo tablas a BigQuery...")
        try:
            subir_a_bigquery(client, df_fc_total,  TABLA_FORECAST,  "Pronóstico Total")
            subir_a_bigquery(client, df_fc_prods,  TABLA_PROD_FC,   "Pronóstico Productos")
            print(f"\n🎉 ¡Tablas actualizadas exitosamente en {PROYECTO}.{DATASET}!")
            print(f"   Looker Studio reflejará los nuevos datos en el próximo refresco.\n")
        except Exception as e:
            print(f"\n❌ Error al subir datos: {e}")
            print("   Verificar permisos: la cuenta de servicio necesita BigQuery Data Editor.\n")
            sys.exit(1)
    else:
        print("ℹ️  Modo dry-run: datos calculados pero NO subidos a BigQuery.")
        print(f"   Para subir, ejecutar sin --dry-run\n")

        # En dry-run, exportar a CSV como referencia
        csv_total = "forecast_total_preview.csv"
        csv_prods = "forecast_productos_preview.csv"
        df_fc_total.to_csv(csv_total, index=False, encoding='utf-8-sig')
        df_fc_prods[df_fc_prods['tipo']=='pronóstico'].to_csv(csv_prods, index=False, encoding='utf-8-sig')
        print(f"   📄 CSV exportados como referencia:")
        print(f"      • {csv_total}")
        print(f"      • {csv_prods}\n")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()