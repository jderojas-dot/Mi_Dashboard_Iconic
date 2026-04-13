"""
╔══════════════════════════════════════════════════════════════════════╗
║   ICONIC TERROIRS — Script de Actualización de Pronósticos          ║
║   BigQuery: TB_FORECAST_TOTAL + TB_FORECAST_PRODUCTOS               ║
╠══════════════════════════════════════════════════════════════════════╣
║   Método: Amazon Chronos 2 (chronos-bolt-small)                     ║
║   Horizonte: 3 meses hacia adelante desde el último mes con datos   ║
║   Banda de confianza: Percentil 10 (fc_lo) y 90 (fc_hi)            ║
╠══════════════════════════════════════════════════════════════════════╣
║   INSTALACIÓN DE DEPENDENCIAS:                                      ║
║     pip install "chronos-forecasting" torch pandas google-cloud-bigquery db-dtypes python-dateutil
║                                                                     ║
║   AUTENTICACIÓN:                                                    ║
║     Opción A (recomendada): Service Account JSON                    ║
║       → Editar RUTA_CREDENCIALES más abajo                          ║
║     Opción B: gcloud CLI                                            ║
║       → Dejar RUTA_CREDENCIALES = None                              ║
║                                                                     ║
║   USO:                                                              ║
║     python actualizar_forecast_bigquery.py                          ║
║     python actualizar_forecast_bigquery.py --dry-run   (sin subir)  ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import argparse
import sys
import re
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ─────────────────────────────────────────────
# ① CONFIGURACIÓN — EDITAR AQUÍ
# ─────────────────────────────────────────────
PROYECTO        = "dashboard-iconic-terroirs"
DATASET         = "Mis_Tablas"
VISTA_VENTAS    = "VW_VENTAS_DASHBOARD"
TABLA_FORECAST  = "TB_FORECAST_TOTAL"
TABLA_PROD_FC   = "TB_FORECAST_PRODUCTOS"
MESES_FORECAST  = 3          # Horizonte de pronóstico en meses
NUM_SAMPLES     = 50         # Muestras Chronos para estimar quantiles (CPU)
MIN_MESES_SERIE = 6          # Mínimo de meses para pronosticar un producto
# Ruta al archivo JSON de la cuenta de servicio.
# Si usas gcloud auth, dejar en None
RUTA_CREDENCIALES = r"C:\Users\JAVIER\OneDrive\Documentos\Dashboard_Iconic\BD\credentials.json"
# RUTA_CREDENCIALES = None


# ─────────────────────────────────────────────
# ② DEPENDENCIAS
# ─────────────────────────────────────────────
def verificar_dependencias():
    paquetes = {
        'google.cloud.bigquery': 'google-cloud-bigquery',
        'pandas': 'pandas',
        'numpy': 'numpy',
        'dateutil': 'python-dateutil',
        'torch': 'torch',
        'chronos': 'chronos-forecasting',
    }
    faltantes = []
    for mod, pkg in paquetes.items():
        try:
            __import__(mod)
        except ImportError:
            faltantes.append(pkg)
    if faltantes:
        print(f"❌ Dependencias faltantes: {', '.join(faltantes)}")
        print("   Instalar con: pip install " + " ".join(faltantes))
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
        return bigquery.Client(project=PROYECTO, credentials=creds)
    else:
        return bigquery.Client(project=PROYECTO)


# ─────────────────────────────────────────────
# ④ LECTURA DE DATOS HISTÓRICOS DESDE BIGQUERY
# ─────────────────────────────────────────────
def leer_serie_total(client):
    """Lee la serie mensual total de ventas desde VW_VENTAS_DASHBOARD."""
    sql = f"""
    SELECT
      DATE(CAST(ANNO AS INT64), CAST(MES AS INT64), 1)   AS periodo,
      CAST(ANNO AS INT64)                                 AS anno,
      CAST(MES AS INT64)                                  AS mes,
      ROUND(SUM(VENTA_NETA_MN), 2)                        AS venta_neta,
      ROUND(SUM(MARGEN_MN), 2)                             AS margen_neto,
      SUM(CANTIDAD)                                        AS unidades,
      COUNT(DISTINCT COD_VENTA)                            AS pedidos
    FROM `{PROYECTO}.{DATASET}.{VISTA_VENTAS}`
    GROUP BY 1, 2, 3
    ORDER BY 1
    """
    print("   📥 Leyendo serie mensual total desde BigQuery...")
    df = client.query(sql).to_dataframe()
    df['periodo'] = pd.to_datetime(df['periodo'])
    print(f"   ✅ {len(df)} meses cargados ({df['periodo'].min().strftime('%b %Y')} → {df['periodo'].max().strftime('%b %Y')})")
    return df


def strip_vintage(name: str) -> str:
    """Elimina la añada (año) del nombre del producto para agrupar."""
    # Elimina años 1960-2040 que aparezcan como palabra independiente
    cleaned = re.sub(r'\s*(1[9]\d{2}|20[0-4]\d)\b', '', str(name))
    # Elimina espacios múltiples
    return re.sub(r'\s{2,}', ' ', cleaned).strip()


def leer_serie_productos(client):
    """
    Lee las series mensuales de TODOS los productos, agrupando por
    nombre sin añada y obteniendo la marca dominante de cada uno.
    """
    sql = f"""
    SELECT
      PRODUCTO,
      IFNULL(MARCA_PRODUCTO, 'Sin Marca')                 AS marca,
      DATE(CAST(ANNO AS INT64), CAST(MES AS INT64), 1)    AS periodo,
      CAST(ANNO AS INT64)                                  AS anno,
      CAST(MES AS INT64)                                   AS mes,
      SUM(CANTIDAD)                                        AS unidades
    FROM `{PROYECTO}.{DATASET}.{VISTA_VENTAS}`
    WHERE CANTIDAD > 0
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 3
    """
    print("   📥 Leyendo series de todos los productos desde BigQuery...")
    df = client.query(sql).to_dataframe()
    df['periodo'] = pd.to_datetime(df['periodo'])

    # ── Eliminar añada del nombre del producto ──
    df['producto_base'] = df['PRODUCTO'].apply(strip_vintage)

    # ── Obtener la marca más representativa de cada producto_base ──
    marca_map = (
        df.groupby(['producto_base', 'marca'])['unidades']
        .sum()
        .reset_index()
        .sort_values('unidades', ascending=False)
        .groupby('producto_base')
        .first()
        .reset_index()[['producto_base', 'marca']]
    )
    df = df.drop(columns=['marca']).merge(marca_map, on='producto_base', how='left')

    # ── Reagrupar sumando unidades por producto_base (sin añada) ──
    df_agg = (
        df.groupby(['producto_base', 'marca', 'periodo', 'anno', 'mes'])['unidades']
        .sum()
        .reset_index()
        .rename(columns={'producto_base': 'PRODUCTO'})
        .sort_values(['PRODUCTO', 'periodo'])
    )

    # ── Filtrar productos con series demasiado cortas ──
    conteo = df_agg.groupby('PRODUCTO')['periodo'].count()
    prod_validos = conteo[conteo >= MIN_MESES_SERIE].index
    df_agg = df_agg[df_agg['PRODUCTO'].isin(prod_validos)].copy()

    productos = df_agg['PRODUCTO'].unique()
    print(f"   ✅ {len(productos)} productos únicos (sin añada, ≥{MIN_MESES_SERIE} meses de historial)")
    return df_agg


# ─────────────────────────────────────────────
# ⑤ MODELO DE PRONÓSTICO — AMAZON CHRONOS 2
# ─────────────────────────────────────────────
def init_pipeline():
    """
    Inicializa el pipeline de Amazon Chronos Bolt (Chronos 2).
    Descarga el modelo la primera vez (~300 MB desde HuggingFace Hub).
    """
    import torch

    print("\n🚀 Cargando modelo Amazon Chronos Bolt (chronos-bolt-small)...")
    print("   ⏳ La primera vez descarga ~300 MB desde HuggingFace Hub...")

    try:
        # Intentar con la clase específica de Bolt (chronos-forecasting >= 1.4)
        from chronos import ChronosBoltPipeline
        pipeline = ChronosBoltPipeline.from_pretrained(
            "amazon/chronos-bolt-small",
            device_map="cpu",
            torch_dtype=torch.float32,
        )
        print("   ✅ ChronosBoltPipeline cargado (amazon/chronos-bolt-small)")
    except (ImportError, Exception) as e:
        # Fallback a ChronosPipeline con modelo T5 Small
        print(f"   ⚠️  ChronosBolt no disponible ({e}). Usando ChronosPipeline T5-Small...")
        from chronos import ChronosPipeline
        pipeline = ChronosPipeline.from_pretrained(
            "amazon/chronos-t5-small",
            device_map="cpu",
            torch_dtype=torch.float32,
        )
        print("   ✅ ChronosPipeline cargado (amazon/chronos-t5-small)")

    return pipeline


def pronosticar_chronos(serie, pipeline, meses_fc=3, num_samples=50):
    """
    Aplica Amazon Chronos 2 sobre una serie temporal mensual.

    Parámetros
    ----------
    serie      : array-like con los valores mensuales históricos
    pipeline   : ChronosPipeline inicializado
    meses_fc   : número de meses a pronosticar
    num_samples: muestras para estimar quantiles (más = más preciso, más lento)

    Retorna
    -------
    dict con claves:
        forecast : pronóstico central P50  (len = meses_fc)
        fc_lo    : límite inferior P10     (len = meses_fc)
        fc_hi    : límite superior P90     (len = meses_fc)
    """
    import torch

    y = np.array(serie, dtype=np.float32)

    # Serie demasiado corta: usar promedio simple como fallback
    if len(y) < 3:
        val = float(np.mean(y)) if len(y) > 0 else 0.0
        return {
            'forecast': np.full(meses_fc, max(0.0, val)),
            'fc_lo':    np.zeros(meses_fc),
            'fc_hi':    np.full(meses_fc, max(0.0, val * 1.5 + 1)),
        }

    context = torch.tensor(y, dtype=torch.float32).unsqueeze(0)  # [1, T]

    with torch.no_grad():
        # samples: tensor de forma [1, num_samples, meses_fc]
        samples = pipeline.predict(
            context=context,
            prediction_length=meses_fc,
            num_samples=num_samples,
        )

    s = samples[0].float().numpy()  # [num_samples, meses_fc]

    return {
        'forecast': np.maximum(0.0, np.percentile(s, 50, axis=0)),
        'fc_lo':    np.maximum(0.0, np.percentile(s, 10, axis=0)),
        'fc_hi':    np.maximum(0.0, np.percentile(s, 90, axis=0)),
    }


# ─────────────────────────────────────────────
# ⑥ CONSTRUCCIÓN DE DATAFRAMES DE RESULTADOS
# ─────────────────────────────────────────────
NOMBRES_MESES = ['Ene','Feb','Mar','Abr','May','Jun',
                 'Jul','Ago','Sep','Oct','Nov','Dic']


def construir_df_forecast_total(df_hist, resultado, meses_fc):
    """
    Une el histórico real con las filas de pronóstico
    en el formato de TB_FORECAST_TOTAL.
    """
    rows_hist = []
    for _, r in df_hist.iterrows():
        rows_hist.append({
            'periodo':    r['periodo'].date(),
            'anno':       int(r['anno']),
            'mes':        int(r['mes']),
            'venta_real': float(r['venta_neta']),
            'forecast':   None,
            'fc_lo':      None,
            'fc_hi':      None,
            'tipo':       'histórico',
        })

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
    Construye las filas de pronóstico por producto (unidades).
    Incluye columna 'marca' para habilitar el filtro de marca en el dashboard.
    """
    rows = []

    for producto, (res, marca) in resultados_prods.items():
        df_p = df_hist_prods[df_hist_prods['PRODUCTO'] == producto]
        ultimo = df_p['periodo'].max()

        # Histórico del producto
        for _, r in df_p.iterrows():
            rows.append({
                'producto':   producto,
                'marca':      marca,
                'periodo':    r['periodo'].date(),
                'anno':       int(r['anno']),
                'mes':        int(r['mes']),
                'uds_real':   float(r['unidades']),
                'fc_uds':     None,
                'fc_lo':      None,
                'fc_hi':      None,
                'mes_label':  f"{NOMBRES_MESES[int(r['mes'])-1]} {int(r['anno'])}",
                'tipo':       'histórico',
            })

        # Pronóstico del producto
        for i in range(meses_fc):
            periodo_fc = (pd.Timestamp(ultimo) + relativedelta(months=i+1)).replace(day=1)
            rows.append({
                'producto':  producto,
                'marca':     marca,
                'periodo':   periodo_fc.date(),
                'anno':      periodo_fc.year,
                'mes':       periodo_fc.month,
                'uds_real':  None,
                'fc_uds':    round(float(res['forecast'][i]), 1),
                'fc_lo':     round(float(res['fc_lo'][i]), 1),
                'fc_hi':     round(float(res['fc_hi'][i]), 1),
                'mes_label': f"{NOMBRES_MESES[periodo_fc.month-1]} {periodo_fc.year}",
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
    )
    job = client.load_table_from_dataframe(df, tabla_full, job_config=job_config)
    job.result()

    tabla_bq = client.get_table(tabla_full)
    print(f"   ✅ {tabla}: {tabla_bq.num_rows} filas en BigQuery")


# ─────────────────────────────────────────────
# ⑧ REPORTE EN CONSOLA
# ─────────────────────────────────────────────
def imprimir_reporte(df_total, resultados_total, df_prods, resultados_prods):
    """Imprime un resumen del pronóstico en consola."""
    sep = "─" * 65

    print(f"\n{'═'*65}")
    print(f"  REPORTE DE PRONÓSTICO (Chronos 2) — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'═'*65}")

    # — Pronóstico total —
    print(f"\n📊 PRONÓSTICO TOTAL (Ventas Netas en S/)")
    print(sep)
    print(f"  {'Período':<14} {'P50 (central)':<16} {'P10 (Mín)':<14} {'P90 (Máx)':<14}")
    print(sep)
    fc_rows = df_total[df_total['tipo'] == 'pronóstico']
    for _, r in fc_rows.iterrows():
        per = r['periodo'].strftime('%b %Y')
        print(f"  {per:<14} S/ {r['forecast']:<13,.2f} S/ {r['fc_lo']:<11,.2f} S/ {r['fc_hi']:<11,.2f}")

    print(f"\n  ⬅ Último mes histórico: {df_total[df_total['tipo']=='histórico']['periodo'].max().strftime('%b %Y')}")

    # — Pronóstico por producto —
    print(f"\n📦 PRONÓSTICO POR PRODUCTO (Unidades) — {len(resultados_prods)} artículos")
    print(sep)
    for prod, (res, marca) in list(resultados_prods.items())[:10]:  # Mostrar solo los primeros 10
        print(f"\n  [{marca}] {prod[:45]}")
        for i, v in enumerate(res['forecast']):
            print(f"    Mes +{i+1}: {v:>8.1f} uds  (IC: {res['fc_lo'][i]:.1f} – {res['fc_hi'][i]:.1f})")

    if len(resultados_prods) > 10:
        print(f"\n  ... y {len(resultados_prods) - 10} productos más.")

    print(f"\n{'═'*65}\n")


# ─────────────────────────────────────────────
# ⑨ PROGRAMA PRINCIPAL
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Actualiza pronósticos de ventas en BigQuery con Amazon Chronos 2.'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Calcular y mostrar pronóstico sin subir a BigQuery')
    parser.add_argument('--meses', type=int, default=MESES_FORECAST,
                        help=f'Meses a pronosticar (default: {MESES_FORECAST})')
    args = parser.parse_args()

    print("\n" + "═"*65)
    print("  ICONIC TERROIRS — Actualización de Pronósticos")
    print("  Método: Amazon Chronos 2 (chronos-bolt-small / CPU)")
    print("═"*65)

    verificar_dependencias()

    # Conectar a BigQuery
    if not args.dry_run:
        print("\n🔌 Conectando a BigQuery...")
        try:
            client = get_bq_client()
            print(f"   ✅ Conectado al proyecto: {PROYECTO}")
        except Exception as e:
            print(f"   ❌ Error de conexión: {e}")
            sys.exit(1)
    else:
        print("\n⚠️  Modo DRY RUN — no se subirá nada a BigQuery")
        client = None

    # ── INICIALIZAR PIPELINE DE CHRONOS ──
    pipeline = init_pipeline()

    # ── SERIE TOTAL ──
    print("\n① Procesando serie total de ventas...")
    if args.dry_run:
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
            'anno': [p.year for p in periodos],
            'mes': [p.month for p in periodos],
            'venta_neta': ventas_locales,
        })
        print(f"   ✅ Usando {len(df_hist)} meses de datos locales (dry-run)")
    else:
        df_hist = leer_serie_total(client)

    print("   🧠 Calculando pronóstico total con Chronos 2...")
    resultado_total = pronosticar_chronos(
        serie=df_hist['venta_neta'].values,
        pipeline=pipeline,
        meses_fc=args.meses,
        num_samples=NUM_SAMPLES,
    )
    df_fc_total = construir_df_forecast_total(df_hist, resultado_total, args.meses)

    # ── SERIE POR PRODUCTO ──
    print(f"\n② Procesando series de todos los artículos...")
    if args.dry_run:
        prod_data = {
            'CANTINE DEI ROSSO DI MONTEPULCIANO': {
                'marca': 'CANTINE DEI',
                'uds': [5,8,12,10,7,15,13,6,11,16,10,16,9,12,14,18,14,16,
                        20,18,22,18,11,16,18,21,21,19,27,26,16,15,22,21,15,
                        15,18,21,19,22,20,26,18,23,21,24,19,11]
            },
            'TALIS PINOT GRIGIO': {
                'marca': 'TALIS',
                'uds': [10,15,18,16,12,25,22,11,19,27,18,27,16,21,25,32,24,
                        27,34,31,38,31,20,27,30,36,37,32,45,45,27,26,38,36,
                        27,25,31,37,33,38,35,44,32,39,36,41,33,20]
            },
        }
        periodos_p = pd.date_range(start='2022-03-01', periods=48, freq='MS')
        rows_p = []
        for prod_name, info in prod_data.items():
            for i, u in enumerate(info['uds']):
                rows_p.append({
                    'PRODUCTO': prod_name,
                    'marca': info['marca'],
                    'periodo': periodos_p[i],
                    'anno': periodos_p[i].year,
                    'mes': periodos_p[i].month,
                    'unidades': u,
                })
        df_hist_prods = pd.DataFrame(rows_p)
        print(f"   ✅ Usando {len(prod_data)} productos de ejemplo (dry-run)")
    else:
        df_hist_prods = leer_serie_productos(client)

    # Calcular pronóstico por producto
    resultados_prods = {}
    productos_unicos = df_hist_prods['PRODUCTO'].unique()
    total_prods = len(productos_unicos)

    rango_total = pd.date_range(
        start=df_hist['periodo'].min(),
        end=df_hist['periodo'].max(),
        freq='MS'
    )

    for idx, producto in enumerate(productos_unicos, 1):
        df_p = df_hist_prods[df_hist_prods['PRODUCTO'] == producto].copy()
        marca = df_p['marca'].iloc[0] if 'marca' in df_p.columns else 'Sin Marca'

        # Rellenar meses faltantes con 0
        serie_completa = (
            df_p.set_index('periodo')['unidades']
            .reindex(rango_total, fill_value=0)
            .values
        )

        print(f"   [{idx:>3}/{total_prods}] 🔮 {producto[:50]}", end='\r')

        res = pronosticar_chronos(
            serie=serie_completa,
            pipeline=pipeline,
            meses_fc=args.meses,
            num_samples=NUM_SAMPLES,
        )
        resultados_prods[producto] = (res, marca)

    print(f"\n   ✅ Pronósticos calculados para {total_prods} artículos.")

    df_fc_prods = construir_df_forecast_productos(
        df_hist_prods, resultados_prods, args.meses
    )

    # ── REPORTE ──
    imprimir_reporte(df_fc_total, resultado_total, df_fc_prods, resultados_prods)

    # ── SUBIR A BIGQUERY ──
    if not args.dry_run:
        print("③ Subiendo tablas a BigQuery...")
        try:
            subir_a_bigquery(client, df_fc_total,  TABLA_FORECAST, "Pronóstico Total")
            subir_a_bigquery(client, df_fc_prods,  TABLA_PROD_FC,  "Pronóstico Productos")
            print(f"\n🎉 ¡Tablas actualizadas exitosamente en {PROYECTO}.{DATASET}!")
            print(f"   Dashboard reflejará los nuevos datos en el próximo refresco.\n")
        except Exception as e:
            print(f"\n❌ Error al subir datos: {e}")
            print("   Verificar permisos: la cuenta de servicio necesita BigQuery Data Editor.\n")
            sys.exit(1)
    else:
        print("ℹ️  Modo dry-run: datos calculados pero NO subidos a BigQuery.")
        df_fc_total.to_csv("forecast_total_preview.csv", index=False, encoding='utf-8-sig')
        fc_prd_preview = df_fc_prods[df_fc_prods['tipo'] == 'pronóstico']
        fc_prd_preview.to_csv("forecast_productos_preview.csv", index=False, encoding='utf-8-sig')
        print(f"   📄 CSV exportados: forecast_total_preview.csv, forecast_productos_preview.csv\n")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()