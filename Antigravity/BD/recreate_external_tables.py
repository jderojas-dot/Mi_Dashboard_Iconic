import os
import sys
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Import configuration from backend
sys.path.append(str(Path(__file__).parent.parent / "backend"))
try:
    from config import BQ_PROJECT, BQ_DATASET, CREDENTIALS_PATH
except ImportError:
    print("[ERROR] No se pudo cargar config.py. Usando valores por defecto.")
    BQ_PROJECT = "dashboard-iconic-terroirs"
    BQ_DATASET = "Mis_Tablas"
    CREDENTIALS_PATH = str(Path(__file__).parent.parent / "credentials.json")

# Google Sheets IDs from sync.py
GOOGLE_SHEETS = {
    "TB_Ventas_Cabecera": "1g2ySPgLKN1O6ep3UuLzyx1YPycLE6IFzmljFw_GCWfQ",
    "TB_Ventas_Detalle": "18TrvK6wDfzgJ1Ke3C8ushitUcPDZzUMmNTolaDxnZ3g",
    "TB_mov_inventario": "1RaT0Vdx4rt0x-QnfhS0_FMVpTse_kljVrHPN8sQ-ZcQ",
    "TB_Productos": "1a0zFpAbe48W0-JqITFFCNAlx0JQlDDNCiOCaCLWM7gU"
}

def get_client():
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive"
    ]
    if CREDENTIALS_PATH and os.path.exists(CREDENTIALS_PATH):
        print(f"[INFO] Usando archivo de credenciales: {CREDENTIALS_PATH}")
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH, scopes=scopes
        )
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)
    print("[INFO] Usando Google Cloud Application Default Credentials (ADC)...")
    return bigquery.Client(project=BQ_PROJECT)

def get_schema_for_table(table_name):
    if table_name == "TB_Productos":
        cols = [
            ("COD_PRODUCTO", "STRING"),
            ("DESCRIPCION_PRODUCTO_SERVICIO", "STRING"),
            ("TIPO_PRODUCTO", "STRING"),
            ("NOMBRE_TIPO_BEBIDA", "STRING"),
            ("PAIS_ORIGEN_PRODUCTO", "STRING"),
            ("TIPO_EXISTENCIA", "STRING"),
            ("MARCA_PRODUCTO", "STRING"),
            ("FECHA_REGISTRO_PRODUCTO", "STRING")
        ]
    elif table_name == "TB_Ventas_Detalle":
        cols = [
            ("COD_VENTA", "STRING"),
            ("COD_PRODUCTO", "STRING"),
            ("DESCRIPCION_PRODUCTO_SERVICIO", "STRING"),
            ("CANTIDAD_VENTA", "FLOAT64"),
            ("VALOR_UNIT_VENTA_ITEM", "FLOAT64"),
            ("VALOR_NETO_VENTA_ITEM", "FLOAT64"),
            ("IGV_VENTA_ITEM", "FLOAT64"),
            ("TOTAL_VENTA_ITEM", "FLOAT64"),
            ("CLAVE_FACTURA", "STRING"),
            ("CLAVE_INVENTARIO", "STRING")
        ]
    elif table_name == "TB_mov_inventario":
        cols = [
            ("COD_INVENTARIO", "STRING"),
            ("FECHA_MOV_INVENTARIO", "STRING"),
            ("ANNO", "INT64"),
            ("REG_CONTABLE", "STRING"),
            ("TIPO_OPERACION", "STRING"),
            ("TIPO_MOVIMIENTO", "STRING"),
            ("COD_PRODUCTO", "STRING"),
            ("DESCRIPCION_PRODUCTO_SERVICIO", "STRING"),
            ("NOMBRE_ALMACEN", "STRING"),
            ("ENTRADA_SALIDA", "STRING"),
            ("CANTIDAD_ENTRADA_SALIDA", "FLOAT64"),
            ("COSTO_UNIT_MN", "FLOAT64"),
            ("COSTO_TOTAL_MN", "FLOAT64"),
            ("COSTO_UNIT_ME", "FLOAT64"),
            ("COSTO_TOTAL_ME", "FLOAT64"),
            ("TIPO_MONEDA_MOV_INVENTARIO", "STRING"),
            ("TIPO_CAMBIO_MOV_INVENTARIO", "FLOAT64"),
            ("USUARIO_CREACION_MOV_INVENTARIO", "STRING"),
            ("FECHA_REGISTRO", "STRING"),
            ("USUARIO_MODIFICACION", "STRING"),
            ("FECHA_MODIFICACION", "STRING"),
            ("TDI_CLIENTE_PROVEEDOR", "STRING"),
            ("DOC_CLIENTE_PROVEEDOR", "STRING"),
            ("CLIENTE_PROVEEDOR", "STRING"),
            ("TIPO_DOC_INVENTARIO", "STRING"),
            ("SERIE_DOC_INVENTARIO", "STRING"),
            ("NUMERO_DOC_INVENTARIO", "STRING"),
            ("CLAVE_INVENTARIO", "STRING")
        ]
    elif table_name == "TB_Ventas_Cabecera":
        cols = [
            ("COD_VENTA", "STRING"),
            ("ANNO", "INT64"),
            ("MES", "INT64"),
            ("REG_CONTABLE", "STRING"),
            ("FECHA_MOV", "STRING"),
            ("TIPO_DOC_VENTA", "STRING"),
            ("SERIE_DOC_VENTA", "STRING"),
            ("NUMERO_DOC_VENTA", "STRING"),
            ("TDI_CLIENTE", "STRING"),
            ("DOC_CLIENTE", "STRING"),
            ("CLIENTE", "STRING"),
            ("FECHA_EMISION", "STRING"),
            ("FECHA_VENCIMIENTO", "STRING"),
            ("FORMA_PAGO", "STRING"),
            ("COD_MONEDA", "STRING"),
            ("TIPO_MONEDA_VENTA", "STRING"),
            ("TIPO_CAMBIO_VENTA", "FLOAT64"),
            ("TDI_VENDEDOR", "STRING"),
            ("DOC_VENDEDOR", "STRING"),
            ("VENDEDOR", "STRING"),
            ("ESTADO_ANULADO", "STRING"),
            ("VALOR_NETO_TOTAL_VENTA", "FLOAT64"),
            ("DESCUENTO_VENTA_TOTAL", "FLOAT64"),
            ("VALOR_AFECTO_TOTAL", "FLOAT64"),
            ("VALOR_INAFECTO_TOTAL", "FLOAT64"),
            ("IGV_VENTAS_TOTAL", "FLOAT64"),
            ("TOTAL_VENTA", "FLOAT64"),
            ("RETENCION_VENTA", "FLOAT64"),
            ("TOTAL_VENTA_POR_COBRAR", "FLOAT64"),
            ("OBSERVACION_VENTA", "STRING"),
            ("DOC_REF_VENTA_FECHA", "STRING"),
            ("DOC_REF_VENTA_TIPO", "STRING"),
            ("DOC_REF_VENTA_SERIE", "STRING"),
            ("DOC_REF_VENTA_NUMERO", "STRING"),
            ("DOC_ELECTRONICO_VENTA", "STRING"),
            ("DOC_ELECTRONICO_ENV_SUNAT_VENTA", "STRING"),
            ("DOC_ELECTRONICO_DESCRIP_RESP_VENTA", "STRING"),
            ("FECHA_REG_DOC_ELECTRONICO_VENTA", "STRING"),
            ("USUARIO_CREACION_VENTA", "STRING"),
            ("CLAVE_FACTURA", "STRING")
        ]
    else:
        return []
    return [bigquery.SchemaField(name, ftype) for name, ftype in cols]

def create_external_table(client, table_name, sheet_id):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    
    # 1. Eliminar la tabla si ya existe para evitar conflictos
    try:
        client.delete_table(table_id)
        print(f"[DELETE] Tabla anterior eliminada: {table_id}")
    except Exception:
        pass
        
    # 2. Configurar la tabla externa con esquema explícito
    table = bigquery.Table(table_id)
    table.schema = get_schema_for_table(table_name)
    
    external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
    external_config.source_uris = [f"https://docs.google.com/spreadsheets/d/{sheet_id}"]
    external_config.options.skip_leading_rows = 1
    external_config.autodetect = False  # Usar nuestro esquema exacto
    
    table.external_data_configuration = external_config
    
    # 3. Crear la tabla
    client.create_table(table)
    print(f"[OK] Tabla externa creada con exito: {table_id}")

def create_base_view(client):
    view_id = f"{BQ_PROJECT}.{BQ_DATASET}.VW_VENTAS_DASHBOARD"
    
    # Query de union robusta con parseo de fecha
    view_query = f"""
    SELECT
      d.COD_VENTA,
      PARSE_DATE('%d/%m/%Y', c.FECHA_MOV) AS FECHA,
      CAST(c.ANNO AS INT64) AS ANNO,
      CAST(c.MES AS INT64) AS MES,
      c.CLIENTE,
      CAST(c.DOC_CLIENTE AS STRING) AS DOC_CLIENTE,
      c.VENDEDOR,
      c.FORMA_PAGO,
      c.TIPO_DOC_VENTA,
      c.TIPO_MONEDA_VENTA,
      CAST(c.TIPO_CAMBIO_VENTA AS FLOAT64) AS TIPO_CAMBIO_VENTA,
      d.COD_PRODUCTO,
      d.DESCRIPCION_PRODUCTO_SERVICIO AS PRODUCTO,
      CAST(d.CANTIDAD_VENTA AS FLOAT64) AS CANTIDAD,
      CAST(d.VALOR_UNIT_VENTA_ITEM AS FLOAT64) AS PRECIO_UNIT_ME,
      CAST(d.VALOR_NETO_VENTA_ITEM AS FLOAT64) AS VENTA_NETA_ME,
      CAST(d.IGV_VENTA_ITEM AS FLOAT64) AS IGV,
      CAST(d.TOTAL_VENTA_ITEM AS FLOAT64) AS TOTAL_ITEM,
      d.CLAVE_INVENTARIO,
      CAST(CASE WHEN c.TIPO_MONEDA_VENTA = 'US$' THEN d.VALOR_NETO_VENTA_ITEM * c.TIPO_CAMBIO_VENTA ELSE d.VALOR_NETO_VENTA_ITEM END AS FLOAT64) AS VENTA_NETA_MN,
      IFNULL(CAST(i.COSTO_TOTAL_MN AS FLOAT64), 0.0) AS COSTO_MN_TOTAL,
      IFNULL(CAST(i.COSTO_TOTAL_ME AS FLOAT64), 0.0) AS COSTO_ME_TOTAL,
      CAST((CASE WHEN c.TIPO_MONEDA_VENTA = 'US$' THEN d.VALOR_NETO_VENTA_ITEM * c.TIPO_CAMBIO_VENTA ELSE d.VALOR_NETO_VENTA_ITEM END) - IFNULL(i.COSTO_TOTAL_MN, 0.0) AS FLOAT64) AS MARGEN_MN,
      p.PAIS_ORIGEN_PRODUCTO,
      p.MARCA_PRODUCTO,
      p.NOMBRE_TIPO_BEBIDA,
      p.TIPO_PRODUCTO
    FROM `{BQ_PROJECT}.{BQ_DATASET}.TB_Ventas_Detalle` d
    INNER JOIN `{BQ_PROJECT}.{BQ_DATASET}.TB_Ventas_Cabecera` c ON d.COD_VENTA = c.COD_VENTA
    LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.TB_Productos` p ON d.COD_PRODUCTO = p.COD_PRODUCTO
    LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.TB_mov_inventario` i ON d.CLAVE_INVENTARIO = i.CLAVE_INVENTARIO
    """
    
    # 1. Eliminar vista anterior si existe
    try:
        client.delete_table(view_id)
        print(f"[DELETE] Vista anterior eliminada: {view_id}")
    except Exception:
        pass
        
    # 2. Crear la nueva vista
    view = bigquery.Table(view_id)
    view.view_query = view_query
    client.create_table(view)
    print(f"[OK] Vista base VW_VENTAS_DASHBOARD recreada con exito.")

def main():
    print("[START] Iniciando restauracion de tablas externas y vista base...")
    try:
        client = get_client()
        client.list_datasets(max_results=1)
        print("[OK] Conexion establecida con exito a BigQuery.")
    except Exception as e:
        print(f"[ERROR] FALLO DE CONEXION: {e}")
        return
        
    # Recrear tablas externas
    for table_name, sheet_id in GOOGLE_SHEETS.items():
        print(f"\n[TABLE] Creando tabla externa: {table_name}")
        try:
            create_external_table(client, table_name, sheet_id)
        except Exception as e:
            print(f"[ERROR] Al crear tabla {table_name}: {e}")
            
    # Recrear vista base
    print("\n[VIEW] Recreando vista base...")
    try:
        create_base_view(client)
    except Exception as e:
        print(f"[ERROR] Al crear vista base: {e}")
        
    print("\n[END] Proceso de restauracion basica completado.")

if __name__ == "__main__":
    main()
