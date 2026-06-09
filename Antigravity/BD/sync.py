"""
╔══════════════════════════════════════════════════════════════════╗
║  ICONIC TERROIRS — Sincronizador Excel → Google Sheets          ║
║  Detecta nuevas filas comparando la última clave del Excel       ║
║  vs la última clave en GSheet (no solo por conteo).             ║
╚══════════════════════════════════════════════════════════════════╝
Uso: python sync.py          (modo monitor - mantiene abierto)
"""
import time
import math
import datetime
import unicodedata
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os, sys

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

CREDENTIALS = r"C:\Dashboard_Iconic\Antigravity\BD\credentials.json"

# ============================================================
#  CONFIGURACIÓN - Agrega aquí todos tus archivos
#  key_col: columna de clave primaria (col 0 = primera columna)
#  Si es None, se usa comparación por conteo de filas.
# ============================================================
ARCHIVOS = [
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Ventas\TB_Ventas_Cabecera.xlsx",
        "sheet_id": "1g2ySPgLKN1O6ep3UuLzyx1YPycLE6IFzmljFw_GCWfQ",
        "hoja":     "Hoja 1",
        "key_col":  0,          # COD_VENTA es la clave primaria
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Ventas\TB_Ventas_Detalle.xlsx",
        "sheet_id": "18TrvK6wDfzgJ1Ke3C8ushitUcPDZzUMmNTolaDxnZ3g",
        "hoja":     "Hoja 1",
        "key_col":  0,          # COD_VENTA
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Inventario\TB_mov_inventario.xlsx",
        "sheet_id": "1RaT0Vdx4rt0x-QnfhS0_FMVpTse_kljVrHPN8sQ-ZcQ",
        "hoja":     "Hoja 1",
        "key_col":  0,          # COD_INVENTARIO
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Productos\TB_Productos.xlsx",
        "sheet_id": "1a0zFpAbe48W0-JqITFFCNAlx0JQlDDNCiOCaCLWM7gU",
        "hoja":     "Hoja 1",
        "key_col":  None,       # sin clave primaria clara → comparar por conteo
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Finanzas\TB_movimientos_contabilidad.xlsx",
        "sheet_id": "1MK8QvGDKJmAb3dJ-5HIx_lT_pLFCpCkYIdMaGcm0zPA",
        "hoja":     "Hoja 1",
        "key_col":  None,
    },
    # Agrega los que necesites...
]
# ============================================================

# Renombrado de columnas con acentos/ñ → ASCII para BigQuery
COLUMN_RENAME = {
    "AÑO":  "ANNO",
    "A\u00f1O": "ANNO",
}

def normalizar_col(col):
    """Normaliza nombre de columna: quita acentos/ñ, pasa a mayúsculas."""
    try:
        nkfd = unicodedata.normalize('NFKD', str(col).upper().strip())
        ascii_col = ''.join(c for c in nkfd if ord(c) < 128)
        return COLUMN_RENAME.get(ascii_col, ascii_col)
    except Exception:
        return col

def get_sheet(sheet_id, hoja):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id).worksheet(hoja)

def convertir_valor(val):
    try:
        if pd.isnull(val):
            return ""
    except (TypeError, ValueError):
        pass
    if val is None:
        return ""
    if isinstance(val, (pd.Timestamp, datetime.datetime)):
        return val.strftime("%d/%m/%Y")
    if isinstance(val, datetime.date):
        return val.strftime("%d/%m/%Y")
    if isinstance(val, float):
        if math.isnan(val):
            return ""
        if val.is_integer():
            return int(val)
        return round(val, 6)
    return val

def sync_new_rows(config):
    excel_path = config["excel"]
    sheet_id   = config["sheet_id"]
    hoja       = config["hoja"]
    key_col    = config.get("key_col", None)
    nombre     = os.path.basename(excel_path)

    try:
        print(f"[SYNC] {nombre}...")

        # 1. Leer Excel
        df = pd.read_excel(excel_path)

        # 2. Normalizar nombres de columnas
        df.columns = [normalizar_col(c) for c in df.columns]

        # 3. Convertir valores
        df = df.where(pd.notnull(df), None)
        df = df.apply(lambda col: col.map(convertir_valor))

        # 4. Obtener estado actual del GSheet
        sheet    = get_sheet(sheet_id, hoja)
        existing = sheet.get_all_values()
        existing_count = len(existing)  # incluye header

        # ── Hoja vacía: subir todo ──────────────────────────────
        if existing_count == 0:
            all_data = [df.columns.tolist()] + df.values.tolist()
            sheet.append_rows(all_data, value_input_option="USER_ENTERED")
            print(f"  [OK] Hoja vacia. {len(df)} filas subidas.")
            return

        filas_sheet = existing_count - 1  # excluir header
        filas_excel = len(df)

        # ── Comparar por clave primaria ─────────────────────────
        if key_col is not None and filas_sheet > 0:
            # Obtener el conjunto de claves ya en GSheet
            keys_in_sheet = set(row[key_col] for row in existing[1:] if len(row) > key_col and row[key_col])

            # Filas del Excel cuya clave NO está en el GSheet
            nuevas_df = df[~df.iloc[:, key_col].astype(str).isin(keys_in_sheet)]

            if len(nuevas_df) == 0:
                print(f"  [--] Sin filas nuevas ({filas_sheet} en GSheet = {filas_excel} en Excel).")
                return

            print(f"  [+]  {len(nuevas_df)} filas nuevas detectadas (por clave). Subiendo...")
            sheet.append_rows(nuevas_df.values.tolist(), value_input_option="USER_ENTERED")
            print(f"  [OK] {len(nuevas_df)} fila(s) agregadas.")

        # ── Comparar por conteo (fallback) ──────────────────────
        else:
            if filas_excel > filas_sheet:
                nuevas = df.iloc[filas_sheet:]
                sheet.append_rows(nuevas.values.tolist(), value_input_option="USER_ENTERED")
                print(f"  [OK] {len(nuevas)} fila(s) nuevas por conteo.")
            else:
                print(f"  [--] Sin filas nuevas (conteo: {filas_sheet} GSheet / {filas_excel} Excel).")

    except Exception as e:
        print(f"  [ERROR] {nombre}: {e}")


class ExcelHandler(FileSystemEventHandler):
    def __init__(self, config):
        self.config    = config
        self.last_sync = 0

    def on_modified(self, event):
        if os.path.abspath(event.src_path) != os.path.abspath(self.config["excel"]):
            return
        now = time.time()
        if now - self.last_sync < 5:   # debounce 5s
            return
        self.last_sync = now
        time.sleep(2)                   # esperar a que Excel termine de escribir
        sync_new_rows(self.config)


if __name__ == "__main__":
    print(f"ICONIC TERROIRS - Monitor de sync ({len(ARCHIVOS)} archivo(s))\n")

    observers = []
    for config in ARCHIVOS:
        # Sincronización inicial al arrancar
        sync_new_rows(config)

        # Crear observer para detectar cambios en el Excel
        folder  = os.path.dirname(os.path.abspath(config["excel"]))
        handler = ExcelHandler(config)
        obs     = Observer()
        obs.schedule(handler, path=folder, recursive=False)
        obs.start()
        observers.append(obs)
        print(f"  Monitoreando: {config['excel']}")

    print("\n  Presiona Ctrl+C para detener.\n")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        for obs in observers:
            obs.stop()
    for obs in observers:
        obs.join()