import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os

CREDENTIALS = r"C:\Dashboard_Iconic\Antigravity\BD\credentials.json"

# ============================================================
#  CONFIGURACIÓN - Agrega aquí todos tus archivos
# ============================================================
ARCHIVOS = [
    {
        "excel":      r"C:\Dashboard_Iconic\Antigravity\BD\Ventas\TB_Ventas_Cabecera.xlsx",
        "sheet_id":   "1g2ySPgLKN1O6ep3UuLzyx1YPycLE6IFzmljFw_GCWfQ",
        "hoja":       "Hoja 1"
    },
    {
        "excel":      r"C:\Dashboard_Iconic\Antigravity\BD\Ventas\TB_Ventas_Detalle.xlsx",
        "sheet_id":   "18TrvK6wDfzgJ1Ke3C8ushitUcPDZzUMmNTolaDxnZ3g",
        "hoja":       "Hoja 1"
    },
    {
        "excel":      r"C:\Dashboard_Iconic\Antigravity\BD\Inventario\TB_mov_inventario.xlsx",
        "sheet_id":   "1RaT0Vdx4rt0x-QnfhS0_FMVpTse_kljVrHPN8sQ-ZcQ",
        "hoja":       "Hoja 1"
    },
    {
        "excel":      r"C:\Dashboard_Iconic\Antigravity\BD\Productos\TB_Productos.xlsx",
        "sheet_id":   "1a0zFpAbe48W0-JqITFFCNAlx0JQlDDNCiOCaCLWM7gU",
        "hoja":       "Hoja 1"
    },
    # Agrega los que necesites...
]
# ============================================================

def get_sheet(sheet_id, hoja):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id).worksheet(hoja)

def convertir_valor(val):
    import math
    import datetime

    # Primero verificar NaT y NaN antes que cualquier otra cosa
    try:
        if pd.isnull(val):
            return ""
    except (TypeError, ValueError):
        pass

    if val is None:
        return ""
    if isinstance(val, (pd.Timestamp, datetime.datetime)):
        return val.strftime("%d/%m/%Y") if val.hour or val.minute else val.strftime("%d/%m/%Y")
    if isinstance(val, datetime.date):
        return val.strftime("%d/%m/%Y")
    if isinstance(val, float):
        if math.isnan(val):
            return ""
        if val.is_integer():
            return int(val)
        return val
    return val

def sync_new_rows(config):
    excel_path = config["excel"]
    sheet_id   = config["sheet_id"]
    hoja       = config["hoja"]
    nombre     = os.path.basename(excel_path)

    try:
        print(f"🔄 [{nombre}] Sincronizando...")

        # Leer Excel SIN forzar dtype=str, para preservar tipos
        df = pd.read_excel(excel_path)

        # Reemplazar NaN por string vacío
        df = df.where(pd.notnull(df), None)

        # Convertir cada celda al tipo correcto
        df = df.apply(lambda col: col.map(convertir_valor))

        sheet = get_sheet(sheet_id, hoja)
        existing = sheet.get_all_values()
        existing_count = len(existing)

        if existing_count == 0:
            data = [df.columns.tolist()] + df.values.tolist()
            sheet.append_rows(data, value_input_option="USER_ENTERED")
            print(f"✅ [{nombre}] Hoja vacía. Se subieron {len(df)} filas + encabezado.")
            return

        filas_en_sheets = existing_count - 1
        filas_en_excel  = len(df)

        if filas_en_excel > filas_en_sheets:
            nuevas = df.iloc[filas_en_sheets:]
            sheet.append_rows(nuevas.values.tolist(), value_input_option="USER_ENTERED")
            print(f"✅ [{nombre}] {len(nuevas)} fila(s) nueva(s) agregada(s).")
        else:
            print(f"ℹ️  [{nombre}] No hay filas nuevas.")

    except Exception as e:
        print(f"❌ [{nombre}] Error: {e}")

class ExcelHandler(FileSystemEventHandler):
    def __init__(self, config):
        self.config    = config
        self.last_sync = 0

    def on_modified(self, event):
        if os.path.abspath(event.src_path) != os.path.abspath(self.config["excel"]):
            return
        now = time.time()
        if now - self.last_sync < 3:
            return
        self.last_sync = now
        time.sleep(1.5)
        sync_new_rows(self.config)

if __name__ == "__main__":
    print(f"👀 Monitoreando {len(ARCHIVOS)} archivo(s)...\n")

    observers = []
    for config in ARCHIVOS:
        # Sincronización inicial
        sync_new_rows(config)

        # Crear un observer por cada archivo
        folder = os.path.dirname(os.path.abspath(config["excel"]))
        handler = ExcelHandler(config)
        observer = Observer()
        observer.schedule(handler, path=folder, recursive=False)
        observer.start()
        observers.append(observer)
        print(f"   ✔ Monitoreando: {config['excel']}")

    print("\n   Presiona Ctrl+C para detener.\n")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        for obs in observers:
            obs.stop()
    for obs in observers:
        obs.join()