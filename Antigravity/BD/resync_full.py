"""
╔══════════════════════════════════════════════════════════════════╗
║  ICONIC TERROIRS — Re-Sync Completo Excel → Google Sheets       ║
║  Limpia cada hoja y re-sube TODOS los datos desde los Excel     ║
╚══════════════════════════════════════════════════════════════════╝
Uso: python resync_full.py
"""
import sys, os, time, math, datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

CREDENTIALS = r"C:\Dashboard_Iconic\Antigravity\BD\credentials.json"

ARCHIVOS = [
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Ventas\TB_Ventas_Cabecera.xlsx",
        "sheet_id": "1g2ySPgLKN1O6ep3UuLzyx1YPycLE6IFzmljFw_GCWfQ",
        "hoja":     "Hoja 1",
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Ventas\TB_Ventas_Detalle.xlsx",
        "sheet_id": "18TrvK6wDfzgJ1Ke3C8ushitUcPDZzUMmNTolaDxnZ3g",
        "hoja":     "Hoja 1",
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Inventario\TB_mov_inventario.xlsx",
        "sheet_id": "1RaT0Vdx4rt0x-QnfhS0_FMVpTse_kljVrHPN8sQ-ZcQ",
        "hoja":     "Hoja 1",
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Productos\TB_Productos.xlsx",
        "sheet_id": "1a0zFpAbe48W0-JqITFFCNAlx0JQlDDNCiOCaCLWM7gU",
        "hoja":     "Hoja 1",
    },
    {
        "excel":    r"C:\Dashboard_Iconic\Antigravity\BD\Finanzas\TB_movimientos_contabilidad.xlsx",
        "sheet_id": "1MK8QvGDKJmAb3dJ-5HIx_lT_pLFCpCkYIdMaGcm0zPA",
        "hoja":     "Hoja 1",
    },
]

# Mapeo de renombrado de columnas (Excel tiene acentos/ñ, BigQuery necesita ASCII)
COLUMN_RENAME = {
    "AÑO":  "ANNO",
    "A\u00f1O": "ANNO",
    "A\xd1O": "ANNO",
    "A?O":  "ANNO",
}

def get_client():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS, scopes=scopes)
    return gspread.authorize(creds)


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


def resync_file(config, client):
    excel_path = config["excel"]
    sheet_id   = config["sheet_id"]
    hoja       = config["hoja"]
    nombre     = os.path.basename(excel_path)

    print(f"\n{'='*60}")
    print(f"[{nombre}] Iniciando re-sync completo...")
    t0 = time.time()

    # 1. Leer Excel
    print(f"  [1/4] Leyendo Excel...")
    df = pd.read_excel(excel_path)
    print(f"        {len(df)} filas leidas.")

    # 2. Renombrar columnas con acentos/ñ a versiones ASCII
    renamed = {}
    for col in df.columns:
        col_upper = col.upper().strip()
        if col_upper in COLUMN_RENAME:
            renamed[col] = COLUMN_RENAME[col_upper]
        else:
            # Intentar normalizar cualquier caracter raro
            try:
                import unicodedata
                normalized = unicodedata.normalize('NFKD', col_upper)
                ascii_col  = ''.join(c for c in normalized if ord(c) < 128)
                if ascii_col != col_upper:
                    renamed[col] = ascii_col
            except Exception:
                pass
    if renamed:
        print(f"        Columnas renombradas: {renamed}")
        df = df.rename(columns=renamed)

    # 3. Convertir valores
    print(f"  [2/4] Convirtiendo tipos de datos...")
    df = df.where(pd.notnull(df), None)
    df = df.apply(lambda col: col.map(convertir_valor))

    # 4. Limpiar la hoja y re-subir
    print(f"  [3/4] Accediendo a Google Sheets...")
    worksheet = client.open_by_key(sheet_id).worksheet(hoja)

    print(f"  [4/4] Limpiando hoja y subiendo {len(df)} filas + encabezado...")
    worksheet.clear()
    time.sleep(2)  # Esperar para evitar rate limit

    headers = df.columns.tolist()
    data    = df.values.tolist()
    all_data = [headers] + data

    # Subir en lotes de 5000 filas para evitar timeouts
    BATCH = 5000
    if len(all_data) <= BATCH:
        worksheet.append_rows(all_data, value_input_option="USER_ENTERED")
    else:
        print(f"        Subiendo en lotes (total {len(all_data)} filas)...")
        # Primera fila
        worksheet.append_rows(all_data[:BATCH], value_input_option="USER_ENTERED")
        for start in range(BATCH, len(all_data), BATCH):
            chunk = all_data[start:start + BATCH]
            print(f"        Lote {start//BATCH + 1}: filas {start}-{start+len(chunk)}...")
            worksheet.append_rows(chunk, value_input_option="USER_ENTERED")
            time.sleep(1)

    elapsed = time.time() - t0
    print(f"  OK [{nombre}] {len(df)} filas subidas en {elapsed:.1f}s")


def main():
    print("ICONIC TERROIRS - Re-Sync Completo Excel -> Google Sheets")
    print("="*60)

    try:
        client = get_client()
        print("[OK] Conectado a Google Sheets.\n")
    except Exception as e:
        print(f"[ERROR] No se pudo conectar: {e}")
        sys.exit(1)

    errores = []
    for config in ARCHIVOS:
        try:
            resync_file(config, client)
        except Exception as e:
            nombre = os.path.basename(config["excel"])
            print(f"\n[ERROR] {nombre}: {e}")
            errores.append(nombre)

    print(f"\n{'='*60}")
    if errores:
        print(f"[WARN] Completado con errores en: {', '.join(errores)}")
    else:
        print("[OK] Re-sync completado sin errores.")
    print("\nPROXIMO PASO: ejecutar materializar_vistas.py para actualizar BigQuery")


if __name__ == "__main__":
    main()
