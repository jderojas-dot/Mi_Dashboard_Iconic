# ═══════════════════════════════════════════════════════
#  ICONIC TERROIRS — Configuración del Backend
#  Editar estos valores antes de iniciar el servidor
# ═══════════════════════════════════════════════════════

# ── BigQuery ──
BQ_PROJECT   = "dashboard-iconic-terroirs"
BQ_DATASET   = "Mis_Tablas"

# ── Credenciales ──
from pathlib import Path
# El archivo credentials.json está en la carpeta Antigravity (junto a backend y frontend)
CREDENTIALS_PATH = str(Path(__file__).parent.parent / "credentials.json")
# CREDENTIALS_PATH = "/ruta/a/tu/service-account.json"

# ── CORS: orígenes permitidos ──
# En producción, reemplazar "*" con el dominio real del frontend:
# Ej: ["https://dashboard.iconicterroirs.com"]
ALLOWED_ORIGINS = ["*"]
