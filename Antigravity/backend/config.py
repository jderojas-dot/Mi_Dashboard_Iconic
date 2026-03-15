# ═══════════════════════════════════════════════════════
#  ICONIC TERROIRS — Configuración del Backend
#  Editar estos valores antes de iniciar el servidor
# ═══════════════════════════════════════════════════════

# ── BigQuery ──
BQ_PROJECT   = "dashboard-iconic-terroirs"
BQ_DATASET   = "Mis_Tablas"

# ── Credenciales ──
# Opción A: ruta al archivo JSON de la cuenta de servicio
# Opción B: dejar en None si usas 'gcloud auth application-default login'
CREDENTIALS_PATH = None
# CREDENTIALS_PATH = "/ruta/a/tu/service-account.json"

# ── CORS: orígenes permitidos ──
# En producción, reemplazar "*" con el dominio real del frontend:
# Ej: ["https://dashboard.iconicterroirs.com"]
ALLOWED_ORIGINS = ["*"]
