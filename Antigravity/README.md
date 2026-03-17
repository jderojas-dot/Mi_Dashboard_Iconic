# 🍷 Iconic Terroirs — Intelligence Dashboard 24/7
## Aplicación web full-stack: BigQuery → FastAPI → Frontend

---

## 🏗️ Estructura del Proyecto
El proyecto ha sido organizado para producción en la carpeta **Antigravity**:

```
Dashboard_Iconic/
└── Antigravity/
    ├── backend/         ← API FastAPI (Motor del Dashboard)
    │   ├── main.py      ← Archivo principal de la API
    │   └── config.py    ← Configuración de BigQuery
    └── frontend/
        └── public/
            └── index.html ← Interfaz visual (Dashboard)
```

---

## 🚀 Pasos para Independencia Total (24/7)

Para que el dashboard funcione siempre, sin depender de tu computadora encendida, sigue estos pasos:

### 1. Subir cambios a GitHub
Como hemos movido los archivos a la carpeta `Antigravity`, debes actualizar tu repositorio en GitHub. 
*   Si usas la web de GitHub: Sube la carpeta `Antigravity` completa.
*   Si usas comandos: `git add .`, `git commit -m "Reorganización Antigravity"`, `git push`.

### 2. Configurar Render.com (Backend)
Entra a tu servicio en Render y ajusta esto en **Settings**:
*   **Root Directory:** `Antigravity/backend` (Antes era `backend`).
*   **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
*   **Environment Variables:** Asegúrate de tener `GOOGLE_APPLICATION_CREDENTIALS_JSON` con el contenido de tu archivo de credenciales.

### 3. Publicar el Frontend (GitHub Pages)
1. En tu repositorio de GitHub, ve a **Settings** → **Pages**.
2. En **Build and deployment** → **Branch**, selecciona `main` y la carpeta `/(root)`.
3. Dale a **Save**.
4. **Link de acceso:** Tu dashboard estará en:
   `https://[tu-usuario].github.io/[nombre-repo]/Antigravity/frontend/public/index.html`

---

## 🛠️ Tecnologías
*   **BigQuery:** Base de datos persistente en Google Cloud.
*   **FastAPI:** Backend rápido y eficiente.
*   **Chart.js:** Visualizaciones premium.
*   **GitHub Pages:** Hosting gratuito y eterno para el frontend.
