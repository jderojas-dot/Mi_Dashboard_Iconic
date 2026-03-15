# 🍷 Iconic Terroirs — Intelligence Dashboard
## Aplicación web full-stack: BigQuery → FastAPI → Frontend

---

## Arquitectura del Proyecto

```
iconic-dashboard/
├── backend/
│   ├── main.py          ← API FastAPI (todos los endpoints)
│   ├── config.py        ← Configuración BigQuery + credenciales
│   └── requirements.txt ← Dependencias Python
└── frontend/
    └── public/
        └── index.html   ← Dashboard completo (HTML + CSS + JS)
```

**Flujo de datos:**
```
BigQuery (vistas SQL) → FastAPI (Python) → index.html (Chart.js)
```

---

## OPCIÓN A — Despliegue Local (recomendado para empezar)

### Paso 1: Instalar Python y dependencias

```bash
# Crear entorno virtual
python -m venv venv

# Activar entorno
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Instalar dependencias
pip install -r backend/requirements.txt
```

### Paso 2: Configurar credenciales BigQuery

Editar `backend/config.py`:

```python
# Opción A: con archivo de cuenta de servicio
CREDENTIALS_PATH = "/ruta/a/tu/service-account.json"

# Opción B: con gcloud CLI (ejecutar primero:)
# gcloud auth application-default login
CREDENTIALS_PATH = None
```

### Paso 3: Iniciar el servidor backend

```bash
cd backend
uvicorn main:app --reload --port 8000
```

Verificar que funciona:
```
http://localhost:8000/api/kpis       → KPIs globales
http://localhost:8000/api/kpis?anno=2025 → KPIs año 2025
http://localhost:8000/docs           → Documentación interactiva de la API
```

### Paso 4: Abrir el dashboard

Abrir directamente en el navegador:
```
frontend/public/index.html
```

O servir con un servidor simple:
```bash
cd frontend/public
python -m http.server 3000
# Abrir: http://localhost:3000
```

---

## OPCIÓN B — Despliegue en la Nube (compartir con socios)

Esta opción te permite subir tu dashboard a internet para que cualquier persona con el enlace (link) pueda verlo. Está explicada paso a paso para que sea súper fácil.

### Fase 1: Guardar tu proyecto en GitHub (Tu "caja de seguridad" en internet)

GitHub es una página donde los programadores subimos nuestros archivos. Render necesita que tu proyecto esté allí para poder leerlo.

1. **Entra a [github.com](https://github.com/)** y haz clic en "Sign up" (Registrarse) para crearte una cuenta gratuita si no tienes una. Te pedirá confirmar tu correo.
2. Una vez dentro de tu cuenta, busca un botón verde a la izquierda que dice **"New"** (Nuevo) para crear un nuevo "Repositorio" (es como una carpeta en la nube).
3. En **"Repository name"** ponle un nombre fácil y sin espacios, por ejemplo: `mi-dashboard-vinos`.
4. Déjalo en **"Public"** (Público) y baja hasta hacer clic en el botón verde **"Create repository"**.
5. Te aparecerá una pantalla con código. Haz clic en el texto azul pequeño que dice **"uploading an existing file"** (subir archivos existentes).
6. Arrastra **TODA** la carpeta de tu proyecto de tu computadora hacia esa pantalla de GitHub. Verás cómo empiezan a cargar los archivos (`backend`, `frontend`, `README.md`, etc.).
7. Cuando termine de cargar, baja del todo, escribe un título (como "Subida inicial") y haz clic en el botón verde **"Commit changes"** (Guardar cambios). ¡Listo! Tus archivos ya están en internet.

### Fase 2: Subir el Backend a Render.com (El "motor" de tu dashboard)

Render.com es un servicio que tomará tus archivos de Python y mantendrá la conexión a tu base de datos funcionando las 24 horas del día gratis.

1. **Entra a [render.com](https://render.com/)** y haz clic en **"Get Started"** (Empezar). Para que sea más rápido, usa la opción **"Sign up with GitHub"** (Ingresar usando la cuenta de GitHub que acabas de crear).
2. Cuando estés en el inicio o "Dashboard" de Render, busca el botón de **"New +"** (Nuevo) en la esquina superior derecha y selecciona **"Web Service"** (Servicio web).
3. Selecciona la primera opción: **"Build and deploy from a Git repository"** y haz clic en Next.
4. Te pedirá conectar tu cuenta de GitHub y te aparecerá en la lista el proyecto que creaste (`mi-dashboard-vinos`). Haz clic en el botón **"Connect"** a su derecha.
5. Verás un formulario. No te asustes, solo debes rellenar estas opciones:
   - **Name (Nombre):** Ponle algo como `api-vinos`.
   - **Branch:** Déjalo como esté (suele decir `main`).
   - **Root Directory:** Aquí debes escribir: `backend` *(¡Este paso es súper importante!)*.
   - **Runtime:** Cambia la opción a `Python 3`.
   - **Build Command:** Borra lo que haya y escribe esto exactamente: `pip install -r requirements.txt`
   - **Start Command:** Borra lo que haya y escribe esto exactamente: `uvicorn main:app --host 0.0.0.0 --port $PORT`
6. En la parte de los planes, asegúrate de seleccionar el que dice **"Free" ($0/mes)**.
7. **Baja hasta "Advanced" o "Environment Variables" (Variables de Entorno)** y haz clic en el botón **"Add Environment Variable"** (Añadir variable). 
   - En la primera cajita (la de la izquierda, llamada **Key**), escribe: `GOOGLE_APPLICATION_CREDENTIALS_JSON`
   - En la cajita de al lado (la de la derecha, llamada **Value**), abre el archivo "JSON" de Google que te dieron para conectarte (ese que tiene contraseñas y texto raro), copia todo su contenido, ¡y pégalo aquí!. Esto le dará permiso a Render para entrar a BigQuery.
8. Baja hasta el final y haz clic en el botón morado **"Create Web Service"** (Crear).
9. Verás una pantalla negra con texto apareciendo. Tráete un café y **espera unos minutos**. Cuando termine de cargar, arriba aparecerá un texto que dice **"Live"** en verde.
10. Arriba de todo, debajo del nombre de tu servicio, verás una dirección web similar a `https://api-vinos-xxxx.onrender.com`. ¡Cópiala en algún lado y no la pierdas! Ese es tu backend vivo en la nube.

### Fase 3: Conectar lo visual (Frontend) y publicarlo gratis con GitHub Pages

Ahora hay que decirle al HTML que hemos subido a internet que ya no debe buscar los datos en tu computadora, sino en el Render que acabamos de crear.

1. Vuelve a **GitHub.com** a la página de tu repositorio (`mi-dashboard-vinos`).
2. Haz clic en la carpeta `frontend`, luego en `public`, y por último abre el archivo `index.html`.
3. Verás el código. Para editarlo, haz clic en el icono con forma de **Lápiz** arriba a la derecha.
4. Ve al principio del código de JavaScript, y busca la línea que luce así (está casi al inicio de los códigos raros):
   `const API = "http://localhost:8000";`
5. Bórrala y pega allí la dirección que guardaste de Render (el paso 10 anterior). Quedará algo así:
   `const API = "https://api-vinos-xxxx.onrender.com";`
6. Ve al final de la página y haz clic en el botón verde **"Commit changes"** para guardar.
7. Ahora, mira las pestañas superiores en esa misma página de tu proyecto y haz clic en la que tiene icono de engranaje que dice **"Settings"** (Configuración).
8. En el menú del lado izquierdo, baja un poco y busca la opción **"Pages"**. Haz clic ahí.
9. En la sección "Build and deployment", donde dice "Branch" (que suele poner "None"), haz clic ahí y selecciona **"main"**. Al ladito, selecciona `/ (root)`. 
10. Dale al botón **"Save"**. 
11. Espera unos 2 o 3 minutos. Actualiza la página con F5. En la parte de arriba aparecerá un recuadro indicando que tu sitio está en línea (Your site is live) junto a un link.
12. Ese link probablemente te lleve solo a una página en blanco. ¿Por qué? Porque tus archivos están guardados dentro de las carpetas `frontend/public/`.
    Para ver la página, toma ese link general que te dio GitHub, y **ponle al final las carpetas**. 
    Debe verse exactamente así:
    `https://[tu-usuario].github.io/mi-dashboard-vinos/frontend/public/index.html`

¡Eso es todo! Si entras a este enlace en tu celular, tablet o computadora, o se lo mandas a tus socios, ya podrán ver el dashboard en vivo con los datos conectados a BigQuery.

---

## Endpoints de la API

| Endpoint | Descripción | Parámetros |
|---|---|---|
| `GET /api/kpis` | 7 KPIs con delta vs año anterior | `?anno=2025` |
| `GET /api/serie-mensual` | Serie mensual completa | `?anno=2025` |
| `GET /api/por-pais` | Venta por país | `?anno=2025` |
| `GET /api/por-tipo` | Venta por tipo de bebida | `?anno=2025` |
| `GET /api/por-marca` | Top 10 marcas | `?anno=2025` |
| `GET /api/top-clientes` | Clientes con RFM | `?anno=2025&limit=50` |
| `GET /api/top-productos` | Productos más vendidos | `?anno=2025&limit=50` |
| `GET /api/vendedores` | Performance vendedores | `?anno=2025` |
| `GET /api/margenes-marca` | Márgenes por marca | `?anno=2025` |
| `GET /api/segmentos-precio` | Análisis de segmentos | `?anno=2025` |
| `GET /api/estacionalidad` | Índice de estacionalidad | — |
| `GET /api/forecast-total` | Serie + pronóstico Mar-May 2026 | — |
| `GET /api/forecast-productos` | Pronóstico top 5 productos | — |

---

## Agregar Seguridad (antes de compartir)

Para que solo los socios accedan, agregar autenticación simple con token:

En `backend/main.py`:
```python
from fastapi import Header, HTTPException

API_TOKEN = "tu-token-secreto-aqui"  # cambiar por uno seguro

async def verify_token(x_api_key: str = Header(...)):
    if x_api_key != API_TOKEN:
        raise HTTPException(status_code=403, detail="Token inválido")
```

En `frontend/public/index.html`:
```javascript
const HEADERS = {"X-API-Key": "tu-token-secreto-aqui"};
// En apiFetch():
const r = await fetch(`${API}${path}${y}`, {headers: HEADERS});
```

---

## Actualizar datos de pronóstico mensualmente

```bash
# Ejecutar el script Python del proyecto (ya entregado)
python actualizar_forecast_bigquery.py

# Programar automáticamente (cron - Linux/Mac):
# Día 1 de cada mes a las 8am:
0 8 1 * * /ruta/venv/bin/python /ruta/actualizar_forecast_bigquery.py
```

---

## Tecnologías Utilizadas

| Capa | Tecnología | Por qué |
|---|---|---|
| Base de datos | BigQuery (Google Cloud) | Ya tienes los datos ahí |
| Backend API | FastAPI (Python) | Rápido, async, documentación automática |
| Gráficas | Chart.js 4.4 | Sin dependencias de build, funciona en cualquier hosting |
| Fuentes | Cormorant Garamond + Outfit | Tipografía premium para presentación a socios |
| Frontend | HTML/CSS/JS puro | Sin frameworks = fácil de editar y desplegar |
| Hosting sugerido | Render.com (backend) + GitHub Pages (frontend) | Costo cero para este volumen de datos |
