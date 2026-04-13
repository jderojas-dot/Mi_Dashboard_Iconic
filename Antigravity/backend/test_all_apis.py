import requests
import time

apis = [
    "/api/kpis?anno=2026",
    "/api/serie-mensual?anno=2026",
    "/api/top-clientes-year?anno=2026&limit=50",
    "/api/top-productos-year?anno=2026&limit=50",
    "/api/rentabilidad-origen?anno=2026",
    "/api/por-tipo?anno=2026",
    "/api/por-marca?anno=2026",
    "/api/frecuencia-compra?anno=2026",
    "/api/retencion",
    "/api/estrategias-ia?anno=2026",
    "/api/serie-mensual",
    "/api/market-share-tipo?anno=2026",
    "/api/dependencia-marca?anno=2026",
    "/api/rfm-stats?anno=2026",
    "/api/rfm-details?anno=2026",
    "/api/rfm-mix-details?anno=2026",
    "/api/rfm-cross-sell?anno=2026",
    "/api/analisis-experto-clientes",
    "/api/productos-mix-pais?anno=2026",
    "/api/productos-all?anno=2026",
    "/api/marcas-all?anno=2026",
    "/api/segmentos-precio-year?anno=2026"
]

base_url = "http://127.0.0.1:8000"

print("Starting API tests...")
for api in apis:
    url = f"{base_url}{api}"
    try:
        t0 = time.time()
        r = requests.get(url, timeout=10)
        t1 = time.time()
        if r.status_code == 200:
            print(f"OK [200 OK] {api} ({int((t1-t0)*1000)}ms)")
        else:
            print(f"FAIL [{r.status_code}] {api} - {r.text[:100]}")
    except Exception as e:
        print(f"ERR [Error] {api} - {str(e)}")
print("Done.")
