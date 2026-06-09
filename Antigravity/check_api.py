import urllib.request, json, sys

def check(url, label):
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.loads(r.read())
        print(f"\n[OK] {label} -> {len(data)} registros")
        for row in data[:12]:
            mes   = row.get("mes", "?")
            venta = row.get("venta_neta", 0)
            ped   = row.get("pedidos", 0)
            print(f"     Mes {mes:>2} | Venta: {venta:>12,.0f} | Pedidos: {ped}")
    except Exception as e:
        print(f"\n[ERROR] {label}: {e}")

check("http://localhost:8000/api/serie-mensual?anno=2026", "Serie mensual 2026")
check("http://localhost:8000/api/kpis?anno=2026", "KPIs 2026")
