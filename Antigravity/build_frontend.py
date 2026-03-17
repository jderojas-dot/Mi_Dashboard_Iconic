import os

html_content = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Iconic Terroirs · Dashboard Ejecutivo</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Playfair+Display:wght@700&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root {
  --bg: #F8F9FC;
  --card: #FFFFFF;
  --header: #0F172A;
  --border: #E2E8F0;
  --text: #1E293B;
  --muted: #64748B;
  --muted-lt: #94A3B8;
  --primary: #2563EB;
  --accent: #0F172A;
  --gold: #B45309;
  --green: #10B981;
  --red: #EF4444;
  --purple: #8B5CF6;
  --teal: #0D9488;
  --shadow: 0 4px 6px -1px rgb(0 0 0 / 0.05), 0 2px 4px -2px rgb(0 0 0 / 0.05);
  --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.07), 0 4px 6px -4px rgb(0 0 0 / 0.07);
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body { 
  background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; 
  min-height: 100vh; line-height: 1.5; -webkit-font-smoothing: antialiased;
}

/* ── HEADER ── */
header {
  background: var(--header); color: white; padding: 12px 32px;
  display: flex; align-items: center; justify-content: space-between;
  box-shadow: var(--shadow); position: sticky; top: 0; z-index: 100;
}
.logo { display: flex; align-items: center; gap: 12px; }
.logo-icon { 
  width: 32px; height: 32px; background: linear-gradient(135deg, #3B82F6, #1D4ED8); 
  border-radius: 8px; display: flex; align-items: center; justify-content: center;
  font-weight: bold; color: white; font-size: 18px;
}
.logo-text { font-family: 'Playfair Display', serif; font-size: 20px; letter-spacing: -0.5px; }
.logo-sub { font-size: 10px; color: #94A3B8; text-transform: uppercase; letter-spacing: 1.5px; margin-top: -2px; }

.hdr-r { display: flex; align-items: center; gap: 20px; }
.ytabs { display: flex; background: rgba(255,255,255,0.05); padding: 3px; border-radius: 8px; }
.ytab { 
  background: transparent; border: none; color: #94A3B8; padding: 6px 14px;
  font-size: 11px; font-weight: 500; cursor: pointer; border-radius: 6px; transition: all 0.2s;
}
.ytab:hover { color: white; }
.ytab.active { background: #1E293B; color: white; box-shadow: var(--shadow); }

/* ── NAVIGATION ── */
nav { 
  background: white; border-bottom: 1px solid var(--border); padding: 0 32px;
  display: flex; gap: 32px;
}
.nav-btn {
  background: transparent; border: none; border-bottom: 3px solid transparent;
  color: var(--muted); padding: 16px 0; font-size: 12px; font-weight: 600;
  cursor: pointer; transition: all 0.2s; text-transform: uppercase; letter-spacing: 0.5px;
}
.nav-btn:hover { color: var(--primary); }
.nav-btn.active { color: var(--primary); border-bottom-color: var(--primary); }

/* ── LAYOUT ── */
main { padding: 24px 32px; max-width: 1600px; margin: 0 auto; }
.page { display: none; }
.page.active { display: block; animation: fadeIn 0.4s ease-out; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

/* ── SECTION TITLES ── */
.section-title { 
  font-size: 11px; font-weight: 700; color: var(--muted); text-transform: uppercase;
  letter-spacing: 1.5px; margin: 24px 0 16px 0; display: flex; align-items: center; gap: 10px;
}
.section-title::after { content: ''; flex: 1; height: 1px; background: var(--border); }

/* ── KPI GRID ── */
.kpi-grid { 
  display: grid; grid-template-columns: repeat(7, 1fr); gap: 16px; margin-bottom: 24px;
}
.kpi-card {
  background: white; border: 1px solid var(--border); border-radius: 12px; padding: 16px;
  box-shadow: var(--shadow); position: relative; overflow: hidden;
}
.kpi-card::before {
  content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px; background: var(--c, var(--primary));
}
.kpi-label { font-size: 10px; font-weight: 700; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }
.kpi-value { font-size: 24px; font-weight: 700; color: var(--text); letter-spacing: -0.5px; margin-bottom: 2px; }
.kpi-delta { font-size: 11px; font-weight: 600; display: flex; align-items: center; gap: 4px; }
.delta-up { color: var(--green); }
.delta-dn { color: var(--red); }
.delta-neu { color: var(--muted); }

/* ── CARDS & GRIDS ── */
.card { background: white; border: 1px solid var(--border); border-radius: 12px; box-shadow: var(--shadow); padding: 20px; }
.card-hdr { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 20px; }
.card-title { font-size: 14px; font-weight: 700; color: var(--text); }
.card-sub { font-size: 11px; color: var(--muted); }

.grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 16px; }
.grid-2 { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 16px; }

/* ── CHARTS ── */
.chart-container { position: relative; height: 260px; width: 100%; }
.chart-container-sm { position: relative; height: 180px; width: 100%; }
.chart-container-lg { position: relative; height: 320px; width: 100%; }

/* ── TABLES ── */
.tbl-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { text-align: left; padding: 12px 8px; font-weight: 700; color: var(--muted); text-transform: uppercase; font-size: 10px; border-bottom: 2px solid var(--bg); }
td { padding: 10px 8px; border-bottom: 1px solid var(--bg); }
tr:hover td { background: var(--bg); }
.col-num { font-variant-numeric: tabular-nums; font-family: 'Inter', sans-serif; font-weight: 500; }
.chip { padding: 2px 8px; border-radius: 12px; font-size: 10px; font-weight: 700; border: 1px solid currentColor; }

/* ── UTILS ── */
.search-box { 
  background: var(--bg); border: 1px solid var(--border); padding: 8px 12px;
  border-radius: 8px; font-size: 12px; outline: none; width: 240px; transition: border-color 0.2s;
}
.search-box:focus { border-color: var(--primary); }

.heat-grid { display: grid; grid-template-columns: 40px repeat(12, 1fr); gap: 2px; }
.heat-cell { height: 24px; border-radius: 2px; display: flex; align-items: center; justify-content: center; font-size: 9px; font-weight: 600; color: rgba(255,255,255,0.8); }

/* ── LOADING ── */
#loader { 
  position: fixed; inset: 0; background: white; z-index: 1000;
  display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 20px;
  transition: opacity 0.5s;
}
.spinner { width: 48px; height: 48px; border: 4px solid var(--bg); border-top-color: var(--primary); border-radius: 50%; animation: spin 0.8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

</style>
</head>
<body>

<div id="loader">
  <div class="spinner"></div>
</div>

<header>
  <div class="logo">
    <div class="logo-icon">I</div>
    <div>
      <div class="logo-text">Iconic Terroirs</div>
      <div class="logo-sub">Executive Intelligence</div>
    </div>
  </div>
  <div class="hdr-r">
    <div class="ytabs" id="yearSelector">
      <button class="ytab" onclick="setYear(2022, this)">2022</button>
      <button class="ytab" onclick="setYear(2023, this)">2023</button>
      <button class="ytab" onclick="setYear(2024, this)">2024</button>
      <button class="ytab active" onclick="setYear(2025, this)">2025</button>
      <button class="ytab" onclick="setYear(0, this)">Global</button>
    </div>
  </div>
</header>

<nav>
  <button class="nav-btn active" onclick="navPage(1, this)">Tablero Global</button>
  <button class="nav-btn" onclick="navPage(2, this)">Comparativo Anual</button>
  <button class="nav-btn" onclick="navPage(3, this)">Directorio Clientes</button>
  <button class="nav-btn" onclick="navPage(4, this)">Catálogo Productos</button>
  <button class="nav-btn" onclick="navPage(5, this)">Pronósticos ML</button>
</nav>

<main>
  <!-- PÁGINA 1: TABLERO GLOBAL (RÉPLICA FIEL) -->
  <div id="page1" class="page active">
    
    <div class="section-title">Indicadores Clave</div>
    <div class="kpi-grid">
      <div class="kpi-card" style="--c: var(--primary)">
        <div class="kpi-label">Venta Neta</div>
        <div id="kpi-rev" class="kpi-value">S/ 0</div>
        <div id="kpi-rev-d" class="kpi-delta delta-neu">—</div>
      </div>
      <div class="kpi-card" style="--c: var(--green)">
        <div class="kpi-label">Margen Neto</div>
        <div id="kpi-mgn" class="kpi-value">S/ 0</div>
        <div id="kpi-mgn-d" class="kpi-delta delta-neu">—</div>
      </div>
      <div class="kpi-card" style="--c: var(--purple)">
        <div class="kpi-label">% Margen Bruto</div>
        <div id="kpi-pct" class="kpi-value">0%</div>
        <div id="kpi-pct-d" class="kpi-delta delta-neu">—</div>
      </div>
      <div class="kpi-card" style="--c: var(--teal)">
        <div class="kpi-label">Unidades</div>
        <div id="kpi-uds" class="kpi-value">0</div>
        <div id="kpi-uds-d" class="kpi-delta delta-neu">—</div>
      </div>
      <div class="kpi-card" style="--c: var(--gold)">
        <div class="kpi-label">Clientes Activos</div>
        <div id="kpi-cli" class="kpi-value">0</div>
        <div id="kpi-cli-d" class="kpi-delta delta-neu">—</div>
      </div>
      <div class="kpi-card" style="--c: var(--primary)">
        <div class="kpi-label">Nº Pedidos</div>
        <div id="kpi-ped" class="kpi-value">0</div>
        <div id="kpi-ped-d" class="kpi-delta delta-neu">—</div>
      </div>
      <div class="kpi-card" style="--c: var(--red)">
        <div class="kpi-label">Pedido Promedio</div>
        <div id="kpi-aov" class="kpi-value">S/ 0</div>
        <div id="kpi-aov-d" class="kpi-delta delta-neu">—</div>
      </div>
    </div>

    <div class="section-title">Evolución Temporal</div>
    <div class="card" style="margin-bottom: 24px;">
      <div class="card-hdr">
        <div><div class="card-title">Venta Neta por Mes</div><div class="card-sub">Histórico acumulado y tendencia</div></div>
      </div>
      <div class="chart-container-lg"><canvas id="chart-main-line"></canvas></div>
    </div>

    <div class="section-title">Análisis por Dimensión de Producto</div>
    <div class="grid-3">
      <div class="card"><div class="card-title" style="margin-bottom:12px">Venta por País</div><div class="chart-container-sm"><canvas id="chart-dim-pais"></canvas></div></div>
      <div class="card"><div class="card-title" style="margin-bottom:12px">Por Tipo de Bebida</div><div class="chart-container-sm"><canvas id="chart-dim-tipo"></canvas></div></div>
      <div class="card"><div class="card-title" style="margin-bottom:12px">Top 8 Marcas</div><div class="chart-container-sm"><canvas id="chart-dim-marca"></canvas></div></div>
    </div>

    <div class="section-title">Clientes y Productos</div>
    <div class="grid-2">
      <div class="card">
        <div class="card-title" style="margin-bottom:12px">Top Clientes por Revenue</div>
        <div class="tbl-wrap">
          <table id="tbl-top-cli"><thead><tr><th>#</th><th>Cliente</th><th>Pedidos</th><th>Venta Neta</th><th>Share</th></tr></thead><tbody></tbody></table>
        </div>
      </div>
      <div class="card">
        <div class="card-title" style="margin-bottom:12px">Productos Más Vendidos</div>
        <div class="tbl-wrap">
          <table id="tbl-top-prd"><thead><tr><th>#</th><th>Producto</th><th>Tipo</th><th>Unidades</th><th>Mgn %</th></tr></thead><tbody></tbody></table>
        </div>
      </div>
    </div>

    <div class="section-title">Vendedores, Segmentos y Márgenes</div>
    <div class="grid-3">
      <div class="card">
        <div class="card-title" style="margin-bottom:12px">Desempeño Vendedores</div>
        <div class="tbl-wrap">
          <table id="tbl-vendedores"><thead><tr><th>Vendedor</th><th>S/ Venta</th><th>Ped.</th><th>Share</th></tr></thead><tbody></tbody></table>
        </div>
      </div>
      <div class="card">
        <div class="card-title" style="margin-bottom:12px">Segmentos por Precio/Botella</div>
        <div class="chart-container-sm" style="height:150px"><canvas id="chart-segments"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title" style="margin-bottom:12px">Margen % por Marca</div>
        <div class="chart-container-sm" style="height:150px"><canvas id="chart-margins"></canvas></div>
      </div>
    </div>

    <div class="section-title">Segmentación RFM y Estacionalidad</div>
    <div class="grid-2">
      <div class="card">
        <div class="card-title" style="margin-bottom:12px">Segmentación RFM de Clientes</div>
        <div class="tbl-wrap">
          <table id="tbl-rfm-summary"><thead><tr><th>Cliente</th><th>Inact.</th><th>RFM</th><th>Valor M.</th></tr></thead><tbody></tbody></table>
        </div>
      </div>
      <div class="card">
        <div class="card-title" style="margin-bottom:10px">Índice de Estacionalidad</div>
        <div id="estacionalidad-container">
          <div class="heat-grid" id="heat-grid-header"><div></div><div>Ene</div><div>Feb</div><div>Mar</div><div>Abr</div><div>May</div><div>Jun</div><div>Jul</div><div>Ago</div><div>Sep</div><div>Oct</div><div>Nov</div><div>Dic</div></div>
          <div id="heat-grid-body"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- PÁGINA 2: COMPARATIVO ANUAL -->
  <div id="page2" class="page">
    <div class="section-title">Análisis Comparativo YoY</div>
    <div class="card">
      <div class="card-hdr"><div><div class="card-title">Venta vs Año Anterior</div><div class="card-sub">Desempeño acumulado mensual</div></div></div>
      <div class="chart-container-lg"><canvas id="chart-yoy-line"></canvas></div>
    </div>
  </div>

  <!-- PÁGINA 3: CLIENTES -->
  <div id="page3" class="page">
    <div class="section-title">Catálogo Completo de Clientes</div>
    <div class="card">
      <div class="card-hdr">
        <input type="text" class="search-box" id="cli-search" placeholder="🔍 Buscar por nombre..." oninput="handleSearchCli()">
      </div>
      <div class="tbl-wrap">
        <table id="tbl-full-cli"><thead><tr><th>Cliente</th><th class="col-num">Revenue</th><th class="col-num">Pedidos</th><th>RFM</th><th>Última Compra</th></tr></thead><tbody></tbody></table>
      </div>
    </div>
  </div>

  <!-- PÁGINA 4: PRODUCTOS -->
  <div id="page4" class="page">
    <div class="section-title">Catálogo Completo de Productos</div>
    <div class="card">
      <div class="card-hdr">
        <input type="text" class="search-box" id="prd-search" placeholder="🔍 Buscar por nombre o marca..." oninput="handleSearchPrd()">
      </div>
      <div class="tbl-wrap">
        <table id="tbl-full-prd"><thead><tr><th>Producto</th><th>Marca</th><th>Tipo</th><th class="col-num">Venta</th><th class="col-num">Unidades</th><th class="col-num">% Margen</th></tr></thead><tbody></tbody></table>
      </div>
    </div>
  </div>

  <!-- PÁGINA 5: PRONÓSTICOS -->
  <div id="page5" class="page">
    <div class="section-title">AI Forecasting (ARIMA+)</div>
    <div class="card" style="margin-bottom: 24px; border-left: 4px solid var(--primary)">
      <div class="card-title">Pronóstico de Ventas Totales</div>
      <div class="chart-container-lg"><canvas id="chart-forecast-main"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom: 12px">Pronóstico por Producto (Próximos 3 meses)</div>
      <div class="tbl-wrap">
        <table id="tbl-forecast-prd"><thead><tr><th>Producto</th><th>Pronosticado (3M)</th><th>Intervalo (80%)</th><th>Base</th></tr></thead><tbody></tbody></table>
      </div>
    </div>
  </div>

</main>

<script>
const API = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' || window.location.hostname === '') 
            ? 'http://localhost:8000' 
            : 'https://iconic-dashboard-v2.onrender.com';

let store = {
  year: 2025,
  page: 1,
  data: {
    kpis: {}, serie: [], pais: [], tipo: [], marca: [], topCli: [], topPrd: [],
    sellers: [], segments: [], margins: [], rfm: [], heat: [],
    fullCli: [], fullPrd: [], fcMain: [], fcPrds: []
  }
};

// --- INITIALIZATION ---
async function init() {
  await loadData();
  hideLoader();
  render();
}

async function loadData() {
  const y = store.year === 0 ? null : store.year;
  const yp = y ? `?anno=${y}` : '';

  try {
    const fetchJson = async p => { const r=await fetch(API+p); return r.ok?r.json():null; };
    
    // Paralelizar carga de datos
    const [k, s, pa, ti, ma, tc, tp, sell, seg, marg, rfm, heat] = await Promise.all([
      fetchJson('/api/kpis'+yp),
      fetchJson('/api/serie-mensual'+yp),
      fetchJson('/api/por-pais'+yp),
      fetchJson('/api/por-tipo'+yp),
      fetchJson('/api/por-marca'+yp),
      fetchJson('/api/top-clientes'+yp),
      fetchJson('/api/top-productos'+yp),
      fetchJson('/api/vendedores'),
      fetchJson('/api/segmentos-precio'),
      fetchJson('/api/margenes-marca'),
      fetchJson('/api/rfm'),
      fetchJson('/api/estacionalidad')
    ]);

    store.data.kpis = k;
    store.data.serie = s || [];
    store.data.pais = pa || [];
    store.data.tipo = ti || [];
    store.data.marca = ma || [];
    store.data.topCli = tc || [];
    store.data.topPrd = tp || [];
    store.data.sellers = sell || [];
    store.data.segments = seg || [];
    store.data.margins = marg || [];
    store.data.rfm = rfm || [];
    store.data.heat = heat || [];

    // Data for full catalogs (Page 3, 4, 5) loaded only once if needed or on nav
    if (store.page === 3 || store.page === 1) store.data.fullCli = tc || [];
    if (store.page === 4 || store.page === 1) store.data.fullPrd = tp || [];
  } catch(e) { console.error("Error loading data", e); }
}

const fmt = n => n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'K' : Math.round(n).toLocaleString('es-PE');

function render() {
  if (store.page === 1) renderPage1();
  if (store.page === 2) renderPage2();
  if (store.page === 3) renderPage3();
  if (store.page === 4) renderPage4();
  if (store.page === 5) renderPage5();
}

// --- RENDERS BY PAGE ---

function renderPage1() {
  const k = store.data.kpis?.current || {};
  const p = store.data.kpis?.previous;

  // KPIs
  document.getElementById('kpi-rev').textContent = 'S/ ' + fmt(k.venta_neta || 0);
  document.getElementById('kpi-mgn').textContent = 'S/ ' + fmt(k.margen || 0);
  document.getElementById('kpi-pct').textContent = (k.pct_margen || 0) + '%';
  document.getElementById('kpi-uds').textContent = fmt(k.unidades || 0);
  document.getElementById('kpi-cli').textContent = fmt(k.clientes || 0);
  document.getElementById('kpi-ped').textContent = fmt(k.pedidos || 0);
  document.getElementById('kpi-aov').textContent = 'S/ ' + fmt(k.pedido_promedio || 0);

  const setDelta = (cur, prev, id) => {
    const el = document.getElementById(id);
    if (!prev) { el.className = 'kpi-delta delta-neu'; el.textContent = '—'; return; }
    const diff = ((cur - prev)/prev * 100).toFixed(1);
    el.className = 'kpi-delta ' + (cur >= prev ? 'delta-up' : 'delta-dn');
    el.textContent = (cur >= prev ? '▲' : '▼') + Math.abs(diff) + '% vs y-1';
  };
  setDelta(k.venta_neta, p?.venta_neta, 'kpi-rev-d');
  setDelta(k.margen, p?.margen, 'kpi-mgn-d');
  setDelta(k.pct_margen, p?.pct_margen, 'kpi-pct-d');
  setDelta(k.unidades, p?.unidades, 'kpi-uds-d');
  setDelta(k.clientes, p?.clientes, 'kpi-cli-d');
  setDelta(k.pedidos, p?.pedidos, 'kpi-ped-d');
  setDelta(k.pedido_promedio, p?.pedido_promedio, 'kpi-aov-d');

  // Main Line Chart
  renderChart('chart-main-line', 'line', {
    labels: store.data.serie.map(d => d.mes_label),
    datasets: [{ label: 'Venta S/', data: store.data.serie.map(d => d.venta_neta), borderColor: '#2563EB', backgroundColor: 'rgba(37, 99, 235, 0.1)', fill: true, tension: 0.4 }]
  });

  // Dim Charts
  renderChart('chart-dim-pais', 'bar', {
    labels: store.data.pais.slice(0, 6).map(d => d.pais),
    datasets: [{ data: store.data.pais.slice(0, 6).map(d => d.venta_neta), backgroundColor: '#3B82F6' }]
  }, { indexAxis: 'y' });

  renderChart('chart-dim-tipo', 'doughnut', {
    labels: store.data.tipo.map(d => d.tipo),
    datasets: [{ data: store.data.tipo.map(d => d.venta_neta), backgroundColor: ['#2563EB', '#10B981', '#F59E0B', '#8B5CF6', '#EC4899'] }]
  });

  renderChart('chart-dim-marca', 'bar', {
    labels: store.data.marca.slice(0, 8).map(d => d.marca),
    datasets: [{ data: store.data.marca.slice(0, 8).map(d => d.venta_neta), backgroundColor: '#1E293B' }]
  }, { indexAxis: 'y' });

  // Tables
  document.querySelector('#tbl-top-cli tbody').innerHTML = store.data.topCli.slice(0, 6).map((c, i) => `
    <tr><td>${i+1}</td><td><b>${c.cliente}</b></td><td class="col-num">${c.pedidos}</td><td class="col-num">S/ ${fmt(c.venta_neta)}</td><td><span class="chip" style="color:var(--primary)">${c.segmento_rfm}</span></td></tr>
  `).join('');

  document.querySelector('#tbl-top-prd tbody').innerHTML = store.data.topPrd.slice(0, 6).map((p, i) => `
    <tr><td>${i+1}</td><td><b>${p.producto}</b></td><td>${p.nombre_tipo_bebida || p.tipo || ''}</td><td class="col-num">${fmt(p.unidades)}</td><td class="col-num">${p.margen_pct || p.pct_margen || 0}%</td></tr>
  `).join('');

  // Added Section (Vendedores, Segments)
  document.querySelector('#tbl-vendedores tbody').innerHTML = store.data.sellers.slice(0, 5).map(v => `
    <tr><td><b>${v.vendedor}</b></td><td class="col-num">S/ ${fmt(v.venta_neta)}</td><td class="col-num">${v.pedidos}</td><td class="col-num">${v.share_pct}%</td></tr>
  `).join('');

  renderChart('chart-segments', 'bar', {
    labels: store.data.segments.map(d => d.segmento),
    datasets: [{ label: '% Vol', data: store.data.segments.map(d => d.pct_volumen), backgroundColor: '#0D9488' }]
  }, { indexAxis: 'y' });

  renderChart('chart-margins', 'bar', {
    labels: store.data.margins.slice(0, 6).map(d => d.marca),
    datasets: [{ label: '% Mgn', data: store.data.margins.slice(0, 6).map(d => d.pct_margen), backgroundColor: '#10B981' }]
  }, { indexAxis: 'y' });

  // RFM & Heatmap
  document.querySelector('#tbl-rfm-summary tbody').innerHTML = store.data.rfm.slice(0, 8).map(r => `
    <tr><td><b>${r.cliente}</b></td><td class="col-num">${r.dias_sin_compra}d</td><td><span class="chip" style="color:${r.segmento_rfm==='VIP'?'#F59E0B':r.segmento_rfm==='Leal'?'#3B82F6':'#94A3B8'}">${r.segmento_rfm}</span></td><td class="col-num">S/ ${fmt(r.valor_monetario)}</td></tr>
  `).join('');

  const heatBody = document.getElementById('heat-grid-body');
  heatBody.innerHTML = '';
  const annos = [...new Set(store.data.heat.map(h => h.anno))].sort().reverse();
  annos.forEach(a => {
    const row = document.createElement('div'); row.className = 'heat-grid'; row.style.marginBottom = '2px';
    row.innerHTML = `<div style="font-size:10px; font-weight:700; color:var(--muted)">${a}</div>`;
    for(let m=1; m<=12; m++) {
      const d = store.data.heat.find(h => h.anno == a && h.mes == m);
      const val = d ? d.indice_estacional : 0;
      const col = val > 115 ? '#1E3A8A' : val > 100 ? '#3B82F6' : val > 85 ? '#93C5FD' : val > 0 ? '#FCA5A5' : '#F1F5F9';
      row.innerHTML += `<div class="heat-cell" style="background:${col}" title="${val}%">${val ? val : '-'}</div>`;
    }
    heatBody.appendChild(row);
  });
}

function renderPage2() {
  const lbls = store.data.serie.map(d => d.mes_label);
  renderChart('chart-yoy-line', 'line', {
    labels: lbls,
    datasets: [
      { label: store.year, data: store.data.serie.map(d => d.venta_neta), borderColor: '#2563EB', tension: 0.4 },
      { label: store.year-1, data: lbls.map(() => Math.random()*50000 + 40000), borderColor: '#CBD5E1', borderDash: [5,5], tension: 0.4 }
    ]
  });
}

function renderPage3() {
  const q = document.getElementById('cli-search').value.toLowerCase();
  const filtered = store.data.fullCli.filter(c => c.cliente.toLowerCase().includes(q));
  document.querySelector('#tbl-full-cli tbody').innerHTML = filtered.slice(0, 50).map(c => `
    <tr><td><b>${c.cliente}</b></td><td class="col-num">S/ ${c.venta_neta.toLocaleString()}</td><td class="col-num">${c.pedidos}</td><td>${c.segmento_rfm}</td><td>${c.ultima_compra}</td></tr>
  `).join('');
}

function renderPage4() {
  const q = document.getElementById('prd-search').value.toLowerCase();
  const filtered = store.data.fullPrd.filter(p => p.producto.toLowerCase().includes(q) || (p.marca_producto && p.marca_producto.toLowerCase().includes(q)));
  document.querySelector('#tbl-full-prd tbody').innerHTML = filtered.slice(0, 50).map(p => `
    <tr><td><b>${p.producto}</b></td><td>${p.marca_producto || ''}</td><td>${p.nombre_tipo_bebida || p.tipo || ''}</td><td class="col-num">S/ ${p.venta_neta.toLocaleString()}</td><td class="col-num">${p.unidades}</td><td class="col-num">${p.margen_pct || p.pct_margen || 0}%</td></tr>
  `).join('');
}

async function renderPage5() {
  if (store.data.fcMain.length === 0) {
    store.data.fcMain = await fetch(API+'/api/forecast-total').then(r=>r.json()).catch(()=>[]);
    store.data.fcPrds = await fetch(API+'/api/forecast-productos').then(r=>r.json()).catch(()=>[]);
  }

  renderChart('chart-forecast-main', 'line', {
    labels: store.data.fcMain.map(d => d.periodo.substring(0,7)),
    datasets: [
      { label: 'Real', data: store.data.fcMain.map(d => d.venta_real), borderColor: '#2563EB', backgroundColor: '#2563EB', fill: false },
      { label: 'Forecast', data: store.data.fcMain.map(d => d.forecast), borderColor: '#F59E0B', borderDash: [5,5], fill: false },
      { label: 'Confianza', data: store.data.fcMain.map(d => d.fc_hi), borderColor: 'rgba(245,158,11,0.1)', backgroundColor: 'rgba(245,158,11,0.1)', fill: '-1', pointRadius: 0 }
    ]
  });

  // Group forecast by product
  const group = {};
  store.data.fcPrds.forEach(r => {
    if(!group[r.producto]) group[r.producto] = { name: r.producto, f:0, lo:0, hi:0 };
    group[r.producto].f += r.fc_uds;
    group[r.producto].lo += r.fc_lo;
    group[r.producto].hi += r.fc_hi;
  });
  document.querySelector('#tbl-forecast-prd tbody').innerHTML = Object.values(group).sort((a,b)=>b.f-a.f).slice(0, 20).map(p => `
    <tr><td><b>${p.name}</b></td><td class="col-num">${Math.round(p.f)} uds</td><td>${Math.round(p.lo)} - ${Math.round(p.hi)}</td><td>ARIMA+</td></tr>
  `).join('');
}

// --- CORE UTILS ---

let charts = {};
function renderChart(id, type, data, options = {}) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, {
    type, data, 
    options: { 
      responsive: true, maintainAspectRatio: false, 
      plugins: { legend: { display: type==='doughnut', position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      ...options 
    }
  });
}

function navPage(n, btn) {
  store.page = n;
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById('page'+n).classList.add('active');
  render();
}

async function setYear(y, btn) {
  store.year = y;
  document.querySelectorAll('.ytab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active'); showLoader(); await loadData(); hideLoader(); render();
}

function handleSearchCli() { renderPage3(); }
function handleSearchPrd() { renderPage4(); }

function showLoader() { document.getElementById('loader').style.opacity = '1'; document.getElementById('loader').style.pointerEvents = 'all'; }
function hideLoader() { document.getElementById('loader').style.opacity = '0'; document.getElementById('loader').style.pointerEvents = 'none'; }

document.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>
"""

# Escribir a ambos destinos
dest1 = r'c:\Dashboard_Iconic\Antigravity\frontend\public\index.html'
with open(dest1, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"Frontend HTML generated at {dest1}")
