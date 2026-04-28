let statesLatest = [];
let countiesLatest = [];
let citiesLatest = [];
let statesGeo = null;

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "N/A";
  const n = Number(v) * 100;
  return `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
}

function valueClass(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "neutral";
  if (n > 0) return "pos";
  if (n < 0) return "neg";
  return "neutral";
}

async function loadJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}: ${res.status}`);
  return await res.json();
}

function getStateId() {
  const params = new URLSearchParams(window.location.search);
  return params.get("id") || "06";
}

function fillList(id, rows, dataset) {
  const el = document.getElementById(id);
  el.innerHTML = "";
  rows.forEach(r => {
    const li = document.createElement("li");
    li.innerHTML = `<span>${r.region_name}</span><span class="${valueClass(r.yoy_pct_display)}">${fmtPct(r.yoy_pct_display)}</span>`;
    li.style.cursor = "pointer";
    li.onclick = () => {
      window.location.href = `profile.html?dataset=${encodeURIComponent(dataset)}&id=${encodeURIComponent(r.region_id)}`;
    };
    el.appendChild(li);
  });
}

function renderStateSummary(stateId) {
  const row = statesLatest.find(r => r.region_id === stateId);
  if (!row) return;

  document.getElementById("stateName").textContent = row.region_name;
  document.getElementById("stateYoy").textContent = fmtPct(row.yoy_pct_display);
  document.getElementById("stateYoy").className = `summary-number ${valueClass(row.yoy_pct_display)}`;
  document.getElementById("stateTrend").textContent = row.trend_label || "--";
  document.getElementById("stateLatestDate").textContent = row.date ? String(row.date).slice(0, 7) : "--";
  document.getElementById("stateProfileLink").href = `profile.html?dataset=states&id=${encodeURIComponent(stateId)}`;

  const topCounties = countiesLatest
    .filter(r => r.region_id.slice(0, 2) === stateId)
    .sort((a, b) => (b.yoy_pct_display ?? -999) - (a.yoy_pct_display ?? -999))
    .slice(0, 10);

  const topCities = citiesLatest
    .filter(r => r.region_id.slice(0, 2) === stateId)
    .sort((a, b) => (b.yoy_pct_display ?? -999) - (a.yoy_pct_display ?? -999))
    .slice(0, 10);

  fillList("stateTopCounties", topCounties, "counties");
  fillList("stateTopCities", topCities, "cities");
}

function renderMap(selectedId) {
  const trace = {
    type: "choropleth",
    geojson: statesGeo,
    featureidkey: "properties.region_id",
    locations: statesLatest.map(r => r.region_id),
    z: statesLatest.map(r => Number(r.yoy_pct_display ?? 0)),
    text: statesLatest.map(r => `${r.region_name}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}<br>Trend: ${r.trend_label || "N/A"}`),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0.0, "#fb7185"],
      [0.35, "#f59e0b"],
      [0.5, "#cbd5e1"],
      [0.65, "#60a5fa"],
      [1.0, "#4ade80"]
    ],
    zmid: 0,
    marker: { line: { color: "rgba(255,255,255,0.38)", width: 0.9 } },
    colorbar: { title: "Light YoY", tickfont: { color: "#e5ecff" }, titlefont: { color: "#e5ecff" } }
  };

  const layout = {
    geo: {
      fitbounds: "locations",
      projection: { type: "albers usa" },
      bgcolor: "rgba(0,0,0,0)",
      showlakes: false,
      showland: true,
      landcolor: "rgba(255,255,255,0.02)"
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 0, r: 0, b: 0, l: 0 },
    font: { color: "#e5ecff" }
  };

  Plotly.newPlot("statesMap", [trace], layout, { responsive: true, displayModeBar: false });

  document.getElementById("statesMap").on("plotly_click", data => {
    const point = data.points?.[0];
    if (!point) return;
    const id = String(point.location).padStart(2, "0");
    history.replaceState(null, "", `states.html?id=${encodeURIComponent(id)}`);
    renderStateSummary(id);
  });
}

async function init() {
  const [states, counties, cities, geo] = await Promise.all([
    loadJson("data/states_latest.json"),
    loadJson("data/counties_latest.json"),
    loadJson("data/cities_latest.json"),
    loadJson("data/regions/us_states_all.geojson")
  ]);

  statesLatest = states.map(r => ({ ...r, region_id: String(r.region_id).padStart(2, "0") }));
  countiesLatest = counties.map(r => ({ ...r, region_id: String(r.region_id).padStart(5, "0") }));
  citiesLatest = cities.map(r => ({ ...r, region_id: String(r.region_id).padStart(7, "0") }));
  statesGeo = geo;

  const stateId = getStateId();
  renderMap(stateId);
  renderStateSummary(stateId);
}

init().catch(console.error);