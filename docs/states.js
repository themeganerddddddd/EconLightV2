let statesLatest = [];
let countiesLatest = [];
let citiesLatest = [];
let statesGeo = null;
let countiesGeo = null;

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

function featureId(feature) {
  return String(feature?.properties?.region_id ?? feature?.properties?.GEOID ?? feature?.id ?? "");
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

function renderStateMap() {
  const stateMapById = new Map(statesLatest.map(r => [r.region_id, r]));

  const allStateIds = statesGeo.features
    .map(f => featureId(f).padStart(2, "0"))
    .filter(id => id.length === 2);

  const rows = allStateIds.map(id => {
    const row = stateMapById.get(id);
    return {
      region_id: id,
      region_name: row?.region_name || statesGeo.features.find(f => featureId(f).padStart(2, "0") === id)?.properties?.region_name || id,
      yoy_pct_display: row?.yoy_pct_display ?? null,
      trend_label: row?.trend_label || "No data"
    };
  });

  const trace = {
    type: "choropleth",
    geojson: statesGeo,
    featureidkey: "properties.region_id",
    locations: rows.map(r => r.region_id),
    z: rows.map(r => r.yoy_pct_display == null ? null : Number(r.yoy_pct_display)),
    text: rows.map(r => `${r.region_name}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}<br>Trend: ${r.trend_label || "N/A"}`),
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
      scope: "usa",
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

function renderStateCountyMap(stateId) {
  const mapEl = document.getElementById("stateCountyMap");
  if (!mapEl || !countiesGeo?.features) return;

  const stateFeatures = countiesGeo.features.filter(f => featureId(f).padStart(5, "0").slice(0, 2) === stateId);
  const dataRowsById = new Map(countiesLatest.map(r => [r.region_id, r]));

  const rows = stateFeatures.map(f => {
    const id = featureId(f).padStart(5, "0");
    const row = dataRowsById.get(id);
    return {
      region_id: id,
      region_name: row?.region_name || f.properties?.region_name || id,
      yoy_pct_display: row?.yoy_pct_display ?? null,
      trend_label: row?.trend_label || "No data"
    };
  });

  const stateCountyGeo = { type: "FeatureCollection", features: stateFeatures };

  if (!stateFeatures.length) {
    mapEl.innerHTML = `<div class="mini-map-fallback">No county geometry found for this state.</div>`;
    return;
  }

  const trace = {
    type: "choropleth",
    geojson: stateCountyGeo,
    featureidkey: "properties.region_id",
    locations: rows.map(r => r.region_id),
    z: rows.map(r => r.yoy_pct_display == null ? null : Number(r.yoy_pct_display)),
    text: rows.map(r => `${r.region_name}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}<br>Trend: ${r.trend_label}`),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0.0, "#fb7185"],
      [0.35, "#f59e0b"],
      [0.5, "#cbd5e1"],
      [0.65, "#60a5fa"],
      [1.0, "#4ade80"]
    ],
    zmid: 0,
    marker: { line: { color: "rgba(255,255,255,0.18)", width: 0.5 } },
    colorbar: { title: "Light YoY", tickfont: { color: "#e5ecff" }, titlefont: { color: "#e5ecff" } }
  };

  const layout = {
    geo: {
      fitbounds: "locations",
      bgcolor: "rgba(0,0,0,0)",
      showland: true,
      landcolor: "rgba(255,255,255,0.02)"
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 0, r: 0, b: 0, l: 0 },
    font: { color: "#e5ecff" }
  };

  Plotly.newPlot("stateCountyMap", [trace], layout, { responsive: true, displayModeBar: false });

  mapEl.on("plotly_click", data => {
    const point = data.points?.[0];
    if (!point) return;
    const id = String(point.location).padStart(5, "0");
    window.location.href = `profile.html?dataset=counties&id=${encodeURIComponent(id)}`;
  });
}

function renderStateSummary(stateId) {
  const row = statesLatest.find(r => r.region_id === stateId);
  const stateFeature = statesGeo.features.find(f => featureId(f).padStart(2, "0") === stateId);

  document.getElementById("stateName").textContent = row?.region_name || stateFeature?.properties?.region_name || stateId;
  document.getElementById("stateYoy").textContent = fmtPct(row?.yoy_pct_display);
  document.getElementById("stateYoy").className = `summary-number ${valueClass(row?.yoy_pct_display)}`;
  document.getElementById("stateTrend").textContent = row?.trend_label || "No data";
  document.getElementById("stateLatestDate").textContent = row?.date ? String(row.date).slice(0, 7) : "--";
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
  renderStateCountyMap(stateId);
}

async function init() {
  const [states, counties, cities, stateGeo, countyGeo] = await Promise.all([
    loadJson("data/states_latest.json"),
    loadJson("data/counties_latest.json"),
    loadJson("data/cities_latest.json"),
    loadJson("data/regions/us_states_all.geojson"),
    loadJson("data/regions/us_counties_all.geojson")
  ]);

  statesLatest = states.map(r => ({ ...r, region_id: String(r.region_id).padStart(2, "0") }));
  countiesLatest = counties.map(r => ({ ...r, region_id: String(r.region_id).padStart(5, "0") }));
  citiesLatest = cities.map(r => ({ ...r, region_id: String(r.region_id).padStart(7, "0") }));
  statesGeo = stateGeo;
  countiesGeo = countyGeo;

  const stateId = getStateId();
  renderStateMap();
  renderStateSummary(stateId);
}

init().catch(console.error);