let homepageSummary = {};
let profileIndex = [];
let stateLatest = [];
let countyLatest = [];
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

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function setClass(id, className) {
  const el = document.getElementById(id);
  if (el) el.className = className;
}

async function safeLoadJson(path, fallback = null) {
  try {
    const res = await fetch(path);
    if (!res.ok) return fallback;
    return await res.json();
  } catch {
    return fallback;
  }
}

function showStatus(msg) {
  const mount = document.getElementById("statusMount");
  if (!mount) return;
  mount.innerHTML = `<div class="status-card"><strong>Status:</strong> ${msg}</div>`;
}

function renderMiniList(id, rows) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = "";

  (rows || []).slice(0, 5).forEach(row => {
    const yoy = row.yoy_pct_display ?? row.yoy_pct;
    const li = document.createElement("li");
    li.innerHTML = `<span>${row.region_name}</span><span class="${valueClass(yoy)}">${fmtPct(yoy)}</span>`;
    li.style.cursor = "pointer";
    li.onclick = () => {
      const match = profileIndex.find(r => r.region_name === row.region_name);
      if (match) {
        window.location.href = `profile.html?dataset=${encodeURIComponent(match.dataset)}&id=${encodeURIComponent(match.region_id)}`;
      }
    };
    el.appendChild(li);
  });
}

function populateGlobalSearch(results) {
  const sel = document.getElementById("globalSearchResults");
  if (!sel) return;
  sel.innerHTML = "";

  results.forEach(r => {
    const opt = document.createElement("option");
    opt.value = `${r.dataset}|${r.region_id}`;
    opt.textContent = `${r.region_name} (${r.dataset})`;
    sel.appendChild(opt);
  });

  sel.onchange = () => {
    const [dataset, id] = sel.value.split("|");
    if (dataset === "states") {
      window.location.href = `state.html?id=${encodeURIComponent(String(id).padStart(2, "0"))}`;
    } else {
      window.location.href = `profile.html?dataset=${encodeURIComponent(dataset)}&id=${encodeURIComponent(id)}`;
    }
  };
}

function attachGlobalSearch() {
  const input = document.getElementById("globalSearch");
  const sel = document.getElementById("globalSearchResults");
  if (!input || !sel) return;

  if (!profileIndex.length) {
    sel.innerHTML = `<option>Search index unavailable</option>`;
    return;
  }

  input.oninput = () => {
    const q = input.value.trim().toLowerCase();
    const matches = profileIndex.filter(r => r.region_name.toLowerCase().includes(q)).slice(0, 100);
    populateGlobalSearch(matches);
  };

  populateGlobalSearch(profileIndex.slice(0, 50));
}

function renderStateMap() {
  if (!statesGeo?.features) return;

  const rows = stateLatest.map(r => ({
    ...r,
    region_id: String(r.region_id).padStart(2, "0")
  }));

  const trace = {
    type: "choropleth",
    geojson: statesGeo,
    featureidkey: "properties.region_id",
    locations: rows.map(r => r.region_id),
    z: rows.map(r => Number(r.yoy_pct_display ?? 0)),
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

  Plotly.newPlot("countyMap", [trace], layout, { responsive: true, displayModeBar: false });

  document.getElementById("countyMap").on("plotly_click", data => {
    const point = data.points?.[0];
    if (!point) return;
    const id = String(point.location).padStart(2, "0");
    window.location.href = `state.html?id=${encodeURIComponent(id)}`;
  });
}

function renderCountyMap() {
  if (!countiesGeo?.features) return;

  const rows = countyLatest.map(r => ({
    ...r,
    region_id: String(r.region_id).padStart(5, "0")
  }));

  const trace = {
    type: "choropleth",
    geojson: countiesGeo,
    featureidkey: "properties.region_id",
    locations: rows.map(r => r.region_id),
    z: rows.map(r => Number(r.yoy_pct_display ?? 0)),
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
    marker: { line: { color: "rgba(255,255,255,0.08)", width: 0.18 } },
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

  Plotly.newPlot("countyMap", [trace], layout, { responsive: true, displayModeBar: false });

  document.getElementById("countyMap").on("plotly_click", data => {
    const point = data.points?.[0];
    if (!point) return;
    const id = String(point.location).padStart(5, "0");
    window.location.href = `profile.html?dataset=counties&id=${encodeURIComponent(id)}`;
  });
}

function setMapMode(mode) {
  const stateBtn = document.getElementById("stateMapBtn");
  const countyBtn = document.getElementById("countyMapBtn");

  if (stateBtn) stateBtn.classList.toggle("active", mode === "states");
  if (countyBtn) countyBtn.classList.toggle("active", mode === "counties");

  if (mode === "states") {
    setText("mapTitle", "U.S. State Momentum Map");
    setText("mapIntroText", "Click a state to open its state view. Switch to counties for local detail.");
    renderStateMap();
  } else {
    setText("mapTitle", "U.S. County Momentum Map");
    setText("mapIntroText", "Click a county to open its profile and view both its light trend and nowcast.");
    renderCountyMap();
  }
}

async function init() {
  try {
    const [home, profileIdx, states, counties, statesGeoJson, countiesGeoJson] = await Promise.all([
      safeLoadJson("data/homepage_summary.json", {}),
      safeLoadJson("data/profile_index.json", []),
      safeLoadJson("data/states_latest.json", []),
      safeLoadJson("data/counties_latest.json", []),
      safeLoadJson("data/regions/us_states_all.geojson", null),
      safeLoadJson("data/regions/us_counties_all.geojson", null)
    ]);

    homepageSummary = home || {};
    profileIndex = profileIdx || [];
    stateLatest = states || [];
    countyLatest = counties || [];
    statesGeo = statesGeoJson;
    countiesGeo = countiesGeoJson;

    const latestDate =
      homepageSummary?.metros?.summary?.latest_month ||
      homepageSummary?.states?.summary?.latest_month ||
      homepageSummary?.counties?.summary?.latest_month ||
      null;

    setText("homeLatestMonth", latestDate ? `Latest available month: ${String(latestDate).slice(0, 7)}` : "Latest available month: unavailable");
    setText("latestDataDate", latestDate ? String(latestDate).slice(0, 7) : "--");

    const national = homepageSummary?.counties?.summary?.national_yoy_pct;
    setText("nationalLightYoy", fmtPct(national));
    setClass("nationalLightYoy", `summary-number ${valueClass(national)}`);

    const metroSummary = homepageSummary?.metros?.summary || {};
    setText("avgMom", fmtPct(metroSummary.avg_mom));
    setClass("avgMom", `summary-number ${valueClass(metroSummary.avg_mom)}`);

    setText("avgYoy", fmtPct(metroSummary.avg_yoy));
    setClass("avgYoy", `summary-number ${valueClass(metroSummary.avg_yoy)}`);

    renderMiniList("top5States", homepageSummary?.states?.top5 || []);
    renderMiniList("top5Metros", homepageSummary?.metros?.top5 || []);
    renderMiniList("top5Counties", homepageSummary?.counties?.top5 || []);
    renderMiniList("top5Cities", homepageSummary?.cities?.top5 || []);

    attachGlobalSearch();

    document.getElementById("stateMapBtn")?.addEventListener("click", () => setMapMode("states"));
    document.getElementById("countyMapBtn")?.addEventListener("click", () => setMapMode("counties"));

    setMapMode("states");
    showStatus("Loaded homepage data.");
  } catch (err) {
    console.error(err);
    showStatus(`Error loading homepage data: ${err.message}`);
  }
}

init();