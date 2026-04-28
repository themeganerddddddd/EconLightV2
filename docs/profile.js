let profileChart = null;
let profileIndex = [];
let statesGeo = null;
let countiesGeo = null;
let metrosGeo = null;
let countyShardCache = {};
let datasetProfilesCache = {};

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

function getParams() {
  const params = new URLSearchParams(window.location.search);
  return {
    dataset: params.get("dataset") || "states",
    id: params.get("id") || "06"
  };
}

async function getProfile(dataset, id) {
  if (dataset === "counties") {
    if (!datasetProfilesCache.countiesIndex) {
      datasetProfilesCache.countiesIndex = await loadJson("data/profiles/counties_index.json");
    }
    const statefp = datasetProfilesCache.countiesIndex[String(id).padStart(5, "0")];
    if (!statefp) return null;

    if (!countyShardCache[statefp]) {
      countyShardCache[statefp] = await loadJson(`data/profiles/counties_by_state/${statefp}.json`);
    }
    return countyShardCache[statefp][String(id).padStart(5, "0")] || null;
  }

  if (!datasetProfilesCache[dataset]) {
    datasetProfilesCache[dataset] = await loadJson(`data/profiles/${dataset}.json`);
  }

  const key =
    dataset === "states" ? String(id).padStart(2, "0") :
    dataset === "cities" ? String(id).padStart(7, "0") :
    String(id);

  return datasetProfilesCache[dataset][key] || datasetProfilesCache[dataset][String(id)] || null;
}

function featureId(feature) {
  return String(
    feature?.properties?.region_id ??
    feature?.properties?.GEOID ??
    feature?.id ??
    ""
  );
}

function renderShapeMap(p) {
  const boxId = "profileMiniMap";
  const id =
    p.dataset === "states" ? String(p.region_id).padStart(2, "0") :
    p.dataset === "counties" ? String(p.region_id).padStart(5, "0") :
    p.dataset === "cities" ? String(p.region_id).padStart(7, "0") :
    String(p.region_id);

  let geo = null;
  let features = [];

  if (p.dataset === "states" && statesGeo?.features) {
    features = statesGeo.features.filter(f => featureId(f).padStart(2, "0") === id);
  } else if (p.dataset === "counties" && countiesGeo?.features) {
    features = countiesGeo.features.filter(f => featureId(f).padStart(5, "0") === id);
  } else if (p.dataset === "metros" && metrosGeo?.features) {
    features = metrosGeo.features.filter(f => featureId(f) === id);
  }

  if (!features.length) {
    document.getElementById(boxId).innerHTML = `<div class="mini-map-fallback">Outline unavailable</div>`;
    return;
  }

  geo = { type: "FeatureCollection", features };

  const trace = {
    type: "choropleth",
    geojson: geo,
    featureidkey: "properties.region_id",
    locations: [id],
    z: [1],
    text: [p.region_name],
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [[0, "#ffd36b"], [1, "#60a5fa"]],
    showscale: false,
    marker: { line: { color: "rgba(255,255,255,0.85)", width: 1.2 } }
  };

  const layout = {
    geo: {
      fitbounds: "locations",
      bgcolor: "rgba(0,0,0,0)",
      showland: true,
      landcolor: "rgba(255,255,255,0.03)"
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 0, r: 0, b: 0, l: 0 },
    font: { color: "#e5ecff" }
  };

  Plotly.newPlot(boxId, [trace], layout, { responsive: true, displayModeBar: false });
}

function setupRankArrows(p) {
  const sameDataset = profileIndex
    .filter(r => r.dataset === p.dataset)
    .sort((a, b) => (a.rank_overall ?? 999999) - (b.rank_overall ?? 999999));

  const idx = sameDataset.findIndex(r => String(r.region_id) === String(p.region_id));

  const prev = document.getElementById("prevRankBtn");
  const next = document.getElementById("nextRankBtn");

  if (prev) {
    prev.disabled = idx <= 0;
    prev.onclick = () => {
      if (idx > 0) {
        const r = sameDataset[idx - 1];
        if (r.dataset === "states") {
          window.location.href = `states.html?id=${encodeURIComponent(String(r.region_id).padStart(2, "0"))}`;
        } else {
          window.location.href = `profile.html?dataset=${encodeURIComponent(r.dataset)}&id=${encodeURIComponent(r.region_id)}`;
        }
      }
    };
  }

  if (next) {
    next.disabled = idx < 0 || idx >= sameDataset.length - 1;
    next.onclick = () => {
      if (idx >= 0 && idx < sameDataset.length - 1) {
        const r = sameDataset[idx + 1];
        if (r.dataset === "states") {
          window.location.href = `states.html?id=${encodeURIComponent(String(r.region_id).padStart(2, "0"))}`;
        } else {
          window.location.href = `profile.html?dataset=${encodeURIComponent(r.dataset)}&id=${encodeURIComponent(r.region_id)}`;
        }
      }
    };
  }
}

function renderProfile(p) {
  const latestDate = p.history?.length ? String(p.history[p.history.length - 1].date).slice(0, 7) : "--";

  document.getElementById("profileName").textContent = p.region_name;
  document.getElementById("profileTitleName").textContent = p.region_name;
  document.getElementById("profileSubtitle").textContent = `${p.dataset} profile with light data and nowcast.`;
  document.getElementById("profileTypeText").textContent = `${p.dataset} · latest available data: ${latestDate}`;

  document.getElementById("profileLatestDate").textContent = `Latest: ${latestDate}`;
  document.getElementById("profileRank").textContent = `Rank: ${p.rank_overall ?? "--"}`;
  document.getElementById("profilePercentile").textContent = p.percentile != null ? `Percentile: ${p.percentile}%` : "Percentile: --";
  document.getElementById("profileTrend").textContent = `Trend: ${p.trend_label || "--"}`;

  const yoy = document.getElementById("profileYoy");
  yoy.textContent = fmtPct(p.yoy_pct_display);
  yoy.className = `summary-number ${valueClass(p.yoy_pct_display)}`;

  const mom = document.getElementById("profileMom");
  mom.textContent = fmtPct(p.mom_pct_display);
  mom.className = `summary-number ${valueClass(p.mom_pct_display)}`;

  document.getElementById("profileDirection").textContent = p.direction || "--";
  document.getElementById("profileDirection").className = `summary-number small ${valueClass(p.yoy_pct_display)}`;
  document.getElementById("profileTrendScore").textContent = p.trend_score != null ? Number(p.trend_score).toFixed(2) : "--";

  document.getElementById("profileEmpNowcast").textContent = fmtPct(p.employment_yoy_nowcast);
  document.getElementById("profileLaborNowcast").textContent = fmtPct(p.labor_force_yoy_nowcast);
  document.getElementById("profileUrate").textContent = fmtPct(p.unemployment_rate_yoy_change_nowcast);
  document.getElementById("profileConfidence").textContent = p.confidence || "--";
  document.getElementById("profileIndustry").textContent = p.industry_proxy || "--";
  document.getElementById("profilePopulation").textContent = p.population_growth_proxy != null ? fmtPct(p.population_growth_proxy) : "--";

  const ctx = document.getElementById("profileChart").getContext("2d");
  if (profileChart) profileChart.destroy();

  profileChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: (p.history || []).map(r => String(r.date).slice(0, 7)),
      datasets: [{
        label: `${p.region_name} light density`,
        data: (p.history || []).map(r => Number(r.density_3m_smooth ?? r.light_density)),
        borderColor: "#ffd36b",
        backgroundColor: "rgba(255,211,107,0.12)",
        tension: 0.25,
        fill: true,
        pointRadius: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: "#e5ecff" } }
      },
      scales: {
        x: { ticks: { color: "#9fb0d0" }, grid: { color: "rgba(255,255,255,0.06)" } },
        y: { ticks: { color: "#9fb0d0" }, grid: { color: "rgba(255,255,255,0.06)" } }
      }
    }
  });

  renderShapeMap(p);
  setupRankArrows(p);
}

async function init() {
  profileIndex = await loadJson("data/profile_index.json");
  statesGeo = await loadJson("data/regions/us_states_all.geojson").catch(() => null);
  countiesGeo = await loadJson("data/regions/us_counties_all.geojson").catch(() => null);
  metrosGeo = await loadJson("data/regions/us_metros_all.geojson").catch(() => null);

  const params = getParams();
  const p = await getProfile(params.dataset, params.id);

  if (!p) {
    document.getElementById("profileName").textContent = "Profile not found";
    return;
  }

  renderProfile(p);
}

init().catch(console.error);