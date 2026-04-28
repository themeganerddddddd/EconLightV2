let profileChart = null;
let profileIndex = [];
let statesGeo = null;
let countiesGeo = null;
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
    dataset: params.get("dataset") || "metros",
    id: params.get("id") || ""
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
  return datasetProfilesCache[dataset][String(id)] || null;
}

function renderMiniMap(dataset, regionId, regionName) {
  const boxId = "profileMiniMap";

  if (dataset === "states" && statesGeo?.features) {
    const feature = statesGeo.features.find(f => String(f.properties.region_id).padStart(2, "0") === String(regionId).padStart(2, "0"));
    if (!feature) return;
    const trace = {
      type: "choropleth",
      geojson: { type: "FeatureCollection", features: [feature] },
      featureidkey: "properties.region_id",
      locations: [String(regionId).padStart(2, "0")],
      z: [1],
      text: [regionName],
      hovertemplate: "%{text}<extra></extra>",
      colorscale: [[0, "#60a5fa"], [1, "#60a5fa"]],
      showscale: false,
      marker: { line: { color: "rgba(255,255,255,0.75)", width: 1.2 } }
    };
    const layout = {
      geo: { fitbounds: "locations", bgcolor: "rgba(0,0,0,0)", showland: true, landcolor: "rgba(255,255,255,0.03)" },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      margin: { t: 0, r: 0, b: 0, l: 0 },
      font: { color: "#e5ecff" }
    };
    Plotly.newPlot(boxId, [trace], layout, { responsive: true, displayModeBar: false });
    return;
  }

  if (dataset === "counties" && countiesGeo?.features) {
    const targetId = String(regionId).padStart(5, "0");
    const feature = countiesGeo.features.find(f => String(f.properties.region_id).padStart(5, "0") === targetId);
    if (!feature) {
      document.getElementById(boxId).innerHTML = `<div class="mini-map-fallback">County outline unavailable</div>`;
      return;
    }
    const trace = {
      type: "choropleth",
      geojson: { type: "FeatureCollection", features: [feature] },
      featureidkey: "properties.region_id",
      locations: [targetId],
      z: [1],
      text: [regionName],
      hovertemplate: "%{text}<extra></extra>",
      colorscale: [[0, "#ffd36b"], [1, "#ffd36b"]],
      showscale: false,
      marker: { line: { color: "rgba(255,255,255,0.75)", width: 1.0 } }
    };
    const layout = {
      geo: { fitbounds: "locations", bgcolor: "rgba(0,0,0,0)", showland: true, landcolor: "rgba(255,255,255,0.03)" },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      margin: { t: 0, r: 0, b: 0, l: 0 },
      font: { color: "#e5ecff" }
    };
    Plotly.newPlot(boxId, [trace], layout, { responsive: true, displayModeBar: false });
    return;
  }

  document.getElementById(boxId).innerHTML = `<div class="mini-map-fallback">Outline map available for states and counties</div>`;
}

function renderProfile(p) {
  document.getElementById("profileName").textContent = p.region_name;
  document.getElementById("profileSubtitle").textContent = `${p.dataset} profile with light data and nowcast.`;

  document.getElementById("profileRank").textContent = p.rank_overall ?? "--";
  document.getElementById("profilePercentile").textContent = p.percentile != null ? `${p.percentile}%` : "--";
  document.getElementById("profileTrend").textContent = p.trend_label || "--";
  document.getElementById("profileLatestDate").textContent = p.history?.length ? String(p.history[p.history.length - 1].date).slice(0, 7) : "--";

  document.getElementById("profileDirection").textContent = p.direction || "--";
  document.getElementById("profileYoy").textContent = fmtPct(p.yoy_pct_display);
  document.getElementById("profileYoy").className = valueClass(p.yoy_pct_display);
  document.getElementById("profileMom").textContent = fmtPct(p.mom_pct_display);
  document.getElementById("profileMom").className = valueClass(p.mom_pct_display);
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
      maintainAspectRatio: false
    }
  });

  renderMiniMap(p.dataset, p.region_id, p.region_name);
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




async function init() {
  profileIndex = await loadJson("data/profile_index.json");
  statesGeo = await loadJson("data/regions/us_states_all.geojson").catch(() => null);
  countiesGeo = await loadJson("data/regions/us_counties_all.geojson").catch(() => null);

  const params = getParams();
  const p = await getProfile(params.dataset, params.id);

  if (!p) {
    document.getElementById("profileName").textContent = "Profile not found";
    return;
  }

  renderProfile(p);
setupRankArrows(p);

}


init().catch(console.error);