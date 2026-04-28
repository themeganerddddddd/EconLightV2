let profileChart = null;
let profileIndex = [];
let statesGeo = null;
let countiesGeo = null;
let metrosGeo = null;

let statesLatest = [];
let countiesLatest = [];
let citiesLatest = [];
let metrosLatest = [];

let countyShardCache = {};
let datasetProfilesCache = {};

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "N/A";
  const n = Number(v) * 100;
  return `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
}

function fmtNum(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
  return Number(v).toFixed(2);
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

async function safeLoadJson(path, fallback = null) {
  try {
    return await loadJson(path);
  } catch {
    return fallback;
  }
}

function getParams() {
  const params = new URLSearchParams(window.location.search);
  return {
    dataset: params.get("dataset") || "states",
    id: params.get("id") || "06"
  };
}

function normalizedId(dataset, id) {
  if (dataset === "states") return String(id).padStart(2, "0");
  if (dataset === "counties") return String(id).padStart(5, "0");
  if (dataset === "cities") return String(id).padStart(7, "0");
  return String(id);
}

function featureId(feature) {
  return String(
    feature?.properties?.region_id ??
    feature?.properties?.GEOID ??
    feature?.id ??
    ""
  );
}

function getStateIdForProfile(p) {
  if (p.dataset === "states") return normalizedId("states", p.region_id);
  if (p.dataset === "counties") return normalizedId("counties", p.region_id).slice(0, 2);
  if (p.dataset === "cities") return normalizedId("cities", p.region_id).slice(0, 2);
  return null;
}

function getLatestRowsForDataset(dataset) {
  if (dataset === "states") return statesLatest;
  if (dataset === "counties") return countiesLatest;
  if (dataset === "cities") return citiesLatest;
  if (dataset === "metros") return metrosLatest;
  return [];
}

async function getProfile(dataset, id) {
  if (dataset === "counties") {
    if (!datasetProfilesCache.countiesIndex) {
      datasetProfilesCache.countiesIndex = await loadJson("data/profiles/counties_index.json");
    }
    const countyId = normalizedId("counties", id);
    const statefp = datasetProfilesCache.countiesIndex[countyId] || countyId.slice(0, 2);

    if (!countyShardCache[statefp]) {
      countyShardCache[statefp] = await loadJson(`data/profiles/counties_by_state/${statefp}.json`);
    }
    return countyShardCache[statefp][countyId] || null;
  }

  if (!datasetProfilesCache[dataset]) {
    datasetProfilesCache[dataset] = await loadJson(`data/profiles/${dataset}.json`);
  }

  const key = normalizedId(dataset, id);
  return datasetProfilesCache[dataset][key] || datasetProfilesCache[dataset][String(id)] || null;
}

function computePeerStats(p) {
  const dataset = p.dataset;
  const id = normalizedId(dataset, p.region_id);
  const stateId = getStateIdForProfile(p);
  const value = Number(p.yoy_pct_display);

  let rows = getLatestRowsForDataset(dataset).map(r => ({
    ...r,
    region_id: normalizedId(dataset, r.region_id)
  }));

  rows = rows
    .filter(r => r.yoy_pct_display !== null && r.yoy_pct_display !== undefined && !Number.isNaN(Number(r.yoy_pct_display)))
    .sort((a, b) => Number(b.yoy_pct_display) - Number(a.yoy_pct_display));

  const nationalIndex = rows.findIndex(r => r.region_id === id);
  const nationalRank = nationalIndex >= 0 ? nationalIndex + 1 : p.rank_overall;

  let peerRows = rows;
  let peerLabel = `${dataset}`;
  let peerAverageLabel = `${dataset} average`;
  let peerDifferenceLabel = `Difference from ${dataset} average`;

  if (dataset === "counties" || dataset === "cities") {
    peerRows = rows.filter(r => r.region_id.slice(0, 2) === stateId);
    peerLabel = dataset === "counties" ? "counties in state" : "cities in state";
    peerAverageLabel = dataset === "counties" ? "State county average" : "State city average";
    peerDifferenceLabel = dataset === "counties" ? "Difference from state county average" : "Difference from state city average";
  } else if (dataset === "states") {
    peerLabel = "states";
    peerAverageLabel = "State average";
    peerDifferenceLabel = "Difference from state average";
  } else if (dataset === "metros") {
    peerLabel = "metros";
    peerAverageLabel = "Metro average";
    peerDifferenceLabel = "Difference from metro average";
  }

  const peerIndex = peerRows.findIndex(r => r.region_id === id);
  const peerRank = peerIndex >= 0 ? peerIndex + 1 : null;

  const valid = peerRows.map(r => Number(r.yoy_pct_display)).filter(v => Number.isFinite(v));
  const peerAvg = valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : null;
  const peerDiff = Number.isFinite(value) && peerAvg !== null ? value - peerAvg : null;

  let stateLatest = null;
  if (stateId) {
    stateLatest = statesLatest.find(r => normalizedId("states", r.region_id) === stateId);
  }

  return {
    nationalRank,
    nationalCount: rows.length,
    peerRank,
    peerCount: peerRows.length,
    peerAvg,
    peerDiff,
    peerLabel,
    peerAverageLabel,
    peerDifferenceLabel,
    stateLatest
  };
}

function getStateCountyFeatures(stateId) {
  return countiesGeo?.features?.filter(f =>
    featureId(f).padStart(5, "0").slice(0, 2) === stateId
  ) || [];
}

function makeProfileMapForState(p) {
  const stateId = normalizedId("states", p.region_id);
  const features = getStateCountyFeatures(stateId);

  if (features.length) {
    const locations = features.map(f => featureId(f).padStart(5, "0"));
    const rowsById = new Map(countiesLatest.map(r => [normalizedId("counties", r.region_id), r]));
    const z = locations.map(loc => Number(rowsById.get(loc)?.yoy_pct_display ?? 0));
    return { features, locations, z, selectedId: null, mapType: "state-counties" };
  }

  const stateFeatures = statesGeo?.features?.filter(f => featureId(f).padStart(2, "0") === stateId) || [];
  return { features: stateFeatures, locations: [stateId], z: [1], selectedId: stateId, mapType: "state" };
}

function makeProfileMapForMetro(p) {
  const id = normalizedId("metros", p.region_id);
  const features = metrosGeo?.features?.filter(f => featureId(f) === id) || [];
  return { features, locations: [id], z: [1], selectedId: id, mapType: "metro" };
}

function makeProfileMapForCounty(p) {
  const countyId = normalizedId("counties", p.region_id);
  const stateId = countyId.slice(0, 2);
  const features = getStateCountyFeatures(stateId);
  const locations = features.map(f => featureId(f).padStart(5, "0"));
  const z = locations.map(loc => loc === countyId ? 1 : 0.18);
  return { features, locations, z, selectedId: countyId, mapType: "county-in-state" };
}

function makeProfileMapForCity(p) {
  const stateId = normalizedId("cities", p.region_id).slice(0, 2);
  const features = getStateCountyFeatures(stateId);
  const locations = features.map(f => featureId(f).padStart(5, "0"));
  const z = locations.map(() => 0.35);
  return { features, locations, z, selectedId: null, mapType: "city-state-background" };
}

function renderShapeMap(p) {
  const boxId = "profileMiniMap";
  const el = document.getElementById(boxId);

  let mapData = null;

  if (p.dataset === "states") mapData = makeProfileMapForState(p);
  if (p.dataset === "counties") mapData = makeProfileMapForCounty(p);
  if (p.dataset === "metros") mapData = makeProfileMapForMetro(p);
  if (p.dataset === "cities") mapData = makeProfileMapForCity(p);

  if (!mapData || !mapData.features.length) {
    el.innerHTML = `<div class="mini-map-fallback">Outline unavailable</div>`;
    return;
  }

  const geo = { type: "FeatureCollection", features: mapData.features };

  const trace = {
    type: "choropleth",
    geojson: geo,
    featureidkey: "properties.region_id",
    locations: mapData.locations,
    z: mapData.z,
    text: mapData.locations.map(loc => {
      if (loc === mapData.selectedId) return p.region_name;
      if (mapData.mapType === "county-in-state") return "Other county in state";
      return "Click to open";
    }),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0, "rgba(96,165,250,0.22)"],
      [0.45, "rgba(96,165,250,0.42)"],
      [1, "#ffd36b"]
    ],
    showscale: false,
    marker: { line: { color: "rgba(255,255,255,0.55)", width: 0.7 } }
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

  if (mapData.mapType === "county-in-state" || mapData.mapType === "state-counties") {
    el.on("plotly_click", data => {
      const point = data.points?.[0];
      if (!point) return;
      const clickedId = String(point.location).padStart(5, "0");
      window.location.href = `profile.html?dataset=counties&id=${encodeURIComponent(clickedId)}`;
    });
  }
}

function setupRankArrows(p) {
  const dataset = p.dataset;
  const currentId = normalizedId(dataset, p.region_id);

  const sameDataset = profileIndex
    .filter(r => r.dataset === dataset)
    .sort((a, b) => (a.rank_overall ?? 999999) - (b.rank_overall ?? 999999));

  const idx = sameDataset.findIndex(r => normalizedId(dataset, r.region_id) === currentId);

  const prev = document.getElementById("prevRankBtn");
  const next = document.getElementById("nextRankBtn");

  if (prev) {
    prev.disabled = idx <= 0;
    prev.onclick = () => {
      if (idx > 0) {
        const r = sameDataset[idx - 1];
        if (r.dataset === "states") {
          window.location.href = `states.html?id=${encodeURIComponent(normalizedId("states", r.region_id))}`;
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
          window.location.href = `states.html?id=${encodeURIComponent(normalizedId("states", r.region_id))}`;
        } else {
          window.location.href = `profile.html?dataset=${encodeURIComponent(r.dataset)}&id=${encodeURIComponent(r.region_id)}`;
        }
      }
    };
  }
}

function renderProfile(p) {
  const latestDate = p.history?.length ? String(p.history[p.history.length - 1].date).slice(0, 7) : "--";
  const peer = computePeerStats(p);

  document.getElementById("profileName").textContent = p.region_name;
  document.getElementById("profileTitleName").textContent = p.region_name;
  document.getElementById("profileSubtitle").textContent = `${p.dataset} profile with light data and nowcast.`;
  document.getElementById("profileTypeText").textContent = `${p.dataset} · latest available data: ${latestDate}`;

  document.getElementById("profileLatestDate").textContent = `Latest: ${latestDate}`;
  document.getElementById("profileRank").textContent = `National Rank: ${peer.nationalRank ?? "--"} of ${peer.nationalCount || "--"}`;
  document.getElementById("profilePercentile").textContent = p.percentile != null ? `Percentile: ${p.percentile}%` : "Percentile: --";

  document.getElementById("profileStateRank").textContent =
    peer.peerRank ? `${peer.peerLabel} Rank: ${peer.peerRank} of ${peer.peerCount}` : `${peer.peerLabel} Rank: --`;

  document.getElementById("profileVsState").textContent =
    peer.peerDiff !== null && peer.peerDiff !== undefined ? `Vs. Peer Avg: ${fmtPct(peer.peerDiff)}` : "Vs. Peer Avg: --";

  const yoy = document.getElementById("profileYoy");
  yoy.textContent = fmtPct(p.yoy_pct_display);
  yoy.className = `summary-number ${valueClass(p.yoy_pct_display)}`;

  const mom = document.getElementById("profileMom");
  mom.textContent = fmtPct(p.mom_pct_display);
  mom.className = `summary-number ${valueClass(p.mom_pct_display)}`;

  document.getElementById("profileDirection").textContent = p.direction || "--";
  document.getElementById("profileDirection").className = `summary-number small ${valueClass(p.yoy_pct_display)}`;
  document.getElementById("profileTrendScore").textContent = fmtNum(p.trend_score);

  document.getElementById("profileEmpNowcast").textContent = fmtPct(p.employment_yoy_nowcast);
  document.getElementById("profileLaborNowcast").textContent = fmtPct(p.labor_force_yoy_nowcast);
  document.getElementById("profileUrate").textContent = fmtPct(p.unemployment_rate_yoy_change_nowcast);
  document.getElementById("profileConfidence").textContent = p.confidence || "--";
  document.getElementById("profileIndustry").textContent = p.industry_proxy || "--";
  document.getElementById("profilePopulation").textContent = p.population_growth_proxy != null ? fmtPct(p.population_growth_proxy) : "--";

  document.getElementById("peerRankLabel").textContent = `${peer.peerLabel} ranking`;
  document.getElementById("peerAverageLabel").textContent = peer.peerAverageLabel;
  document.getElementById("peerDifferenceLabel").textContent = peer.peerDifferenceLabel;
  document.getElementById("profileStateRankDetail").textContent = peer.peerRank ? `${peer.peerRank} of ${peer.peerCount}` : "--";
  document.getElementById("profileStateAverage").textContent = peer.peerAvg !== null && peer.peerAvg !== undefined ? fmtPct(peer.peerAvg) : "--";
  document.getElementById("profileStateDifference").textContent = peer.peerDiff !== null && peer.peerDiff !== undefined ? fmtPct(peer.peerDiff) : "--";
  document.getElementById("profileStateDifference").className = valueClass(peer.peerDiff);

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
      plugins: { legend: { labels: { color: "#e5ecff" } } },
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

  const [stateGeo, countyGeo, metroGeo, states, counties, cities, metros] = await Promise.all([
    safeLoadJson("data/regions/us_states_all.geojson", null),
    safeLoadJson("data/regions/us_counties_all.geojson", null),
    safeLoadJson("data/regions/us_metros_all.geojson", null),
    safeLoadJson("data/states_latest.json", []),
    safeLoadJson("data/counties_latest.json", []),
    safeLoadJson("data/cities_latest.json", []),
    safeLoadJson("data/metros_latest.json", [])
  ]);

  statesGeo = stateGeo;
  countiesGeo = countyGeo;
  metrosGeo = metroGeo;

  statesLatest = states.map(r => ({ ...r, region_id: normalizedId("states", r.region_id) }));
  countiesLatest = counties.map(r => ({ ...r, region_id: normalizedId("counties", r.region_id) }));
  citiesLatest = cities.map(r => ({ ...r, region_id: normalizedId("cities", r.region_id) }));
  metrosLatest = metros.map(r => ({ ...r, region_id: normalizedId("metros", r.region_id) }));

  const params = getParams();
  const p = await getProfile(params.dataset, params.id);

  if (!p) {
    document.getElementById("profileName").textContent = "Profile not found";
    return;
  }

  renderProfile(p);
}

init().catch(console.error);