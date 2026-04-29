let profileChart = null;

let profileIndex = [];
let statesGeo = null;
let countiesGeo = null;
let metrosGeo = null;
let citiesGeo = null;

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

function normalizeText(s) {
  return String(s || "").trim().toLowerCase();
}

function capitalize(s) {
  return String(s || "").charAt(0).toUpperCase() + String(s || "").slice(1);
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
    feature?.properties?.GEOID20 ??
    feature?.properties?.GEOID10 ??
    feature?.properties?.CBSAFP ??
    feature?.properties?.PLACEFP ??
    feature?.id ??
    ""
  );
}

function featureName(feature) {
  return String(
    feature?.properties?.region_name ??
    feature?.properties?.NAME ??
    feature?.properties?.NAMELSAD ??
    ""
  );
}

function getStateIdForProfile(p) {
  if (p.dataset === "states") return normalizedId("states", p.region_id);
  if (p.dataset === "counties") return normalizedId("counties", p.region_id).slice(0, 2);
  if (p.dataset === "cities") return normalizedId("cities", p.region_id).slice(0, 2);
  return null;
}

function stateAbbrFromFips(fips) {
  const map = {
    "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC","12":"FL","13":"GA",
    "15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA",
    "26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY",
    "37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX",
    "49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY"
  };
  return map[String(fips).padStart(2, "0")] || "";
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
    const countyId = normalizedId("counties", id);

    if (!datasetProfilesCache.countiesIndex) {
      datasetProfilesCache.countiesIndex = await loadJson("data/profiles/counties_index.json");
    }

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
  let peerRankLabel = `${capitalize(dataset)} Ranking`;
  let peerAverageLabel = `${capitalize(dataset)} Average`;
  let peerDifferenceLabel = `Difference from ${capitalize(dataset)} Average`;

  if (dataset === "counties") {
    peerRows = rows.filter(r => r.region_id.slice(0, 2) === stateId);
    peerRankLabel = "County Ranking in State";
    peerAverageLabel = "Average County Growth in State";
    peerDifferenceLabel = "Difference from State County Average";
  }

  if (dataset === "cities") {
    peerRows = rows.filter(r => r.region_id.slice(0, 2) === stateId);
    peerRankLabel = "City Ranking in State";
    peerAverageLabel = "Average City Growth in State";
    peerDifferenceLabel = "Difference from State City Average";
  }

  if (dataset === "states") {
    peerRankLabel = "State Ranking";
    peerAverageLabel = "Average State Growth";
    peerDifferenceLabel = "Difference from Average State";
  }

  if (dataset === "metros") {
    peerRankLabel = "Metro Ranking";
    peerAverageLabel = "Average Metro Growth";
    peerDifferenceLabel = "Difference from Average Metro";
  }

  const peerIndex = peerRows.findIndex(r => r.region_id === id);
  const peerRank = peerIndex >= 0 ? peerIndex + 1 : null;

  const valid = peerRows.map(r => Number(r.yoy_pct_display)).filter(v => Number.isFinite(v));
  const peerAvg = valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : null;
  const peerDiff = Number.isFinite(value) && peerAvg !== null ? value - peerAvg : null;

  return {
    nationalRank,
    nationalCount: rows.length,
    peerRank,
    peerCount: peerRows.length,
    peerAvg,
    peerDiff,
    peerRankLabel,
    peerAverageLabel,
    peerDifferenceLabel
  };
}

function getStateCountyFeatures(stateId) {
  return countiesGeo?.features?.filter(f =>
    featureId(f).padStart(5, "0").slice(0, 2) === stateId
  ) || [];
}

function getCityFeaturesForState(stateId) {
  if (!citiesGeo?.features) return [];

  return citiesGeo.features.filter(f => {
    const fid = featureId(f).padStart(7, "0");
    return fid.slice(0, 2) === stateId;
  });
}

function getMetroFeaturesForState(stateId) {
  if (!metrosGeo?.features) return [];

  const abbr = stateAbbrFromFips(stateId);
  if (!abbr) return [];

  return metrosGeo.features.filter(f => {
    const id = featureId(f);
    const name = featureName(f);
    const matching = metrosLatest.find(m => normalizedId("metros", m.region_id) === id);
    const metroName = matching?.region_name || name;
    return metroName.includes(abbr);
  });
}

function makeStateCountyMap(p) {
  const stateId = normalizedId("states", p.region_id);
  const features = getStateCountyFeatures(stateId);

  if (features.length) {
    const locations = features.map(f => featureId(f).padStart(5, "0"));
    const rowsById = new Map(countiesLatest.map(r => [normalizedId("counties", r.region_id), r]));

    const z = locations.map(loc => Number(rowsById.get(loc)?.yoy_pct_display ?? 0));
    const text = locations.map(loc => {
      const r = rowsById.get(loc);
      return `${r?.region_name || loc}<br>Light YoY: ${fmtPct(r?.yoy_pct_display)}<br>Click county`;
    });

    return {
      geo: { type: "FeatureCollection", features },
      locations,
      z,
      text,
      clickDataset: "counties",
      colorscale: lightScale()
    };
  }

  const stateFeatures = statesGeo?.features?.filter(f => featureId(f).padStart(2, "0") === stateId) || [];
  return {
    geo: { type: "FeatureCollection", features: stateFeatures },
    locations: [stateId],
    z: [1],
    text: [p.region_name],
    clickDataset: null,
    colorscale: [[0, "#60a5fa"], [1, "#60a5fa"]]
  };
}

function makeCountyInStateMap(p) {
  const countyId = normalizedId("counties", p.region_id);
  const stateId = countyId.slice(0, 2);
  const features = getStateCountyFeatures(stateId);

  const locations = features.map(f => featureId(f).padStart(5, "0"));
  const rowsById = new Map(countiesLatest.map(r => [normalizedId("counties", r.region_id), r]));

  const z = locations.map(loc => loc === countyId ? 999 : Number(rowsById.get(loc)?.yoy_pct_display ?? 0));
  const text = locations.map(loc => {
    const r = rowsById.get(loc);
    const selected = loc === countyId ? "<br><b>Selected county</b>" : "<br>Click county";
    return `${r?.region_name || loc}<br>Light YoY: ${fmtPct(r?.yoy_pct_display)}${selected}`;
  });

  return {
    geo: { type: "FeatureCollection", features },
    locations,
    z,
    text,
    clickDataset: "counties",
    colorscale: [
      [0, "rgba(96,165,250,0.22)"],
      [0.45, "rgba(96,165,250,0.50)"],
      [0.998, "#60a5fa"],
      [1, "#ffd36b"]
    ]
  };
}

function makeMetroMap(p) {
  const id = normalizedId("metros", p.region_id);
  let features = metrosGeo?.features?.filter(f => featureId(f) === id) || [];

  if (!features.length) {
    const targetName = normalizeText(p.region_name);
    features = metrosGeo?.features?.filter(f => normalizeText(featureName(f)) === targetName) || [];
  }

  return {
    geo: { type: "FeatureCollection", features },
    locations: features.map(f => featureId(f)),
    z: features.map(() => 1),
    text: features.map(() => p.region_name),
    clickDataset: null,
    colorscale: [[0, "#60a5fa"], [1, "#ffd36b"]]
  };
}

function makeCityBackgroundMap(p) {
  const stateId = normalizedId("cities", p.region_id).slice(0, 2);
  const countyFeatures = getStateCountyFeatures(stateId);
  const countyLocations = countyFeatures.map(f => featureId(f).padStart(5, "0"));

  return {
    geo: { type: "FeatureCollection", features: countyFeatures },
    locations: countyLocations,
    z: countyLocations.map(() => 0.4),
    text: countyLocations.map(() => "County background"),
    clickDataset: "counties",
    colorscale: [[0, "rgba(96,165,250,0.22)"], [1, "rgba(96,165,250,0.45)"]]
  };
}

function lightScale() {
  return [
    [0.0, "#fb7185"],
    [0.35, "#f59e0b"],
    [0.5, "#cbd5e1"],
    [0.65, "#60a5fa"],
    [1.0, "#4ade80"]
  ];
}

function renderShapeMap(p) {
  const boxId = "profileMiniMap";
  const el = document.getElementById(boxId);

  let mapData = null;

  if (p.dataset === "states") mapData = makeStateCountyMap(p);
  if (p.dataset === "counties") mapData = makeCountyInStateMap(p);
  if (p.dataset === "metros") mapData = makeMetroMap(p);
  if (p.dataset === "cities") mapData = makeCityBackgroundMap(p);

  if (!mapData || !mapData.geo.features.length) {
    el.innerHTML = `<div class="mini-map-fallback">Outline unavailable</div>`;
    return;
  }

  const traces = [
    {
      type: "choropleth",
      geojson: mapData.geo,
      featureidkey: "properties.region_id",
      locations: mapData.locations,
      z: mapData.z,
      text: mapData.text,
      hovertemplate: "%{text}<extra></extra>",
      colorscale: mapData.colorscale,
      zmid: 0,
      showscale: false,
      marker: { line: { color: "rgba(255,255,255,0.55)", width: 0.65 } }
    }
  ];

  if (p.dataset === "states") {
    const stateId = normalizedId("states", p.region_id);

    const cityFeatures = getCityFeaturesForState(stateId);
    if (cityFeatures.length) {
      traces.push({
        type: "choropleth",
        geojson: { type: "FeatureCollection", features: cityFeatures },
        featureidkey: "properties.region_id",
        locations: cityFeatures.map(f => featureId(f).padStart(7, "0")),
        z: cityFeatures.map(() => 1),
        text: cityFeatures.map(f => `${featureName(f)}<br>City`),
        hovertemplate: "%{text}<extra></extra>",
        colorscale: [[0, "#ffd36b"], [1, "#ffd36b"]],
        showscale: false,
        marker: { line: { color: "rgba(255,211,107,0.9)", width: 1.1 } }
      });
    }

    const metroFeatures = getMetroFeaturesForState(stateId);
    if (metroFeatures.length) {
      traces.push({
        type: "choropleth",
        geojson: { type: "FeatureCollection", features: metroFeatures },
        featureidkey: "properties.region_id",
        locations: metroFeatures.map(f => featureId(f)),
        z: metroFeatures.map(() => 1),
        text: metroFeatures.map(f => `${featureName(f)}<br>Metro`),
        hovertemplate: "%{text}<extra></extra>",
        colorscale: [[0, "rgba(255,255,255,0.0)"], [1, "rgba(255,255,255,0.0)"]],
        showscale: false,
        marker: { line: { color: "rgba(255,255,255,0.85)", width: 1.6 } }
      });
    }
  }

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

  Plotly.newPlot(boxId, traces, layout, { responsive: true, displayModeBar: false });

  if (mapData.clickDataset === "counties") {
    el.on("plotly_click", data => {
      const point = data.points?.[0];
      if (!point) return;
      const clickedId = String(point.location).padStart(5, "0");
      window.location.href = `profile.html?dataset=counties&id=${encodeURIComponent(clickedId)}`;
    });
  }
}

function renderProfile(p) {
  const latestDate = p.history?.length ? String(p.history[p.history.length - 1].date).slice(0, 7) : "--";
  const peer = computePeerStats(p);

  document.getElementById("profileName").textContent = p.region_name;
  document.getElementById("profileTitleName").textContent = p.region_name;
  document.getElementById("profileSubtitle").textContent = `${p.dataset} profile with light data and nowcast.`;
  document.getElementById("profileTypeText").textContent = `${p.dataset} · latest available data: ${latestDate}`;

  document.getElementById("profileLatestDate").textContent = latestDate;
  document.getElementById("profileRank").textContent = `${peer.nationalRank ?? "--"} of ${peer.nationalCount || "--"}`;
  document.getElementById("profilePercentile").textContent = p.percentile != null ? `${p.percentile}%` : "--";

  document.getElementById("peerRankTopLabel").textContent = peer.peerRankLabel;
  document.getElementById("profilePeerRankTop").textContent = peer.peerRank ? `${peer.peerRank} of ${peer.peerCount}` : "--";

  document.getElementById("peerDiffTopLabel").textContent = "Vs. Peer Avg.";
  document.getElementById("profileVsPeerTop").textContent = peer.peerDiff !== null && peer.peerDiff !== undefined ? fmtPct(peer.peerDiff) : "--";
  document.getElementById("profileVsPeerTop").className = valueClass(peer.peerDiff);

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

  document.getElementById("peerRankLabel").textContent = peer.peerRankLabel;
  document.getElementById("peerAverageLabel").textContent = peer.peerAverageLabel;
  document.getElementById("peerDifferenceLabel").textContent = peer.peerDifferenceLabel;

  document.getElementById("profilePeerRank").textContent = peer.peerRank ? `${peer.peerRank} of ${peer.peerCount}` : "--";
  document.getElementById("profilePeerAverage").textContent = peer.peerAvg !== null && peer.peerAvg !== undefined ? fmtPct(peer.peerAvg) : "--";
  document.getElementById("profilePeerDifference").textContent = peer.peerDiff !== null && peer.peerDiff !== undefined ? fmtPct(peer.peerDiff) : "--";
  document.getElementById("profilePeerDifference").className = valueClass(peer.peerDiff);

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
}

async function init() {
  profileIndex = await loadJson("data/profile_index.json");

  const [stateGeo, countyGeo, metroGeo, cityGeo, states, counties, cities, metros] = await Promise.all([
    safeLoadJson("data/regions/us_states_all.geojson", null),
    safeLoadJson("data/regions/us_counties_all.geojson", null),
    safeLoadJson("data/regions/us_metros_all.geojson", null),
    safeLoadJson("data/regions/us_cities_top200.geojson", null),
    safeLoadJson("data/states_latest.json", []),
    safeLoadJson("data/counties_latest.json", []),
    safeLoadJson("data/cities_latest.json", []),
    safeLoadJson("data/metros_latest.json", [])
  ]);

  statesGeo = stateGeo;
  countiesGeo = countyGeo;
  metrosGeo = metroGeo;
  citiesGeo = cityGeo;

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