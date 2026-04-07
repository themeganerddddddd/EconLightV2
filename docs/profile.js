let profileChart = null;
let profileIndex = [];
let profiles = {};
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

function getParams() {
  const params = new URLSearchParams(window.location.search);
  return {
    dataset: params.get("dataset") || "metros",
    id: params.get("id") || ""
  };
}

function populateSearch(results) {
  const sel = document.getElementById("globalSearchResults");
  sel.innerHTML = "";

  results.forEach(r => {
    const opt = document.createElement("option");
    opt.value = `${r.dataset}|${r.region_id}`;
    opt.textContent = `${r.region_name} (${r.dataset})`;
    sel.appendChild(opt);
  });

  sel.onchange = () => {
    const [dataset, id] = sel.value.split("|");
    window.location.href = `profile.html?dataset=${encodeURIComponent(dataset)}&id=${encodeURIComponent(id)}`;
  };
}

function renderMiniMap(dataset, regionId, regionName) {
  const boxId = "profileMiniMap";

  if (dataset === "states" && statesGeo?.features) {
    const feature = statesGeo.features.find(f => String(f.properties.region_id) === String(regionId));
    if (!feature) return;

    const trace = {
      type: "choropleth",
      geojson: {
        type: "FeatureCollection",
        features: [feature]
      },
      featureidkey: "properties.region_id",
      locations: [String(regionId)],
      z: [1],
      text: [regionName],
      hovertemplate: "%{text}<extra></extra>",
      colorscale: [[0, "#60a5fa"], [1, "#60a5fa"]],
      showscale: false,
      marker: { line: { color: "rgba(255,255,255,0.75)", width: 1.2 } }
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
      geojson: {
        type: "FeatureCollection",
        features: [feature]
      },
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
    return;
  }

  document.getElementById(boxId).innerHTML = `<div class="mini-map-fallback">Outline map available for states and counties</div>`;
}

function renderProfile(p) {
  document.getElementById("profileName").textContent = p.region_name;
  document.getElementById("profileSubtitle").textContent = `${p.dataset} profile with trend, ranking, percentile, and nowcast.`;

  document.getElementById("profileRank").textContent = p.rank_overall ?? "--";
  document.getElementById("profilePercentile").textContent = p.percentile != null ? `${p.percentile}%` : "--";
  document.getElementById("profileDirection").textContent = p.direction || "--";
  document.getElementById("profileTrend").textContent = p.trend_label || "--";

  const yoy = document.getElementById("profileYoy");
  yoy.textContent = fmtPct(p.yoy_pct_display);
  yoy.className = `summary-number ${valueClass(p.yoy_pct_display)}`;

  const mom = document.getElementById("profileMom");
  mom.textContent = fmtPct(p.mom_pct_display);
  mom.className = `summary-number ${valueClass(p.mom_pct_display)}`;

  const emp = document.getElementById("profileEmpNowcast");
  emp.textContent = fmtPct(p.employment_yoy_nowcast);
  emp.className = `summary-number ${valueClass(p.employment_yoy_nowcast)}`;

  document.getElementById("profileConfidence").textContent = p.confidence || "--";
  document.getElementById("profileIndustry").textContent = p.industry_proxy || "--";
  document.getElementById("profilePopulation").textContent = p.population_growth_proxy != null ? fmtPct(p.population_growth_proxy) : "--";
  document.getElementById("profileUrate").textContent = fmtPct(p.unemployment_rate_yoy_change_nowcast);

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

async function init() {
  profileIndex = await loadJson("data/profile_index.json");
  profiles = await loadJson("data/profiles.json");
  statesGeo = await loadJson("data/regions/us_states_contiguous.geojson").catch(() => null);
  countiesGeo = await loadJson("data/regions/us_counties_contiguous.geojson").catch(() => null);

  const params = getParams();
  const p = profiles?.[params.dataset]?.[params.id];

  if (!p) {
    document.getElementById("profileName").textContent = "Profile not found";
    return;
  }

  renderProfile(p);

  const input = document.getElementById("globalSearch");
  input.oninput = () => {
    const q = input.value.trim().toLowerCase();
    const matches = profileIndex.filter(r =>
      r.region_name.toLowerCase().includes(q)
    ).slice(0, 100);
    populateSearch(matches);
  };

  populateSearch(profileIndex.slice(0, 50));
}

init().catch(console.error);