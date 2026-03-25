let indexChart = null;
let regionChart = null;
let currentDataset = "metros";
let store = {};
let countyShardCache = {};
let filteredRegions = [];
let homepageSummary = {};

function fmtPct(v, capped = false) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "N/A";
  const n = Number(v) * 100;
  const prefix = n >= 0 ? "+" : "";
  if (capped) return `${prefix}${Math.abs(n).toFixed(0)}%+`;
  return `${prefix}${n.toFixed(1)}%`;
}

function numOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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

function showMessage(msg) {
  document.getElementById("statusMount").innerHTML =
    `<div class="status-card"><strong>Status:</strong> ${msg}</div>`;
}

async function loadDataset(dataset) {
  if (dataset === "counties") {
    const [latest, leaders, laggards, indexData, summary, regions, historiesIndex] = await Promise.all([
      loadJson(`data/${dataset}_latest.json`),
      loadJson(`data/${dataset}_leaders.json`),
      loadJson(`data/${dataset}_laggards.json`),
      loadJson(`data/${dataset}_index.json`),
      loadJson(`data/${dataset}_summary.json`),
      loadJson(`data/${dataset}_regions.json`),
      loadJson(`data/${dataset}_histories_index.json`)
    ]);

    store[dataset] = { latest, leaders, laggards, indexData, summary, regions, historiesIndex };
    return;
  }

  const [latest, leaders, laggards, indexData, histories, summary, regions] = await Promise.all([
    loadJson(`data/${dataset}_latest.json`),
    loadJson(`data/${dataset}_leaders.json`),
    loadJson(`data/${dataset}_laggards.json`),
    loadJson(`data/${dataset}_index.json`),
    loadJson(`data/${dataset}_histories.json`),
    loadJson(`data/${dataset}_summary.json`),
    loadJson(`data/${dataset}_regions.json`)
  ]);

  store[dataset] = { latest, leaders, laggards, indexData, histories, summary, regions };
}

async function getCountyHistory(regionId) {
  const dataset = store["counties"];
  const statefp = dataset.historiesIndex[String(regionId)];
  if (!statefp) return [];

  if (!countyShardCache[statefp]) {
    countyShardCache[statefp] = await loadJson(`data/counties_histories_shards/${statefp}.json`);
  }

  return countyShardCache[statefp][String(regionId)] || [];
}

function renderHero(summary, dataset) {
  document.getElementById("heroIndex").textContent =
    summary?.headline_index !== null && summary?.headline_index !== undefined
      ? Number(summary.headline_index).toFixed(1)
      : "--";

  document.getElementById("heroTopRegion").textContent = summary?.top_region?.region_name || "--";
  document.getElementById("heroBottomRegion").textContent = summary?.bottom_region?.region_name || "--";
  document.getElementById("latestMonthText").textContent =
    summary?.latest_month ? `Latest available month: ${String(summary.latest_month).slice(0, 7)}` : "Latest available month: --";

  const labels = {
    metros: "Metro signal",
    states: "State signal",
    counties: "County signal",
    cities: "City signal"
  };
  document.getElementById("indexBadge").textContent = labels[dataset] || "Monthly signal";
}

function renderSummaryCards(summary) {
  const avgMom = document.getElementById("avgMom");
  const avgYoy = document.getElementById("avgYoy");
  const topTrendText = document.getElementById("topTrendText");
  const nationalLightYoy = document.getElementById("nationalLightYoy");

  if (summary?.avg_mom !== undefined && summary?.avg_mom !== null) {
    avgMom.textContent = fmtPct(summary.avg_mom);
    avgMom.className = `summary-number ${valueClass(summary.avg_mom)}`;
  } else {
    avgMom.textContent = "N/A";
    avgMom.className = "summary-number neutral";
  }

  if (summary?.avg_yoy !== undefined && summary?.avg_yoy !== null) {
    avgYoy.textContent = fmtPct(summary.avg_yoy);
    avgYoy.className = `summary-number ${valueClass(summary.avg_yoy)}`;
  } else {
    avgYoy.textContent = "N/A";
    avgYoy.className = "summary-number neutral";
  }

  if (summary?.top_region?.region_name) {
    const yoy = summary.top_region.yoy_pct_display ?? summary.top_region.yoy_pct;
    topTrendText.innerHTML = `${summary.top_region.region_name} <span class="${valueClass(yoy)}">${fmtPct(yoy)}</span>`;
  } else {
    topTrendText.textContent = "--";
  }

  const national = homepageSummary?.counties?.summary?.national_yoy_pct;
  if (national !== undefined && national !== null) {
    nationalLightYoy.textContent = fmtPct(national);
    nationalLightYoy.className = `summary-number ${valueClass(national)}`;
  } else {
    nationalLightYoy.textContent = "N/A";
    nationalLightYoy.className = "summary-number neutral";
  }
}

function renderIndexChart(indexData, datasetLabel) {
  const ctx = document.getElementById("indexChart").getContext("2d");
  if (indexChart) indexChart.destroy();

  indexChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: indexData.map(d => String(d.date).slice(0, 7)),
      datasets: [{
        label: `${datasetLabel} Headline Index`,
        data: indexData.map(d => numOrNull(d.index_level)),
        borderColor: "#8fc0ff",
        backgroundColor: "rgba(143,192,255,0.15)",
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
}

function makeRow(row) {
  const yoyClass = valueClass(row.yoy_pct_display ?? row.yoy_pct);
  const momClass = valueClass(row.mom_pct_display ?? row.mom_pct);
  const trendLabel = row.trend_label || "N/A";
  const trendScore = row.trend_score === null || row.trend_score === undefined
    ? "N/A"
    : Number(row.trend_score).toFixed(2);

  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${row.region_name}</td>
    <td class="${yoyClass}">${fmtPct(row.yoy_pct_display ?? row.yoy_pct, row.yoy_capped)}</td>
    <td class="${momClass}">${fmtPct(row.mom_pct_display ?? row.mom_pct, row.mom_capped)}</td>
    <td title="Trend score: ${trendScore}">${trendLabel}</td>
  `;
  tr.addEventListener("click", async () => {
    document.getElementById("regionPicker").value = row.region_id;
    await renderRegion(currentDataset, row.region_id, row.region_name);
  });
  return tr;
}

function renderTables(leaders, laggards) {
  const brightBody = document.querySelector("#brightTable tbody");
  const dimBody = document.querySelector("#dimTable tbody");
  brightBody.innerHTML = "";
  dimBody.innerHTML = "";

  (leaders || []).slice(0, 20).forEach(r => brightBody.appendChild(makeRow(r)));
  (laggards || []).slice(0, 20).forEach(r => dimBody.appendChild(makeRow(r)));
}

function populatePicker(regions) {
  const picker = document.getElementById("regionPicker");
  picker.innerHTML = "";

  regions.forEach(r => {
    const opt = document.createElement("option");
    opt.value = r.region_id;
    opt.textContent = r.region_name;
    picker.appendChild(opt);
  });
}

function renderRegionPicker(regions) {
  filteredRegions = [...regions];
  populatePicker(filteredRegions);

  const picker = document.getElementById("regionPicker");
  const search = document.getElementById("regionSearch");

  picker.onchange = async () => {
    const selected = filteredRegions.find(r => r.region_id === picker.value);
    if (selected) await renderRegion(currentDataset, selected.region_id, selected.region_name);
  };

  search.value = "";
  search.oninput = () => {
    const q = search.value.trim().toLowerCase();
    filteredRegions = regions.filter(r => r.region_name.toLowerCase().includes(q));
    populatePicker(filteredRegions);
  };
}

async function renderRegion(dataset, regionId, regionName) {
  let rows = [];

  if (dataset === "counties") {
    rows = await getCountyHistory(regionId);
  } else {
    rows = store[dataset].histories[String(regionId)] || [];
  }

  const ctx = document.getElementById("regionChart").getContext("2d");
  if (regionChart) regionChart.destroy();

  regionChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: rows.map(r => String(r.date).slice(0, 7)),
      datasets: [{
        label: `${regionName} light density`,
        data: rows.map(r => numOrNull(r.density_3m_smooth ?? r.light_density)),
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

  document.getElementById("regionTitle").textContent = `Region Explorer — ${regionName}`;
}

function renderMiniList(elementId, rows) {
  const el = document.getElementById(elementId);
  if (!el) return;
  el.innerHTML = "";

  (rows || []).slice(0, 5).forEach(row => {
    const li = document.createElement("li");
    const yoy = row.yoy_pct_display ?? row.yoy_pct;
    li.innerHTML = `<span>${row.region_name}</span><span class="${valueClass(yoy)}">${fmtPct(yoy, row.yoy_capped)}</span>`;
    el.appendChild(li);
  });
}

function renderHomepageLists() {
  renderMiniList("top5Metros", homepageSummary?.metros?.top5 || []);
  renderMiniList("bottom5Metros", homepageSummary?.metros?.bottom5 || []);
  renderMiniList("top5States", homepageSummary?.states?.top5 || []);
  renderMiniList("bottom5States", homepageSummary?.states?.bottom5 || []);
  renderMiniList("top5Counties", homepageSummary?.counties?.top5 || []);
  renderMiniList("bottom5Counties", homepageSummary?.counties?.bottom5 || []);
  renderMiniList("top5Cities", homepageSummary?.cities?.top5 || []);
  renderMiniList("bottom5Cities", homepageSummary?.cities?.bottom5 || []);
}

function renderCountyMap() {
  const countyRows = store?.counties?.latest || [];
  const usable = countyRows.filter(r => r.region_id && r.trend_score !== null && r.trend_score !== undefined);

  const trace = {
    type: "choropleth",
    geojson: "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
    featureidkey: "id",
    locations: usable.map(r => String(r.region_id).padStart(5, "0")),
    z: usable.map(r => Number(r.trend_score)),
    text: usable.map(r => `${r.region_name}<br>Trend: ${r.trend_label || "N/A"}<br>YoY: ${fmtPct(r.yoy_pct_display ?? r.yoy_pct, r.yoy_capped)}`),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0.0, "#fb7185"],
      [0.35, "#f59e0b"],
      [0.5, "#cbd5e1"],
      [0.65, "#60a5fa"],
      [1.0, "#4ade80"]
    ],
    zmid: 0,
    marker: {
      line: {
        color: "rgba(255,255,255,0.08)",
        width: 0.15
      }
    },
    colorbar: {
      title: "Trend",
      tickfont: { color: "#e5ecff" },
      titlefont: { color: "#e5ecff" }
    }
  };

  const layout = {
    geo: {
      scope: "usa",
      bgcolor: "rgba(0,0,0,0)",
      lakecolor: "rgba(0,0,0,0)",
      showlakes: false,
      showland: true,
      landcolor: "rgba(255,255,255,0.02)"
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 0, r: 0, b: 0, l: 0 },
    font: { color: "#e5ecff" }
  };

  Plotly.newPlot("countyMap", [trace], layout, {
    responsive: true,
    displayModeBar: false
  });

  const mapEl = document.getElementById("countyMap");
  mapEl.on("plotly_click", async (data) => {
    const point = data.points?.[0];
    if (!point) return;

    const fips = String(point.location).padStart(5, "0");
    const row = usable.find(r => String(r.region_id).padStart(5, "0") === fips);
    if (!row) return;

    await renderDataset("counties");
    document.getElementById("regionPicker").value = fips;
    await renderRegion("counties", fips, row.region_name);
  });
}

function activateDatasetButton(dataset) {
  document.querySelectorAll(".dataset-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.dataset === dataset);
  });
}

async function renderDataset(dataset) {
  currentDataset = dataset;
  activateDatasetButton(dataset);

  const data = store[dataset];
  renderHero(data.summary, dataset);
  renderSummaryCards(data.summary);
  renderIndexChart(data.indexData, dataset.charAt(0).toUpperCase() + dataset.slice(1));
  renderTables(data.leaders, data.laggards);
  renderRegionPicker(data.regions);

  if (data.regions.length > 0) {
    document.getElementById("regionPicker").value = data.regions[0].region_id;
    await renderRegion(dataset, data.regions[0].region_id, data.regions[0].region_name);
  }

  showMessage(`Loaded ${dataset} dataset.`);
}

async function init() {
  try {
    homepageSummary = await loadJson("data/homepage_summary.json");

    await loadDataset("metros");
    await loadDataset("states");
    await loadDataset("counties");
    await loadDataset("cities");

    renderHomepageLists();
    renderCountyMap();

    document.querySelectorAll(".dataset-btn").forEach(btn => {
      btn.addEventListener("click", async () => renderDataset(btn.dataset.dataset));
    });

    await renderDataset("metros");
  } catch (err) {
    console.error(err);
    showMessage(`Error loading site data: ${err.message}`);
  }
}

init();