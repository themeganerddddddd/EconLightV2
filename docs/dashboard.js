let indexChart = null;
let regionChart = null;
let store = {};
let filteredRegions = [];
let countyShardCache = {};

function fmtPct(v, capped = false) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "N/A";
  const n = Number(v) * 100;
  const prefix = n >= 0 ? "+" : "";
  if (capped) return `${prefix}${Math.abs(n).toFixed(0)}%+`;
  return `${prefix}${n.toFixed(1)}%`;
}

function valueClass(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "neutral";
  if (n > 0) return "pos";
  if (n < 0) return "neg";
  return "neutral";
}

function numOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function loadJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}: ${res.status}`);
  return await res.json();
}

function getDataset() {
  const params = new URLSearchParams(window.location.search);
  return params.get("dataset") || "metros";
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
    return { latest, leaders, laggards, indexData, summary, regions, historiesIndex };
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
  return { latest, leaders, laggards, indexData, histories, summary, regions };
}

async function getCountyHistory(data, regionId) {
  const statefp = data.historiesIndex[String(regionId)];
  if (!statefp) return [];
  if (!countyShardCache[statefp]) {
    countyShardCache[statefp] = await loadJson(`data/counties_histories_shards/${statefp}.json`);
  }
  return countyShardCache[statefp][String(regionId)] || [];
}

function renderHeader(dataset, summary) {
  const label = dataset.charAt(0).toUpperCase() + dataset.slice(1);
  document.getElementById("dashboardTitle").textContent = `${label} Dashboard`;
  document.getElementById("dashboardSubtitle").textContent = `Detailed ${dataset} trends, rankings, and region histories.`;
  document.getElementById("heroIndex").textContent = summary?.headline_index?.toFixed ? summary.headline_index.toFixed(1) : "--";
  document.getElementById("heroTopRegion").textContent = summary?.top_region?.region_name || "--";
  document.getElementById("heroBottomRegion").textContent = summary?.bottom_region?.region_name || "--";
  document.getElementById("latestMonthText").textContent = summary?.latest_month ? String(summary.latest_month).slice(0, 7) : "--";
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
    options: { responsive: true, maintainAspectRatio: false }
  });
}

function makeRow(row, onClick) {
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${row.region_name}</td>
    <td class="${valueClass(row.yoy_pct_display ?? row.yoy_pct)}">${fmtPct(row.yoy_pct_display ?? row.yoy_pct, row.yoy_capped)}</td>
    <td class="${valueClass(row.mom_pct_display ?? row.mom_pct)}">${fmtPct(row.mom_pct_display ?? row.mom_pct, row.mom_capped)}</td>
    <td>${row.trend_label || "N/A"}</td>
  `;
  tr.addEventListener("click", onClick);
  return tr;
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

async function renderRegion(dataset, data, regionId, regionName) {
  const rows = dataset === "counties"
    ? await getCountyHistory(data, regionId)
    : (data.histories[String(regionId)] || []);

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
    options: { responsive: true, maintainAspectRatio: false }
  });

  document.getElementById("regionTitle").textContent = `Region Explorer — ${regionName}`;
}

async function init() {
  const dataset = getDataset();
  const data = await loadDataset(dataset);
  renderHeader(dataset, data.summary);
  renderIndexChart(data.indexData, dataset);

  const brightBody = document.querySelector("#brightTable tbody");
  const dimBody = document.querySelector("#dimTable tbody");
  brightBody.innerHTML = "";
  dimBody.innerHTML = "";

  data.leaders.slice(0, 20).forEach(r => {
    brightBody.appendChild(makeRow(r, async () => {
      document.getElementById("regionPicker").value = r.region_id;
      await renderRegion(dataset, data, r.region_id, r.region_name);
    }));
  });

  data.laggards.slice(0, 20).forEach(r => {
    dimBody.appendChild(makeRow(r, async () => {
      document.getElementById("regionPicker").value = r.region_id;
      await renderRegion(dataset, data, r.region_id, r.region_name);
    }));
  });

  filteredRegions = [...data.regions];
  populatePicker(filteredRegions);

  const picker = document.getElementById("regionPicker");
  const search = document.getElementById("regionSearch");

  picker.onchange = async () => {
    const selected = filteredRegions.find(r => r.region_id === picker.value);
    if (selected) await renderRegion(dataset, data, selected.region_id, selected.region_name);
  };

  search.oninput = () => {
    const q = search.value.trim().toLowerCase();
    filteredRegions = data.regions.filter(r => r.region_name.toLowerCase().includes(q));
    populatePicker(filteredRegions);
  };

  if (data.regions.length > 0) {
    picker.value = data.regions[0].region_id;
    await renderRegion(dataset, data, data.regions[0].region_id, data.regions[0].region_name);
  }
}

init().catch(err => {
  document.getElementById("statusMount").innerHTML =
    `<div class="status-card"><strong>Status:</strong> ${err.message}</div>`;
});