let indexChart = null;
let regionChart = null;
let histories = {};

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "--";
  const n = Number(v) * 100;
  return `${n >= 0 ? "+" : ""}${n.toFixed(1)}%`;
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
  if (!res.ok) {
    throw new Error(`Failed to load ${path}: ${res.status}`);
  }
  return await res.json();
}

function showMessage(msg) {
  const mount = document.getElementById("statusMount");
  mount.innerHTML = `<div class="status-card"><strong>Status:</strong> ${msg}</div>`;
}

async function loadData() {
  try {
    const [latest, leaders, laggards, indexData, historiesData, summary] = await Promise.all([
      loadJson("data/latest.json"),
      loadJson("data/leaders.json"),
      loadJson("data/laggards.json"),
      loadJson("data/index.json"),
      loadJson("data/histories.json"),
      loadJson("data/summary.json"),
    ]);

    histories = historiesData || {};

    if (!Array.isArray(indexData) || indexData.length === 0) {
      showMessage("No index data found.");
      return;
    }

    renderHero(summary);
    renderSummaryCards(summary);
    renderIndexChart(indexData);
    renderTables(leaders, laggards);

    if (Array.isArray(latest)) {
      showMessage(`Loaded ${latest.length} ranking rows.`);
    }
  } catch (err) {
    console.error(err);
    showMessage(`Error loading site data: ${err.message}`);
  }
}

function renderHero(summary) {
  document.getElementById("heroIndex").textContent =
    summary && summary.headline_index !== null && summary.headline_index !== undefined
      ? Number(summary.headline_index).toFixed(1)
      : "--";

  document.getElementById("heroTopRegion").textContent =
    summary?.top_region?.region_name || "--";

  document.getElementById("heroBottomRegion").textContent =
    summary?.bottom_region?.region_name || "--";

  document.getElementById("latestMonthText").textContent =
    summary?.latest_month ? `Latest available month: ${String(summary.latest_month).slice(0, 7)}` : "Latest available month: --";
}

function renderSummaryCards(summary) {
  const avgMom = document.getElementById("avgMom");
  const avgYoy = document.getElementById("avgYoy");
  const topTrendText = document.getElementById("topTrendText");

  if (summary?.avg_mom !== undefined && summary?.avg_mom !== null) {
    avgMom.textContent = fmtPct(summary.avg_mom);
    avgMom.className = `summary-number ${valueClass(summary.avg_mom)}`;
  }

  if (summary?.avg_yoy !== undefined && summary?.avg_yoy !== null) {
    avgYoy.textContent = fmtPct(summary.avg_yoy);
    avgYoy.className = `summary-number ${valueClass(summary.avg_yoy)}`;
  }

  if (summary?.top_region?.region_name) {
    const yoy = summary.top_region.yoy_pct;
    topTrendText.innerHTML = `${summary.top_region.region_name} <span class="${valueClass(yoy)}">${fmtPct(yoy)}</span>`;
  }
}

function renderIndexChart(indexData) {
  const ctx = document.getElementById("indexChart").getContext("2d");

  if (indexChart) indexChart.destroy();

  indexChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: indexData.map(d => String(d.date).slice(0, 7)),
      datasets: [{
        label: "Headline Index",
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
      plugins: {
        legend: { labels: { color: "#e5ecff" } }
      },
      scales: {
        x: {
          ticks: { color: "#9fb0d0" },
          grid: { color: "rgba(255,255,255,0.06)" }
        },
        y: {
          ticks: { color: "#9fb0d0" },
          grid: { color: "rgba(255,255,255,0.06)" }
        }
      }
    }
  });
}

function makeRow(row) {
  const yoyClass = valueClass(row.yoy_pct);
  const momClass = valueClass(row.mom_pct);

  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${row.region_name}</td>
    <td class="${yoyClass}">${fmtPct(row.yoy_pct)}</td>
    <td class="${momClass}">${fmtPct(row.mom_pct)}</td>
    <td>${row.trend_score === null || row.trend_score === undefined ? "--" : Number(row.trend_score).toFixed(2)}</td>
  `;
  tr.addEventListener("click", () => renderRegion(row.region_id, row.region_name));
  return tr;
}

function renderTables(leaders, laggards) {
  const brightBody = document.querySelector("#brightTable tbody");
  const dimBody = document.querySelector("#dimTable tbody");

  brightBody.innerHTML = "";
  dimBody.innerHTML = "";

  (leaders || []).slice(0, 10).forEach(r => brightBody.appendChild(makeRow(r)));
  (laggards || []).slice(0, 10).forEach(r => dimBody.appendChild(makeRow(r)));

  if (leaders && leaders.length > 0) {
    renderRegion(leaders[0].region_id, leaders[0].region_name);
  }
}

function renderRegion(regionId, regionName) {
  const rows = histories[String(regionId)] || [];
  const ctx = document.getElementById("regionChart").getContext("2d");

  if (regionChart) regionChart.destroy();

  regionChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: rows.map(r => String(r.date).slice(0, 7)),
      datasets: [{
        label: `${regionName} light density`,
        data: rows.map(r => numOrNull(r.light_density)),
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
        x: {
          ticks: { color: "#9fb0d0" },
          grid: { color: "rgba(255,255,255,0.06)" }
        },
        y: {
          ticks: { color: "#9fb0d0" },
          grid: { color: "rgba(255,255,255,0.06)" }
        }
      }
    }
  });

  document.getElementById("regionTitle").textContent = `Region Explorer — ${regionName}`;
}

loadData();