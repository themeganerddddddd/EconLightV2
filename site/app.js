let indexChart = null;
let regionChart = null;
let histories = {};

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "--";
  return `${(v * 100).toFixed(1)}%`;
}

async function loadData() {
  const [latestRes, indexRes, historiesRes] = await Promise.all([
    fetch("data/latest.json"),
    fetch("data/index.json"),
    fetch("data/histories.json"),
  ]);

  const latest = await latestRes.json();
  const indexData = await indexRes.json();
  histories = await historiesRes.json();

  renderHero(indexData);
  renderIndexChart(indexData);
  renderTables(latest);
}

function renderHero(indexData) {
  const latest = indexData[indexData.length - 1];
  document.getElementById("heroIndex").textContent = Number(latest.index_level).toFixed(1);
}

function renderIndexChart(indexData) {
  const ctx = document.getElementById("indexChart").getContext("2d");
  indexChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: indexData.map(d => d.date.slice(0, 7)),
      datasets: [{
        label: "Headline Index",
        data: indexData.map(d => d.index_level),
      }]
    },
    options: { responsive: true, maintainAspectRatio: false }
  });
}

function makeRow(row) {
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${row.region_name}</td>
    <td>${fmtPct(row.yoy_pct)}</td>
    <td>${fmtPct(row.mom_pct)}</td>
    <td>${Number(row.trend_score).toFixed(2)}</td>
  `;
  tr.addEventListener("click", () => renderRegion(row.region_id, row.region_name));
  return tr;
}

function renderTables(latest) {
  const brightBody = document.querySelector("#brightTable tbody");
  const dimBody = document.querySelector("#dimTable tbody");

  const bright = [...latest].sort((a, b) => b.trend_score - a.trend_score).slice(0, 5);
  const dim = [...latest].sort((a, b) => a.trend_score - b.trend_score).slice(0, 5);

  bright.forEach(r => brightBody.appendChild(makeRow(r)));
  dim.forEach(r => dimBody.appendChild(makeRow(r)));

  if (bright.length > 0) {
    renderRegion(bright[0].region_id, bright[0].region_name);
  }
}

function renderRegion(regionId, regionName) {
  const rows = histories[String(regionId)] || [];
  const ctx = document.getElementById("regionChart").getContext("2d");

  if (regionChart) regionChart.destroy();

  regionChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: rows.map(r => r.date.slice(0, 7)),
      datasets: [{
        label: `${regionName} light density`,
        data: rows.map(r => r.light_density),
      }]
    },
    options: { responsive: true, maintainAspectRatio: false }
  });

  document.getElementById("regionTitle").textContent = `Region Explorer — ${regionName}`;
}

loadData();