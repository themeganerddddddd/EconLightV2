let nowcastData = {};
let divergenceData = {};
let currentDataset = "metros";

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

function fillTable(tableId, rows, renderer) {
  const body = document.querySelector(`#${tableId} tbody`);
  body.innerHTML = "";
  (rows || []).forEach(row => {
    const tr = document.createElement("tr");
    tr.innerHTML = renderer(row);
    body.appendChild(tr);
  });
}

function renderDataset(dataset) {
  currentDataset = dataset;
  document.querySelectorAll(".dataset-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.dataset === dataset);
  });

  const top = nowcastData?.[dataset]?.top_employment_nowcasts || [];
  const bottom = nowcastData?.[dataset]?.bottom_employment_nowcasts || [];
  const posDiv = divergenceData?.[dataset]?.top_positive_divergence || [];
  const negDiv = divergenceData?.[dataset]?.top_negative_divergence || [];

  const first = top[0];
  document.getElementById("topNowcastName").textContent = first?.region_name || "--";
  document.getElementById("topNowcastValue").textContent = fmtPct(first?.employment_yoy_nowcast);
  document.getElementById("topNowcastValue").className = `summary-number ${valueClass(first?.employment_yoy_nowcast)}`;
  document.getElementById("topNowcastConfidence").textContent = first?.confidence || "--";
  document.getElementById("topIndustryProxy").textContent = first?.industry_proxy || "--";

  fillTable("topNowcastsTable", top.slice(0, 15), (r) => `
    <td>${r.region_name}</td>
    <td class="${valueClass(r.employment_yoy_nowcast)}">${fmtPct(r.employment_yoy_nowcast)}</td>
    <td class="${valueClass(r.labor_force_yoy_nowcast)}">${fmtPct(r.labor_force_yoy_nowcast)}</td>
    <td>${r.confidence}</td>
  `);

  fillTable("bottomNowcastsTable", bottom.slice(0, 15), (r) => `
    <td>${r.region_name}</td>
    <td class="${valueClass(r.employment_yoy_nowcast)}">${fmtPct(r.employment_yoy_nowcast)}</td>
    <td class="${valueClass(r.labor_force_yoy_nowcast)}">${fmtPct(r.labor_force_yoy_nowcast)}</td>
    <td>${r.confidence}</td>
  `);

  fillTable("positiveDivTable", posDiv.slice(0, 15), (r) => `
    <td>${r.region_name}</td>
    <td class="${valueClass(r.divergence_score)}">${fmtPct(r.divergence_score)}</td>
    <td>${fmtPct(r.employment_yoy_nowcast)}</td>
    <td>${fmtPct(r.yoy_pct_display)}</td>
  `);

  fillTable("negativeDivTable", negDiv.slice(0, 15), (r) => `
    <td>${r.region_name}</td>
    <td class="${valueClass(r.divergence_score)}">${fmtPct(r.divergence_score)}</td>
    <td>${fmtPct(r.employment_yoy_nowcast)}</td>
    <td>${fmtPct(r.yoy_pct_display)}</td>
  `);
}

async function init() {
  nowcastData = await loadJson("data/v2_nowcasts.json");
  divergenceData = await loadJson("data/v2_divergence.json");

  document.querySelectorAll(".dataset-btn").forEach(btn => {
    btn.addEventListener("click", () => renderDataset(btn.dataset.dataset));
  });

  renderDataset("metros");
}

init().catch(console.error);