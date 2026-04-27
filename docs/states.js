let statesLatest = [];
let countiesLatest = [];
let citiesLatest = [];

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

function getStateId() {
  const params = new URLSearchParams(window.location.search);
  return params.get("id") || "";
}

function populateStateSelect(rows, selectedId) {
  const sel = document.getElementById("stateSelect");
  sel.innerHTML = "";

  rows.forEach(r => {
    const opt = document.createElement("option");
    opt.value = String(r.region_id).padStart(2, "0");
    opt.textContent = r.region_name;
    if (String(r.region_id).padStart(2, "0") === selectedId) opt.selected = true;
    sel.appendChild(opt);
  });

  sel.onchange = () => {
    window.location.href = `states.html?id=${encodeURIComponent(sel.value)}`;
  };
}

function renderStateSummary(row) {
  document.getElementById("stateName").textContent = row.region_name;
  document.getElementById("stateYoy").textContent = fmtPct(row.yoy_pct_display);
  document.getElementById("stateYoy").className = `summary-number ${valueClass(row.yoy_pct_display)}`;
  document.getElementById("stateTrend").textContent = row.trend_label || "--";
  document.getElementById("stateLatestDate").textContent = row.date ? String(row.date).slice(0, 7) : "--";
}

function fillList(id, rows, dataset) {
  const el = document.getElementById(id);
  el.innerHTML = "";
  rows.forEach(r => {
    const li = document.createElement("li");
    li.innerHTML = `<span>${r.region_name}</span><span class="${valueClass(r.yoy_pct_display)}">${fmtPct(r.yoy_pct_display)}</span>`;
    li.style.cursor = "pointer";
    li.onclick = () => {
      window.location.href = `profile.html?dataset=${encodeURIComponent(dataset)}&id=${encodeURIComponent(r.region_id)}`;
    };
    el.appendChild(li);
  });
}

async function init() {
  const [states, counties, cities] = await Promise.all([
    loadJson("data/states_latest.json"),
    loadJson("data/counties_latest.json"),
    loadJson("data/cities_latest.json")
  ]);

  statesLatest = states.map(r => ({ ...r, region_id: String(r.region_id).padStart(2, "0") }));
  countiesLatest = counties.map(r => ({ ...r, region_id: String(r.region_id).padStart(5, "0") }));
  citiesLatest = cities.map(r => ({ ...r, region_id: String(r.region_id).padStart(7, "0") }));

  let stateId = getStateId();
  if (!stateId && statesLatest.length) stateId = statesLatest[0].region_id;

  populateStateSelect(statesLatest, stateId);

  const stateRow = statesLatest.find(r => r.region_id === stateId);
  if (!stateRow) return;

  renderStateSummary(stateRow);

  const topCounties = countiesLatest
    .filter(r => r.region_id.slice(0, 2) === stateId)
    .sort((a, b) => (b.trend_score ?? -999) - (a.trend_score ?? -999))
    .slice(0, 10);

  const topCities = citiesLatest
    .filter(r => r.region_id.slice(0, 2) === stateId)
    .sort((a, b) => (b.trend_score ?? -999) - (a.trend_score ?? -999))
    .slice(0, 10);

  fillList("stateTopCounties", topCounties, "counties");
  fillList("stateTopCities", topCities, "cities");
}

init().catch(console.error);