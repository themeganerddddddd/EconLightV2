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

function renderMiniList(id, rows) {
  const el = document.getElementById(id);
  el.innerHTML = "";
  (rows || []).slice(0, 5).forEach(row => {
    const yoy = row.yoy_pct_display ?? row.yoy_pct;
    const li = document.createElement("li");
    li.innerHTML = `<span>${row.region_name}</span><span class="${valueClass(yoy)}">${fmtPct(yoy)}</span>`;
    el.appendChild(li);
  });
}

function renderNowcasts(nowcasts) {
  const box = document.getElementById("nowcastCards");
  box.innerHTML = "";

  const rows = [
    ...(nowcasts?.metros?.top_employment_nowcasts || []).slice(0, 3),
    ...(nowcasts?.states?.top_employment_nowcasts || []).slice(0, 3),
  ].slice(0, 6);

  rows.forEach(r => {
    const card = document.createElement("div");
    card.className = "home-list-card";
    card.innerHTML = `
      <div class="summary-title">${r.confidence} confidence</div>
      <div class="summary-number small">${r.region_name}</div>
      <div class="${valueClass(r.employment_yoy_nowcast)} body-copy">Implied employment YoY: ${fmtPct(r.employment_yoy_nowcast)}</div>
      <div class="body-copy">Trend: ${r.trend_label}</div>
    `;
    box.appendChild(card);
  });

  if (rows.length > 0) {
    document.getElementById("topNowcastRegion").textContent = rows[0].region_name;
  }
}

function renderCountyMap(countyRows) {
  const usable = countyRows.filter(r => r.region_id && r.trend_score !== null && r.trend_score !== undefined);

  const trace = {
    type: "choropleth",
    geojson: "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
    featureidkey: "id",
    locations: usable.map(r => String(r.region_id).padStart(5, "0")),
    z: usable.map(r => Number(r.trend_score)),
    text: usable.map(r => `${r.region_name}<br>Trend: ${r.trend_label || "N/A"}`),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0.0, "#fb7185"],
      [0.35, "#f59e0b"],
      [0.5, "#cbd5e1"],
      [0.65, "#60a5fa"],
      [1.0, "#4ade80"]
    ],
    zmid: 0,
    marker: { line: { color: "rgba(255,255,255,0.08)", width: 0.1 } },
    colorbar: { title: "Trend" }
  };

  const layout = {
    geo: { scope: "usa", bgcolor: "rgba(0,0,0,0)", showlakes: false, showland: true },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 0, r: 0, b: 0, l: 0 },
    font: { color: "#e5ecff" }
  };

  Plotly.newPlot("countyMap", [trace], layout, { responsive: true, displayModeBar: false });
}

async function init() {
  const [homepageSummary, countyLatest, nowcasts] = await Promise.all([
    loadJson("data/homepage_summary.json"),
    loadJson("data/counties_latest.json"),
    loadJson("data/v2_nowcasts.json").catch(() => ({}))
  ]);

  document.getElementById("homeLatestMonth").textContent =
    homepageSummary?.metros?.summary?.latest_month
      ? `Latest available month: ${String(homepageSummary.metros.summary.latest_month).slice(0, 7)}`
      : "Latest available month: --";

  const national = homepageSummary?.counties?.summary?.national_yoy_pct;
  document.getElementById("usLightYoy").textContent = fmtPct(national);
  document.getElementById("usLightYoy").className = `stat-value ${valueClass(national)}`;

  renderMiniList("top5Metros", homepageSummary?.metros?.top5 || []);
  renderMiniList("bottom5Metros", homepageSummary?.metros?.bottom5 || []);
  renderMiniList("top5States", homepageSummary?.states?.top5 || []);
  renderMiniList("bottom5States", homepageSummary?.states?.bottom5 || []);
  renderMiniList("top5Counties", homepageSummary?.counties?.top5 || []);
  renderMiniList("bottom5Counties", homepageSummary?.counties?.bottom5 || []);
  renderMiniList("top5Cities", homepageSummary?.cities?.top5 || []);
  renderMiniList("bottom5Cities", homepageSummary?.cities?.bottom5 || []);

  renderCountyMap(countyLatest);
  renderNowcasts(nowcasts);
}

init().catch(err => console.error(err));