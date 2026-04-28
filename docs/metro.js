let metrosLatest = [];
let metrosGeo = null;

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

function withRanks(rows) {
  const sorted = [...rows].sort((a, b) => (b.yoy_pct_display ?? -999) - (a.yoy_pct_display ?? -999));
  return sorted.map((r, i) => ({
    ...r,
    rank_overall: i + 1,
    percentile: sorted.length > 1 ? Math.round((1 - i / (sorted.length - 1)) * 1000) / 10 : 100
  }));
}

function renderTable(rows) {
  const body = document.querySelector("#metrosTable tbody");
  body.innerHTML = "";

  rows.slice(0, 50).forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.rank_overall}</td>
      <td>${r.region_name}</td>
      <td class="${valueClass(r.yoy_pct_display)}">${fmtPct(r.yoy_pct_display)}</td>
      <td>${r.trend_label || "--"}</td>
      <td>${r.percentile}%</td>
    `;
    tr.onclick = () => {
      window.location.href = `profile.html?dataset=metros&id=${encodeURIComponent(r.region_id)}`;
    };
    body.appendChild(tr);
  });
}

function renderMap(rows) {
  if (!metrosGeo?.features) return;

  const trace = {
    type: "choropleth",
    geojson: metrosGeo,
    featureidkey: "properties.region_id",
    locations: rows.map(r => String(r.region_id)),
    z: rows.map(r => Number(r.yoy_pct_display ?? 0)),
    text: rows.map(r => `${r.region_name}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}<br>Trend: ${r.trend_label || "N/A"}`),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0.0, "#fb7185"],
      [0.35, "#f59e0b"],
      [0.5, "#cbd5e1"],
      [0.65, "#60a5fa"],
      [1.0, "#4ade80"]
    ],
    zmid: 0,
    marker: { line: { color: "rgba(255,255,255,0.10)", width: 0.35 } },
    colorbar: { title: "Light YoY", tickfont: { color: "#e5ecff" }, titlefont: { color: "#e5ecff" } }
  };

  const layout = {
    geo: {
      fitbounds: "locations",
      projection: { type: "albers usa" },
      bgcolor: "rgba(0,0,0,0)",
      showlakes: false,
      showland: true,
      landcolor: "rgba(255,255,255,0.02)"
    },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    margin: { t: 0, r: 0, b: 0, l: 0 },
    font: { color: "#e5ecff" }
  };

  Plotly.newPlot("metrosMap", [trace], layout, { responsive: true, displayModeBar: false });

  document.getElementById("metrosMap").on("plotly_click", data => {
    const point = data.points?.[0];
    if (!point) return;
    const id = String(point.location);
    window.location.href = `profile.html?dataset=metros&id=${encodeURIComponent(id)}`;
  });
}

async function init() {
  const [metros, geo] = await Promise.all([
    loadJson("data/metros_latest.json"),
    loadJson("data/regions/us_metros_all.geojson")
  ]);

  metrosLatest = withRanks(metros.map(r => ({ ...r, region_id: String(r.region_id) })));
  metrosGeo = geo;

  renderMap(metrosLatest);
  renderTable(metrosLatest);
}

init().catch(console.error);