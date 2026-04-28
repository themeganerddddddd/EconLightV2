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

function getDataset() {
  const script = document.currentScript || [...document.scripts].find(s => s.src.includes("ranking_page.js"));
  return script?.dataset?.dataset || "counties";
}

function withRanks(rows) {
  const sorted = [...rows].sort((a, b) => (b.yoy_pct_display ?? -999) - (a.yoy_pct_display ?? -999));
  return sorted.map((r, i) => ({
    ...r,
    rank_overall: i + 1,
    percentile: sorted.length > 1 ? Math.round((1 - i / (sorted.length - 1)) * 1000) / 10 : 100
  }));
}

function fillTable(tableId, rows, dataset, limit = null) {
  const body = document.querySelector(`#${tableId} tbody`);
  if (!body) return;
  body.innerHTML = "";

  const visible = limit ? rows.slice(0, limit) : rows;

  visible.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.rank_overall}</td>
      <td>${r.region_name}</td>
      <td class="${valueClass(r.yoy_pct_display)}">${fmtPct(r.yoy_pct_display)}</td>
      <td>${r.trend_label || "--"}</td>
      <td>${r.percentile}%</td>
    `;
    tr.onclick = () => {
      window.location.href = `profile.html?dataset=${encodeURIComponent(dataset)}&id=${encodeURIComponent(r.region_id)}`;
    };
    body.appendChild(tr);
  });
}

async function renderCountyMap(rows) {
  const mapEl = document.getElementById("countiesMap");
  if (!mapEl) return;

  const geo = await loadJson("data/regions/us_counties_all.geojson");

  const trace = {
    type: "choropleth",
    geojson: geo,
    featureidkey: "properties.region_id",
    locations: rows.map(r => String(r.region_id).padStart(5, "0")),
    z: rows.map(r => Number(r.yoy_pct_display ?? 0)),
    text: rows.map(r => `${r.region_name}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}<br>Rank: ${r.rank_overall}`),
    hovertemplate: "%{text}<extra></extra>",
    colorscale: [
      [0.0, "#fb7185"],
      [0.35, "#f59e0b"],
      [0.5, "#cbd5e1"],
      [0.65, "#60a5fa"],
      [1.0, "#4ade80"]
    ],
    zmid: 0,
    marker: { line: { color: "rgba(255,255,255,0.08)", width: 0.15 } },
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

  Plotly.newPlot("countiesMap", [trace], layout, { responsive: true, displayModeBar: false });

  mapEl.on("plotly_click", data => {
    const point = data.points?.[0];
    if (!point) return;
    const id = String(point.location).padStart(5, "0");
    window.location.href = `profile.html?dataset=counties&id=${encodeURIComponent(id)}`;
  });
}

async function init() {
  const dataset = getDataset();
  const rows = await loadJson(`data/${dataset}_latest.json`);
  const cleanRows = rows.map(r => ({
    ...r,
    region_id:
      dataset === "counties"
        ? String(r.region_id).padStart(5, "0")
        : dataset === "cities"
          ? String(r.region_id).padStart(7, "0")
          : String(r.region_id)
  }));

  const ranked = withRanks(cleanRows);
  fillTable("topTable", ranked, dataset, 50);
  fillTable("fullRankingTable", ranked, dataset, null);

  if (dataset === "counties") {
    renderCountyMap(ranked);
  }
}

init().catch(console.error);