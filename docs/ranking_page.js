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
}

init().catch(console.error);