let homepageSummary = {};
let profileIndex = [];
let stateLatest = [];
let countyLatest = [];
let nowcasts = {};
let currentMapMode = "states";

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

function showStatus(msg) {
  const mount = document.getElementById("statusMount");
  if (!mount) return;
  mount.innerHTML = `<div class="status-card"><strong>Status:</strong> ${msg}</div>`;
}

function renderMiniList(id, rows) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = "";

  (rows || []).slice(0, 5).forEach(row => {
    const yoy = row.yoy_pct_display ?? row.yoy_pct;
    const li = document.createElement("li");
    li.innerHTML = `
      <span>${row.region_name}</span>
      <span class="${valueClass(yoy)}">${fmtPct(yoy)}</span>
    `;
    el.appendChild(li);
  });
}

function renderNowcasts(nowcastPayload) {
  const box = document.getElementById("nowcastCards");
  if (!box) return;
  box.innerHTML = "";

  const rows = [
    ...(nowcastPayload?.metros?.top_employment_nowcasts || []).slice(0, 3),
    ...(nowcastPayload?.states?.top_employment_nowcasts || []).slice(0, 3)
  ].slice(0, 6);

  rows.forEach(r => {
    const card = document.createElement("div");
    card.className = "home-list-card";
    card.innerHTML = `
      <div class="summary-title">${r.confidence || "N/A"} confidence</div>
      <div class="summary-number small">${r.region_name}</div>
      <div class="${valueClass(r.employment_yoy_nowcast)} body-copy">
        Implied employment YoY: ${fmtPct(r.employment_yoy_nowcast)}
      </div>
      <div class="body-copy">Trend: ${r.trend_label || "--"}</div>
      <div class="body-copy">Industry: ${r.industry_proxy || "--"}</div>
      <div class="body-copy">Population proxy: ${r.population_growth_proxy == null ? "N/A" : fmtPct(r.population_growth_proxy)}</div>
    `;
    box.appendChild(card);
  });

  if (rows.length > 0) {
    const topEl = document.getElementById("topNowcastRegion");
    if (topEl) topEl.textContent = rows[0].region_name;
  }
}

function populateGlobalSearch(results) {
  const sel = document.getElementById("globalSearchResults");
  if (!sel) return;

  sel.innerHTML = "";

  results.forEach(r => {
    const opt = document.createElement("option");
    opt.value = `${r.dataset}|${r.region_id}`;
    opt.textContent = `${r.region_name} (${r.dataset})`;
    sel.appendChild(opt);
  });

  sel.onchange = () => {
    const [dataset, id] = sel.value.split("|");
    window.location.href = `profile.html?dataset=${encodeURIComponent(dataset)}&id=${encodeURIComponent(id)}`;
  };
}

function attachGlobalSearch() {
  const input = document.getElementById("globalSearch");
  if (!input) return;

  input.oninput = () => {
    const q = input.value.trim().toLowerCase();
    const matches = profileIndex
      .filter(r => r.region_name.toLowerCase().includes(q))
      .slice(0, 100);
    populateGlobalSearch(matches);
  };

  populateGlobalSearch(profileIndex.slice(0, 50));
}

function fipsToStateAbbr(fips) {
  const map = {
    "01":"AL","02":"AK","04":"AZ","05":"AR","06":"CA","08":"CO","09":"CT","10":"DE","11":"DC","12":"FL","13":"GA",
    "15":"HI","16":"ID","17":"IL","18":"IN","19":"IA","20":"KS","21":"KY","22":"LA","23":"ME","24":"MD","25":"MA",
    "26":"MI","27":"MN","28":"MS","29":"MO","30":"MT","31":"NE","32":"NV","33":"NH","34":"NJ","35":"NM","36":"NY",
    "37":"NC","38":"ND","39":"OH","40":"OK","41":"OR","42":"PA","44":"RI","45":"SC","46":"SD","47":"TN","48":"TX",
    "49":"UT","50":"VT","51":"VA","53":"WA","54":"WV","55":"WI","56":"WY"
  };
  return map[String(fips).padStart(2, "0")] || fips;
}

function setMapMode(mode) {
  currentMapMode = mode;
  document.getElementById("stateMapBtn")?.classList.toggle("active", mode === "states");
  document.getElementById("countyMapBtn")?.classList.toggle("active", mode === "counties");

  const title = document.getElementById("mapTitle");
  const intro = document.getElementById("mapIntroText");

  if (mode === "states") {
    if (title) title.textContent = "U.S. State Economic Activity Map";
    if (intro) {
      intro.textContent =
        "This national map shows state-level light-based economic momentum across the United States, including Alaska and Hawaii. Click a state to open its profile with trend history, ranking, percentile, and nowcast.";
    }
    renderStateMap();
  } else {
    if (title) title.textContent = "U.S. County Economic Activity Map";
    if (intro) {
      intro.textContent =
        "This county map shows more local variation in light-based momentum. Click a county to open its profile. County mode emphasizes detail over simplicity.";
    }
    renderCountyMap();
  }
}

function renderStateMap() {
  const usable = stateLatest.filter(
    r => r.region_id && r.yoy_pct_display !== null && r.yoy_pct_display !== undefined
  );

  const trace = {
    type: "choropleth",
    locationmode: "USA-states",
    locations: usable.map(r => fipsToStateAbbr(r.region_id)),
    z: usable.map(r => Number(r.yoy_pct_display)),
    text: usable.map(r =>
      `${r.region_name}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}<br>Trend: ${r.trend_label || "N/A"}`
    ),
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
      line: { color: "rgba(255,255,255,0.38)", width: 0.9 }
    },
    colorbar: {
      title: "Light YoY",
      tickfont: { color: "#e5ecff" },
      titlefont: { color: "#e5ecff" }
    }
  };

  const layout = {
    geo: {
      scope: "usa",
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

  Plotly.newPlot("countyMap", [trace], layout, {
    responsive: true,
    displayModeBar: false
  });

  const mapEl = document.getElementById("countyMap");
  mapEl.on("plotly_click", (data) => {
    const point = data.points?.[0];
    if (!point) return;
    const abbr = point.location;
    const row = usable.find(r => fipsToStateAbbr(r.region_id) === abbr);
    if (!row) return;
    window.location.href = `profile.html?dataset=states&id=${encodeURIComponent(row.region_id)}`;
  });
}

function renderCountyMap() {
  const usable = countyLatest
    .filter(r => r.region_id && r.trend_score !== null && r.trend_score !== undefined)
    .map(r => ({ ...r, region_id: String(r.region_id).padStart(5, "0") }));

  const trace = {
    type: "choropleth",
    geojson: "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
    featureidkey: "id",
    locations: usable.map(r => r.region_id),
    z: usable.map(r => Number(r.trend_score)),
    text: usable.map(r =>
      `${r.region_name}<br>Trend: ${r.trend_label || "N/A"}<br>Light YoY: ${fmtPct(r.yoy_pct_display)}`
    ),
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
      line: { color: "rgba(255,255,255,0.08)", width: 0.18 }
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

  Plotly.newPlot("countyMap", [trace], layout, {
    responsive: true,
    displayModeBar: false
  });

  const mapEl = document.getElementById("countyMap");
  mapEl.on("plotly_click", (data) => {
    const point = data.points?.[0];
    if (!point) return;
    const fips = String(point.location).padStart(5, "0");
    window.location.href = `profile.html?dataset=counties&id=${encodeURIComponent(fips)}`;
  });
}

async function init() {
  try {
    const [
      home,
      profileIdx,
      states,
      counties,
      nowcastPayload
    ] = await Promise.all([
      loadJson("data/homepage_summary.json"),
      loadJson("data/profile_index.json"),
      loadJson("data/states_latest.json"),
      loadJson("data/counties_latest.json"),
      loadJson("data/v2_nowcasts.json").catch(() => ({}))
    ]);

    homepageSummary = home;
    profileIndex = profileIdx;
    stateLatest = states;
    countyLatest = counties;
    nowcasts = nowcastPayload;

    const latestMonthEl = document.getElementById("homeLatestMonth");
    if (latestMonthEl) {
      latestMonthEl.textContent =
        homepageSummary?.metros?.summary?.latest_month
          ? `Latest available month: ${String(homepageSummary.metros.summary.latest_month).slice(0, 7)}`
          : "Latest available month: --";
    }

    const national = homepageSummary?.counties?.summary?.national_yoy_pct;
    const usLightYoy = document.getElementById("usLightYoy");
    const nationalLightYoy = document.getElementById("nationalLightYoy");

    if (usLightYoy) {
      usLightYoy.textContent = fmtPct(national);
      usLightYoy.className = `stat-value ${valueClass(national)}`;
    }

    if (nationalLightYoy) {
      nationalLightYoy.textContent = fmtPct(national);
      nationalLightYoy.className = `summary-number ${valueClass(national)}`;
    }

    const metroSummary = homepageSummary?.metros?.summary || {};
    const avgMom = document.getElementById("avgMom");
    const avgYoy = document.getElementById("avgYoy");
    const topTrendText = document.getElementById("topTrendText");

    if (avgMom) {
      avgMom.textContent = fmtPct(metroSummary.avg_mom);
      avgMom.className = `summary-number ${valueClass(metroSummary.avg_mom)}`;
    }

    if (avgYoy) {
      avgYoy.textContent = fmtPct(metroSummary.avg_yoy);
      avgYoy.className = `summary-number ${valueClass(metroSummary.avg_yoy)}`;
    }

    if (topTrendText) {
      const tr = metroSummary.top_region;
      if (tr?.region_name) {
        const yoy = tr.yoy_pct_display ?? tr.yoy_pct;
        topTrendText.innerHTML = `${tr.region_name} <span class="${valueClass(yoy)}">${fmtPct(yoy)}</span>`;
      } else {
        topTrendText.textContent = "--";
      }
    }

    renderMiniList("top5Metros", homepageSummary?.metros?.top5 || []);
    renderMiniList("bottom5Metros", homepageSummary?.metros?.bottom5 || []);
    renderMiniList("top5States", homepageSummary?.states?.top5 || []);
    renderMiniList("bottom5States", homepageSummary?.states?.bottom5 || []);
    renderMiniList("top5Counties", homepageSummary?.counties?.top5 || []);
    renderMiniList("bottom5Counties", homepageSummary?.counties?.bottom5 || []);
    renderMiniList("top5Cities", homepageSummary?.cities?.top5 || []);
    renderMiniList("bottom5Cities", homepageSummary?.cities?.bottom5 || []);

    renderNowcasts(nowcasts);
    attachGlobalSearch();

    document.getElementById("stateMapBtn")?.addEventListener("click", () => setMapMode("states"));
    document.getElementById("countyMapBtn")?.addEventListener("click", () => setMapMode("counties"));

    setMapMode("states");
    showStatus("Loaded homepage data.");
  } catch (err) {
    console.error(err);
    showStatus(`Error loading homepage data: ${err.message}`);
  }
}

init();