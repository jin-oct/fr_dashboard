const TAB_DEFS = [
  { key: "summary", label: "📰 市場" },
  { key: "jp_stock", label: "🇯🇵 国内株" },
  { key: "us_stock", label: "🇺🇸 米国株" },
  { key: "fx", label: "📊 為替" },
  { key: "crypto", label: "🪙 仮想通貨" },
  { key: "bond", label: "🏦 債券" },
  { key: "watchlist", label: "🏠 監視" }
];

const DATA_FILES = {
  summary: "./data/summary.json",
  jp_stock: "./data/jp_stock.json",
  us_stock: "./data/us_stock.json",
  fx: "./data/fx.json",
  crypto: "./data/crypto.json",
  bond: "./data/bond.json",
  watchlist: "./data/watchlist.json"
};

const FALLBACK_DATA = {
  summary: {
    updated_at: "2026-04-24 00:59 JST",
    market_overview: {
      risk_mode: "Risk Off 寄り",
      macro_tone: "原油高・金利高・ドル高を軸にした相場",
      geopolitical_risk: "high",
      related_assets: {
        oil: "WTI 83.20 (+1.4%)",
        vix: "18.29",
        usdjpy: "159.24",
        fear_greed: "46 (Fear)"
      }
    },
    ai_summary: {
      global: {
        main_theme: "米イラン情勢と原油高で円高方向、159円台前半",
        rise_factors: "原油高、地政学リスク、ボラティリティ上昇",
        fall_factors: "景気懸念、インフレ警戒、金利高止まり",
        risk_on_off: "リスクオフ寄り",
        focus_points: "原油、VIX、USDJPY、米金利",
        jp_swing_hint: "輸出と内需のどちらが優位かを寄り前に確認",
      },
      categories: {
        jp_stock: { label: "日本株", main_theme: "日経平均は原油高と円高警戒で重い", rise_factors: "防衛・資源", fall_factors: "半導体・輸出", focus_points: "日経平均とTOPIXの乖離", jp_swing_hint: "" },
        us_stock: { label: "米国株", main_theme: "金利高がグロースに逆風", rise_factors: "ディフェンシブ", fall_factors: "高PER株", focus_points: "10年債利回り", jp_swing_hint: "" },
        fx: { label: "為替", main_theme: "ドル円は159円台前半でもみ合い", rise_factors: "ドル高要因", fall_factors: "円買い要因", focus_points: "介入警戒", jp_swing_hint: "" },
        crypto: { label: "仮想通貨", main_theme: "BTCはセンチメント悪化でも底堅い", rise_factors: "短期反発", fall_factors: "リスク資産全体の重さ", focus_points: "Fear & Greed", jp_swing_hint: "" },
        bond: { label: "債券", main_theme: "金利高止まりが株に圧力", rise_factors: "短期金利", fall_factors: "長期債価格", focus_points: "米10年", jp_swing_hint: "" }
      }
    },
    news: [
      {
        title: "米イラン情勢と原油高で円高方向、159円台前半",
        summary: "原油高と中東リスクを背景に円買いが入りやすい構図です。ドル円は159円台前半で方向感を探る展開です。",
        sentiment: "negative",
        impact: "high",
        source: "Fallback Desk",
        link: "#",
        published_at: "2026-04-24 00:59 JST",
        tags: ["geopolitics", "scope_macro"],
        risk_level: "high",
        related_assets: ["原油", "VIX", "USDJPY"],
        category: "fx"
      }
    ],
    category_news: {
      jp_stock: [],
      us_stock: [],
      fx: [],
      crypto: [],
      bond: []
    }
  },
  jp_stock: { items: [] },
  us_stock: { items: [] },
  fx: { items: [] },
  crypto: { items: [], top_pumps: [] },
  bond: { items: [] },
  watchlist: { items: [] }
};

function formatNumber(value, digits = 2) {
  return Number(value).toLocaleString("ja-JP", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatPercent(value) {
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatNumber(value, 2)}%`;
}

function toneClassFromNumber(value) {
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

function sentimentLabel(value) {
  if (value === "positive") return "強気";
  if (value === "negative") return "弱気";
  return "中立";
}

function impactLabel(value) {
  if (value === "high") return "影響大";
  if (value === "medium") return "影響中";
  return "影響小";
}

function iconForNews(item) {
  const tags = item.tags || [];
  if (tags.includes("geopolitics")) return "🛰";
  if (tags.includes("oil")) return "🛢";
  if (tags.includes("volatility")) return "📉";
  if (tags.includes("rates")) return "🏦";
  if (tags.includes("crypto")) return "₿";
  return "△";
}

function renderSparkline(points, color) {
  if (!Array.isArray(points) || points.length === 0) {
    return `<div class="empty-state">履歴なし</div>`;
  }
  const width = 320;
  const height = 64;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const spread = max - min || 1;
  const step = width / Math.max(points.length - 1, 1);
  const path = points.map((value, index) => {
    const x = index * step;
    const y = height - ((value - min) / spread) * (height - 10) - 5;
    return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");
  return `
    <svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
      <path d="${path}" fill="none" stroke="${color}" stroke-width="2.2" stroke-linecap="square"></path>
    </svg>
  `;
}

function createTabs() {
  const root = document.getElementById("tabs");
  TAB_DEFS.forEach((tab, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `tab-button${index === 0 ? " is-active" : ""}`;
    button.dataset.tab = tab.key;
    button.textContent = tab.label;
    button.addEventListener("click", () => switchTab(tab.key));
    root.appendChild(button);
  });
}

function switchTab(tabKey) {
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.tab === tabKey);
  });
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.tab === tabKey);
  });
}

function buildRiskCard(summary) {
  const related = summary.market_overview.related_assets || {};
  const news = summary.news || [];
  const lead = news[0];
  const impact = lead ? impactLabel(lead.impact) : "影響中";
  return `
    <div class="risk-top">
      <div>
        <div class="risk-title">地政学リスク / マクロ警戒</div>
        <div class="section-subtitle">${summary.market_overview.macro_tone}</div>
      </div>
      <div class="news-chip status-high">${impact}</div>
    </div>
    <div class="risk-grid">
      <div class="risk-metric"><span>市場ムード</span><strong class="negative">${summary.market_overview.risk_mode}</strong></div>
      <div class="risk-metric"><span>Fear & Greed</span><strong>${related.fear_greed || "N/A"}</strong></div>
      <div class="risk-metric"><span>地政学</span><strong class="status-high">${String(summary.market_overview.geopolitical_risk || "").toUpperCase()}</strong></div>
    </div>
    <div class="risk-linked">
      <span class="metric-chip">VIX ${related.vix || "--"}</span>
      <span class="metric-chip">WTI ${related.oil || "--"}</span>
      <span class="metric-chip">USD/JPY ${related.usdjpy || "--"}</span>
    </div>
    <div class="risk-footer">
      <div>
        <div class="section-title">主要テーマ</div>
        <p>${lead ? lead.title : "主要テーマなし"}</p>
      </div>
      <div class="status-high">${summary.market_overview.geopolitical_risk === "high" ? "HIGH" : "MID"}</div>
    </div>
  `;
}

function buildBriefCard(summary) {
  const global = summary.ai_summary.global;
  return `
    <div class="section-header">
      <div>
        <div class="section-title">TODAY'S BRIEF</div>
        <div class="section-subtitle">市場全体の要点</div>
      </div>
    </div>
    <h2>${global.main_theme}</h2>
    <div class="brief-tags">
      <span class="news-chip negative">↓ ${global.fall_factors}</span>
      <span class="news-chip neutral">${global.risk_on_off}</span>
    </div>
    <p class="brief-copy">${global.focus_points}</p>
  `;
}

function buildPairStrip(summary) {
  const related = summary.market_overview.related_assets || {};
  const chips = [
    `USD/JPY ${related.usdjpy || "--"}`,
    `VIX ${related.vix || "--"}`,
    `WTI ${related.oil || "--"}`,
    `F&G ${related.fear_greed || "--"}`
  ];
  return chips.map((chip, index) => `<span class="asset-chip${index === 0 ? " is-active" : ""}">${chip}</span>`).join("");
}

function buildSummaryQuote(fxData) {
  const item = (fxData.items || [])[0];
  if (!item) return `<div class="empty-state">為替データなし</div>`;
  const changeClass = toneClassFromNumber(item.change_pct);
  return `
    <div class="quote-bid">
      <strong>${item.display_price}</strong>
      <span>/ ${formatNumber(item.price, item.decimals ?? 2)}</span>
    </div>
    <div class="quote-change">
      <span>前日比</span>
      <span class="${changeClass}">${formatPercent(item.change_pct)}</span>
    </div>
    <div class="quote-range">レンジ: ${formatNumber(item.low, item.decimals ?? 2)} - ${formatNumber(item.high, item.decimals ?? 2)}</div>
  `;
}

function todayNote(summary) {
  const global = summary.ai_summary.global;
  return `
    <h3>TODAY'S WATCH</h3>
    <p>${global.jp_swing_hint}</p>
  `;
}

function newsCard(item) {
  const sentimentClass = item.sentiment === "positive" ? "positive" : item.sentiment === "negative" ? "negative" : "neutral";
  const tags = (item.tags || []).slice(0, 3).map((tag) => `<span class="tag">${tag.replace("scope_", "")}</span>`).join("");
  const fxChip = item.category === "fx" && item.fx_bias
    ? `<span class="fx-chip ${item.fx_bias.includes("円高") ? "yen-bullish" : "yen-bearish"}">${item.fx_bias}</span>`
    : "";
  return `
    <article class="news-card">
      <div class="news-header">
        <div class="news-left">
          <div class="news-icon">${iconForNews(item)}</div>
          <div>
            <div class="news-chips">
              <span class="news-chip ${sentimentClass}">${sentimentLabel(item.sentiment)}</span>
              <span class="news-chip">${impactLabel(item.impact)}</span>
              ${fxChip}
            </div>
            <a href="${item.link}" target="_blank" rel="noreferrer"><h3>${item.title}</h3></a>
          </div>
        </div>
      </div>
      <p>${item.summary}</p>
      <div class="news-meta">
        <span>${item.source}</span>
        <span>${item.published_at}</span>
        <span>${(item.related_assets || []).join(" / ")}</span>
      </div>
      <div class="news-tags">${tags}</div>
    </article>
  `;
}

function marketCard(item, type) {
  const template = document.getElementById("marketCardTemplate");
  const node = template.content.firstElementChild.cloneNode(true);
  const changeClass = toneClassFromNumber(item.change_pct || 0);
  const sparkColor = changeClass === "positive" ? "#8af0a7" : changeClass === "negative" ? "#f07f7f" : "#d7b36d";

  node.querySelector(".market-symbol").textContent = item.symbol;
  node.querySelector(".market-name").textContent = item.name;
  node.querySelector(".market-change").textContent = formatPercent(item.change_pct || 0);
  node.querySelector(".market-change").classList.add(changeClass);
  node.querySelector(".market-price").textContent = item.display_price || formatNumber(item.price, item.decimals ?? 2);
  node.querySelector(".market-range").textContent = `高値 ${formatNumber(item.high, item.decimals ?? 2)} / 安値 ${formatNumber(item.low, item.decimals ?? 2)}`;
  node.querySelector(".market-spark").innerHTML = renderSparkline(item.sparkline || [], sparkColor);

  const metaRoot = node.querySelector(".market-meta");
  const chips = [];
  if (type === "crypto") {
    chips.push(`1H ${formatPercent(item.change_1h_pct || 0)}`);
    chips.push(`24H ${formatPercent(item.change_24h_pct || 0)}`);
    chips.push(`VOL ${formatPercent(item.volume_change_pct || 0)}`);
  }
  if (type === "bond" && item.duration) {
    chips.push(`DUR ${item.duration}`);
  }
  metaRoot.innerHTML = chips.map((chip) => `<span class="metric-chip">${chip}</span>`).join("");
  return node;
}

function renderNewsList(rootId, items) {
  const root = document.getElementById(rootId);
  root.innerHTML = "";
  if (!items || items.length === 0) {
    root.innerHTML = `<div class="empty-state">該当ニュースなし</div>`;
    return;
  }
  items.forEach((item) => {
    root.insertAdjacentHTML("beforeend", newsCard(item));
  });
}

function renderCategoryNews(summary, category, rootId) {
  const items = (summary.category_news?.[category] || []).slice(0, 5);
  renderNewsList(rootId, items);
}

function renderMarketGrid(rootId, items, type) {
  const root = document.getElementById(rootId);
  root.innerHTML = "";
  if (!items || items.length === 0) {
    root.innerHTML = `<div class="empty-state">価格データなし</div>`;
    return;
  }
  items.forEach((item) => root.appendChild(marketCard(item, type)));
}

function renderPumpList(items) {
  const root = document.getElementById("pumpList");
  root.innerHTML = "";
  if (!items || items.length === 0) {
    root.innerHTML = `<div class="empty-state">対象なし</div>`;
    return;
  }
  items.forEach((item, index) => {
    const row = document.createElement("div");
    row.className = "pump-row";
    row.innerHTML = `
      <div>
        <div class="market-name">${index + 1}. ${item.symbol}</div>
        <div class="section-subtitle">${item.name}</div>
      </div>
      <div class="positive">score ${formatNumber(item.pump_score, 2)}</div>
    `;
    root.appendChild(row);
  });
}

function renderWatchlist(data) {
  const root = document.getElementById("watchlistGrid");
  root.innerHTML = "";
  const items = data.items || [];
  if (items.length === 0) {
    root.innerHTML = `<div class="empty-state">ウォッチ対象なし</div>`;
    return;
  }
  items.forEach((item) => {
    const article = document.createElement("article");
    article.className = "watchlist-card";
    article.innerHTML = `
      <div class="section-title">${item.symbol}</div>
      <h3>${item.name}</h3>
      <p>${item.thesis || ""}</p>
      <div class="news-tags">
        <span class="tag">${item.category}</span>
        <span class="tag">${item.priority}</span>
      </div>
      <ul>${(item.notes || []).map((note) => `<li>${note}</li>`).join("")}</ul>
    `;
    root.appendChild(article);
  });
}

function renderSummary(summary, fxData) {
  document.getElementById("updatedAtLine").textContent = `最終更新 ${summary.updated_at}`;
  document.getElementById("riskCard").innerHTML = buildRiskCard(summary);
  document.getElementById("globalSummary").innerHTML = buildBriefCard(summary);
  document.getElementById("pairStrip").innerHTML = buildPairStrip(summary);
  document.getElementById("summaryQuote").innerHTML = buildSummaryQuote(fxData);
  document.getElementById("todayNote").innerHTML = todayNote(summary);
  renderNewsList("newsList", (summary.news || []).slice(0, 5));
}

async function loadDashboard() {
  createTabs();

  const fallbackUsed = [];
  const loaded = await Promise.all(
    Object.entries(DATA_FILES).map(async ([key, path]) => {
      try {
        const response = await fetch(path, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return [key, await response.json()];
      } catch (error) {
        fallbackUsed.push(key);
        return [key, FALLBACK_DATA[key]];
      }
    })
  );

  const data = Object.fromEntries(loaded);
  renderSummary(data.summary, data.fx);
  renderCategoryNews(data.summary, "jp_stock", "jpStockNews");
  renderCategoryNews(data.summary, "us_stock", "usStockNews");
  renderCategoryNews(data.summary, "fx", "fxNews");
  renderCategoryNews(data.summary, "crypto", "cryptoNews");
  renderCategoryNews(data.summary, "bond", "bondNews");
  renderMarketGrid("jpStockGrid", data.jp_stock.items || [], "jp_stock");
  renderMarketGrid("usStockGrid", data.us_stock.items || [], "us_stock");
  renderMarketGrid("fxGrid", data.fx.items || [], "fx");
  renderMarketGrid("cryptoGrid", data.crypto.items || [], "crypto");
  renderMarketGrid("bondGrid", data.bond.items || [], "bond");
  renderPumpList(data.crypto.top_pumps || []);
  renderWatchlist(data.watchlist);

  if (fallbackUsed.length > 0) {
    document.getElementById("updatedAtLine").textContent += ` [fallback: ${fallbackUsed.join(", ")}]`;
  }
}

loadDashboard().catch((error) => {
  document.getElementById("views").innerHTML = `<section class="solid-panel"><div class="empty-state">読み込み失敗: ${error.message}</div></section>`;
});
