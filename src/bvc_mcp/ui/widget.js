const els = {
  shell: document.getElementById("app-shell"),
  content: document.getElementById("content"),
  eyebrow: document.getElementById("result-eyebrow"),
  title: document.getElementById("result-title"),
  note: document.getElementById("result-note"),
  badge: document.getElementById("result-badge"),
  body: document.getElementById("result-body"),
};

const state = {
  currentMode: null,
  lastAppliedSignature: "",
  pendingModel: null,
  pendingTimer: null,
  recentSignatures: new Map(),
  lastSearchRows: [],
};

const MODE_ALIASES = {
  inline: ["inline"],
  pip: ["picture-in-picture", "pip", "picture_in_picture"],
  fullscreen: ["fullscreen", "full-screen", "expanded"],
};

const RENDER_PRIORITY = {
  generic: 1,
  search: 1,
  list: 2,
  market_status: 2,
  watchlist_list: 2,
  stock: 3,
  watchlist_detail: 3,
  watchlist_create: 4,
  watchlist_add: 4,
  watchlist_remove: 4,
  watchlist_delete: 4,
  market_summary: 5,
  market_breadth: 5,
  history: 5,
  price_evolution: 5,
  analytics: 6,
  dashboard: 7,
  error: 8,
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function tryParseJson(value) {
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function unwrapPayload(payload) {
  if (payload == null) return null;
  if (typeof payload === "string") {
    const parsed = tryParseJson(payload);
    return parsed ?? { text: payload };
  }
  if (Array.isArray(payload)) return payload;
  if (payload.structuredContent !== undefined) return unwrapPayload(payload.structuredContent);
  if (payload.result !== undefined) return unwrapPayload(payload.result);
  if (Array.isArray(payload.content)) {
    const textItem = payload.content.find((item) => item && item.type === "text" && typeof item.text === "string");
    if (textItem) return unwrapPayload(textItem.text);
  }
  return payload;
}

function stableValue(value) {
  if (value == null) return null;
  if (["string", "number", "boolean"].includes(typeof value)) return value;
  if (Array.isArray(value)) return value.map(stableValue);
  if (isPlainObject(value)) {
    return Object.keys(value).sort().reduce((acc, key) => {
      acc[key] = stableValue(value[key]);
      return acc;
    }, {});
  }
  return String(value);
}

function signatureOf(value) {
  try {
    return JSON.stringify(stableValue(value));
  } catch {
    return `${Date.now()}`;
  }
}

function normalizeNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "--";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return new Intl.NumberFormat("en-US").format(value);
    return new Intl.NumberFormat("en-US", {
      minimumFractionDigits: 0,
      maximumFractionDigits: digits,
    }).format(value);
  }
  if (typeof value === "string" && /^-?\d+(\.\d+)?$/.test(value.trim())) {
    return normalizeNumber(Number(value), digits);
  }
  return String(value);
}

function normalizePrice(value) {
  if (value === null || value === undefined || value === "") return "--";
  return normalizeNumber(value, 2);
}

function numberValue(value) {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const cleaned = value.replaceAll(",", "").replaceAll("%", "").replaceAll("MAD", "").trim();
    if (/^-?\d+(\.\d+)?$/.test(cleaned)) return Number(cleaned);
  }
  return null;
}

function cleanText(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replaceAll("search_stocks()", "the stock search")
    .replaceAll("find_stock()", "the stock search")
    .replaceAll("list_watchlists()", "your saved watchlists")
    .replaceAll("get_snapshots_list()", "the available history range")
    .trim();
}

function compactDate(value) {
  if (!value) return "";
  const normalized = String(value).replace(" ", "T");
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return cleanText(value);
  return new Intl.DateTimeFormat("fr-FR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function longDate(value) {
  if (!value) return "";
  const normalized = String(value).replace(" ", "T");
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return cleanText(value);
  return new Intl.DateTimeFormat("fr-FR", {
    weekday: "long",
    day: "numeric",
    month: "long",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function normalizePercent(value) {
  if (value === null || value === undefined || value === "") return "--";
  if (typeof value === "string") {
    const clean = value.trim();
    if (clean.endsWith("%")) return clean.startsWith("+") || clean.startsWith("-") ? clean : `${Number(clean.slice(0, -1)) > 0 ? "+" : ""}${clean}`;
    if (/^-?\d+(\.\d+)?$/.test(clean)) {
      const numeric = Number(clean);
      return `${numeric > 0 ? "+" : ""}${normalizeNumber(numeric, 2)}%`;
    }
    return clean;
  }
  if (typeof value === "number") {
    return `${value > 0 ? "+" : ""}${normalizeNumber(value, 2)}%`;
  }
  return String(value);
}

function variationTone(value) {
  const numeric = numberValue(value);
  if (numeric !== null) {
    if (numeric > 0) return "positive";
    if (numeric < 0) return "negative";
    return "neutral";
  }
  const raw = String(value ?? "").toLowerCase();
  if (raw.includes("overbought")) return "warning";
  if (raw.includes("oversold")) return "positive";
  if (raw.includes("bearish") || raw.includes("error") || raw.includes("invalid") || raw.includes("not found")) return "negative";
  if (raw.includes("auction") || raw.includes("closed") || raw.includes("attention") || raw.includes("insufficient")) return "warning";
  if (raw.includes("open") || raw.includes("saved") || raw.includes("ready") || raw.includes("confirmed") || raw.includes("bullish") || raw.includes("success")) return "positive";
  return "neutral";
}

function marketStateTone(value) {
  const raw = String(value ?? "").toLowerCase();
  if (raw.includes("auction")) return "warning";
  if (raw.includes("closed")) return "warning";
  if (raw.includes("open")) return "positive";
  return variationTone(value);
}

function requestDisplayMode(mode) {
  if (!window.openai?.requestDisplayMode || !mode || state.currentMode === mode) return;
  const aliases = MODE_ALIASES[mode] ?? [mode];
  state.currentMode = mode;

  for (const candidate of aliases) {
    try {
      const attempt = window.openai.requestDisplayMode({ mode: candidate });
      if (attempt && typeof attempt.catch === "function") attempt.catch(() => {});
      return;
    } catch {
      try {
        const attempt = window.openai.requestDisplayMode(candidate);
        if (attempt && typeof attempt.catch === "function") attempt.catch(() => {});
        return;
      } catch {
        // Try the next alias.
      }
    }
  }
}

function badge(label, tone = "neutral") {
  return `<span class="badge" data-tone="${escapeHtml(tone)}">${escapeHtml(label)}</span>`;
}

function statCard(label, value, meta = "", options = {}) {
  const tone = options.tone ?? variationTone(value);
  const featured = options.featured ? " featured" : "";
  return `
    <article class="stat-card${featured}" data-tone="${escapeHtml(tone)}">
      <div class="stat-label">${escapeHtml(label)}</div>
      <div class="stat-value ${options.valueClass || ""}">${escapeHtml(String(value ?? "--"))}</div>
      ${meta ? `<div class="stat-meta">${escapeHtml(meta)}</div>` : ""}
    </article>
  `;
}

function formatMad(value) {
  if (value === null || value === undefined || value === "") return "--";
  return `${normalizeNumber(value, 2)} MAD`;
}

function lineChart(points, options = {}) {
  const cleaned = points
    .map((point) => ({ label: point.label, value: numberValue(point.value), meta: point.meta }))
    .filter((point) => point.value !== null);
  if (cleaned.length < 2) return "";

  const width = 760;
  const height = 240;
  const paddingX = 18;
  const paddingY = 20;
  const values = cleaned.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 1);
  const stepX = (width - paddingX * 2) / (cleaned.length - 1);

  const coords = cleaned.map((point, index) => {
    const x = paddingX + stepX * index;
    const y = height - paddingY - (((point.value - min) / range) * (height - paddingY * 2));
    return { ...point, x, y };
  });

  const path = coords.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const area = `${path} L ${coords[coords.length - 1].x.toFixed(2)} ${(height - paddingY).toFixed(2)} L ${coords[0].x.toFixed(2)} ${(height - paddingY).toFixed(2)} Z`;
  const labels = coords.filter((_, index) => index === 0 || index === coords.length - 1 || index % Math.max(1, Math.floor(coords.length / 4)) === 0);
  const tone = options.tone ?? "accent";
  const formatValue = options.formatValue ?? formatMad;

  return `
    <section class="chart-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">Trend</div>
          <h3>${escapeHtml(options.title || "Price curve")}</h3>
        </div>
        <div class="chart-range">${escapeHtml(formatValue(min))} to ${escapeHtml(formatValue(max))}</div>
      </div>
      <svg class="line-chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${escapeHtml(options.title || "Price curve")}">
        <defs>
          <linearGradient id="areaGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="rgba(96, 230, 194, 0.30)"></stop>
            <stop offset="100%" stop-color="rgba(96, 230, 194, 0.02)"></stop>
          </linearGradient>
        </defs>
        <path d="${area}" fill="url(#areaGradient)"></path>
        <path d="${path}" class="line-chart-path ${escapeHtml(tone)}"></path>
        ${coords.map((point) => `<circle cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="4" class="line-chart-dot ${escapeHtml(tone)}"></circle>`).join("")}
      </svg>
      <div class="chart-labels">
        ${labels.map((point) => `
          <div class="chart-label">
            <span>${escapeHtml(point.label)}</span>
            <strong>${escapeHtml(formatValue(point.value))}</strong>
          </div>
        `).join("")}
      </div>
    </section>
  `;
}

function rowTone(row) {
  return row.tone ?? variationTone(row.secondaryValue ?? row.status ?? row.title);
}

function tableSection(title, rows, options = {}) {
  if (!rows?.length) return "";
  return `
    <section class="table-card">
      <div class="section-head">
        <div>
          <div class="section-kicker">${escapeHtml(options.kicker || "Details")}</div>
          <h3>${escapeHtml(title)}</h3>
        </div>
        ${options.meta ? `<div class="section-meta">${escapeHtml(options.meta)}</div>` : ""}
      </div>
      <div class="table-rows">
        ${rows.map((row) => `
          <article class="table-row">
            <div class="table-cell table-cell-main">
              <div class="table-title">${escapeHtml(row.title ?? "--")}</div>
              ${row.subtitle ? `<div class="table-subtitle">${escapeHtml(row.subtitle)}</div>` : ""}
            </div>
            <div class="table-cell">
              <div class="table-metric tone-${escapeHtml(rowTone(row))}">${escapeHtml(row.secondaryValue ?? "--")}</div>
              ${row.secondaryLabel ? `<div class="table-subtitle">${escapeHtml(row.secondaryLabel)}</div>` : ""}
            </div>
            <div class="table-cell">
              <div class="table-metric ${row.tertiaryTone ? `tone-${escapeHtml(row.tertiaryTone)}` : ""}">${escapeHtml(row.tertiaryValue ?? "--")}</div>
              ${row.tertiaryLabel ? `<div class="table-subtitle">${escapeHtml(row.tertiaryLabel)}</div>` : ""}
            </div>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

function renderShell(model, body) {
  els.shell.dataset.surface = model.surface;
  els.eyebrow.textContent = model.eyebrow;
  els.title.textContent = model.title;
  els.note.textContent = model.note;
  els.badge.textContent = model.badge;
  els.badge.dataset.tone = model.badgeTone;
  els.body.innerHTML = body;
}

function buildHistoryRows(history) {
  return history.map((row) => ({
    title: compactDate(row.fetched_at ?? row.timestamp ?? row.captured_at) || "Snapshot",
    subtitle: longDate(row.fetched_at ?? row.timestamp ?? row.captured_at) || "Recorded point",
    secondaryValue: formatMad(row.price),
    secondaryLabel: "Price",
    tertiaryValue: row.volume_mad != null ? formatMad(row.volume_mad) : "--",
    tertiaryLabel: row.volume_mad != null ? "Volume" : "Availability",
    tone: variationTone(row.price),
  }));
}

function genericRows(items) {
  return items.map((item) => ({
    title: cleanText(item.symbol ?? item.name ?? item.title ?? "Result"),
    subtitle: cleanText(item.name ?? item.libelle ?? item.company ?? ""),
    secondaryValue: cleanText(
      item.variation_display
      ?? item.variation_pct_display
      ?? item.total_variation_display
      ?? (item.average_variation != null ? normalizePercent(item.average_variation) : "")
      ?? (item.score != null ? `${normalizeNumber(item.score)} pts` : "")
      ?? "--"
    ) || "--",
    secondaryLabel: item.score != null ? "Match" : (item.average_variation != null ? "Average move" : "Variation"),
    tertiaryValue: item.price != null
      ? formatMad(item.price)
      : item.current_price != null
        ? formatMad(item.current_price)
        : item.volume_mad != null
          ? formatMad(item.volume_mad)
          : item.stock_count != null
            ? `${normalizeNumber(item.stock_count)} stocks`
            : "--",
    tertiaryLabel: item.price != null || item.current_price != null ? "Price" : (item.volume_mad != null ? "Volume" : "Value"),
    tone: variationTone(item.variation_display ?? item.variation_pct_display ?? item.total_variation_display ?? item.average_variation ?? item.score),
  }));
}

function normalizeModel(data) {
  if (!isPlainObject(data)) {
    return {
      kind: "generic",
      surface: "inline",
      eyebrow: "Market output",
      title: "Result received",
      note: "ChatGPT returned a response without structured market fields.",
      badge: "Ready",
      badgeTone: "positive",
      stats: [statCard("State", "Ready", "No structured widget payload was provided.", { featured: true, tone: "positive" })],
      rows: [],
    };
  }

  if (typeof data.error === "string") {
    return {
      kind: "error",
      surface: "inline",
      eyebrow: "Request issue",
      title: "The market request could not be completed",
      note: cleanText(data.hint) || "The MCP server returned an error for the current request.",
      badge: "Needs attention",
      badgeTone: "negative",
      stats: [statCard("Error", cleanText(data.error), "Review the message and retry from ChatGPT.", { featured: true, tone: "negative", valueClass: "is-tight" })],
      rows: state.lastSearchRows.slice(0, 3),
      rowsTitle: state.lastSearchRows.length ? "Suggested listings" : "",
    };
  }

  if (data.kind === "dashboard_bootstrap") {
    return {
      kind: "dashboard",
      surface: "fullscreen",
      eyebrow: "Command center",
      title: "Casablanca market room",
      note: cleanText(data.timestamp) || "Live exchange intelligence tuned for conversation-first workflows.",
      badge: cleanText(data.status ?? "Ready"),
      badgeTone: marketStateTone(data.status),
      hero: {
        title: "Premium market intelligence for BVC tools",
        copy: "Open a single surface that feels native to ChatGPT: leadership, breadth, watchlists, and signal-first market context without raw payload clutter.",
      },
      stats: [
        statCard("Market state", cleanText(data.status ?? "Ready"), cleanText(data.timestamp ?? "Latest session"), { featured: true, tone: marketStateTone(data.status) }),
        statCard("Breadth", cleanText(data.breadth?.label ?? "--"), `${normalizeNumber(data.breadth?.gainers ?? 0)} gainers / ${normalizeNumber(data.breadth?.losers ?? 0)} losers`),
        statCard("Top gainer", cleanText(data.leaders?.gainer?.symbol ?? "--"), `${normalizePercent(data.leaders?.gainer?.variation)} · ${cleanText(data.leaders?.gainer?.name ?? "")}`, { tone: "positive" }),
        statCard("Most active", cleanText(data.leaders?.volume?.symbol ?? "--"), cleanText(data.leaders?.volume?.value ?? "--") ),
      ],
      rows: Array.isArray(data.watchlist?.rows) ? data.watchlist.rows.map((row) => ({
        title: cleanText(row.symbol),
        subtitle: cleanText(row.name),
        secondaryValue: normalizePercent(row.variation),
        secondaryLabel: "Variation",
        tertiaryValue: cleanText(row.price),
        tertiaryLabel: "Price",
        tone: variationTone(row.variation),
      })) : [],
      rowsTitle: cleanText(data.watchlist?.name ?? "Core basket"),
      rowsKicker: "Watchlist",
    };
  }

  if (data.result_type === "top_gainers") {
    return {
      kind: "list",
      surface: data.count > 6 ? "fullscreen" : "pip",
      eyebrow: "Leaderboard",
      title: "Top gainers",
      note: `${normalizeNumber(data.count)} stocks led the session on the upside.`,
      badge: `${normalizeNumber(data.count)} rows`,
      badgeTone: "positive",
      rows: genericRows(data.gainers || []),
      rowsTitle: "Positive movers",
    };
  }

  if (data.result_type === "top_losers") {
    return {
      kind: "list",
      surface: data.count > 6 ? "fullscreen" : "pip",
      eyebrow: "Leaderboard",
      title: "Top losers",
      note: `${normalizeNumber(data.count)} stocks are under the most pressure.`,
      badge: `${normalizeNumber(data.count)} rows`,
      badgeTone: "negative",
      rows: genericRows(data.losers || []),
      rowsTitle: "Negative movers",
    };
  }

  if (data.result_type === "top_volume") {
    return {
      kind: "list",
      surface: data.count > 6 ? "fullscreen" : "pip",
      eyebrow: "Liquidity",
      title: "Most active stocks",
      note: "Turnover-ranked leaders from the current BVC session.",
      badge: `${normalizeNumber(data.count)} rows`,
      badgeTone: "neutral",
      rows: genericRows(data.top_volume || []),
      rowsTitle: "Volume leaders",
    };
  }

  if (data.result_type === "search_stocks" || data.result_type === "find_stock") {
    const rows = genericRows(data.results || []);
    state.lastSearchRows = rows;
    return {
      kind: "search",
      surface: data.result_type === "find_stock" || data.count <= 5 ? "pip" : "fullscreen",
      eyebrow: data.result_type === "find_stock" ? "Finder" : "Search",
      title: cleanText(data.query ? `Results for \"${data.query}\"` : "Matching listings"),
      note: `${normalizeNumber(data.count)} matching listing${data.count === 1 ? "" : "s"} found on the exchange.`,
      badge: `${normalizeNumber(data.count)} matches`,
      badgeTone: "neutral",
      rows,
      rowsTitle: "Matching listings",
    };
  }

  if (data.stock && isPlainObject(data.stock)) {
    const stock = data.stock;
    return {
      kind: "stock",
      surface: "pip",
      eyebrow: "Equity card",
      title: `${cleanText(stock.symbol ?? "BVC")} · ${cleanText(stock.name ?? stock.libelle ?? "Casablanca listing")}`,
      note: "A fast-read surface for quote, move, session range, and activity.",
      badge: normalizePercent(stock.variation_pct_display ?? stock.variation_display ?? stock.variation),
      badgeTone: variationTone(stock.variation_pct_display ?? stock.variation_display ?? stock.variation),
      stats: [
        statCard("Current price", formatMad(stock.price), cleanText(stock.name ?? "Latest quote"), { featured: true }),
        statCard("Variation", normalizePercent(stock.variation_pct_display ?? stock.variation_display ?? stock.variation), "Session move", { tone: variationTone(stock.variation_pct_display ?? stock.variation_display ?? stock.variation) }),
        statCard("Session range", `${normalizePrice(stock.low)} to ${normalizePrice(stock.high)} MAD`, "Observed low and high"),
        statCard("Volume", formatMad(stock.volume_mad), "Market activity in dirhams"),
      ],
    };
  }

  if (data.result_type === "stock_history" && Array.isArray(data.history)) {
    const chronological = [...data.history].reverse();
    const prices = chronological.map((row) => numberValue(row.price)).filter((value) => value !== null);
    const first = chronological[0];
    const last = chronological[chronological.length - 1];
    const delta = first && last && numberValue(first.price)
      ? (((numberValue(last.price) - numberValue(first.price)) / numberValue(first.price)) * 100)
      : null;
    return {
      kind: "history",
      surface: "fullscreen",
      eyebrow: "History",
      title: `${cleanText(data.symbol)} recent price history`,
      note: `${normalizeNumber(data.data_points)} locally stored snapshots rendered as a precise line view.`,
      badge: `${normalizeNumber(data.data_points)} points`,
      badgeTone: variationTone(delta),
      stats: [
        statCard("Last price", formatMad(last?.price), compactDate(last?.fetched_at ?? last?.timestamp), { featured: true }),
        statCard("Period move", normalizePercent(delta), `${formatMad(first?.price)} to ${formatMad(last?.price)}`, { tone: variationTone(delta) }),
        statCard("Lowest", formatMad(Math.min(...prices)), "Observed low in the selected history"),
        statCard("Highest", formatMad(Math.max(...prices)), "Observed high in the selected history"),
      ],
      chart: lineChart(chronological.map((row) => ({
        label: compactDate(row.fetched_at ?? row.timestamp ?? row.captured_at),
        value: row.price,
      })), { title: "Recent closes", tone: variationTone(delta) === "negative" ? "negative" : "positive" }),
      rows: buildHistoryRows(chronological.slice(-8).reverse()),
      rowsTitle: "Latest recorded snapshots",
    };
  }

  if (data.result_type === "price_evolution" && Array.isArray(data.data)) {
    const recent = data.data.slice(-10);
    return {
      kind: "price_evolution",
      surface: "fullscreen",
      eyebrow: "Date range",
      title: `${cleanText(data.symbol)} price evolution`,
      note: `${cleanText(data.from)} to ${cleanText(data.to)}`,
      badge: normalizePercent(data.total_variation_display ?? data.total_variation_pct),
      badgeTone: variationTone(data.total_variation_display ?? data.total_variation_pct),
      stats: [
        statCard("Start price", formatMad(data.start_price), cleanText(data.from), { featured: true }),
        statCard("End price", formatMad(data.end_price), cleanText(data.to)),
        statCard("Total move", normalizePercent(data.total_variation_display ?? data.total_variation_pct), `${normalizeNumber(data.data_points)} stored points`, { tone: variationTone(data.total_variation_display ?? data.total_variation_pct) }),
      ],
      chart: lineChart(recent.map((row) => ({ label: compactDate(row.fetched_at ?? row.timestamp ?? row.captured_at), value: row.price })), { title: "Range progression", tone: variationTone(data.total_variation_display ?? data.total_variation_pct) }),
      rows: buildHistoryRows([...recent].reverse()),
      rowsTitle: "Range checkpoints",
    };
  }

  if (data.result_type === "rsi" || data.current_rsi !== undefined || data.current_sma !== undefined || data.volatility_pct !== undefined || data.correlation !== undefined || Array.isArray(data.momentum)) {
    const stats = [];
    if (data.current_rsi !== undefined) {
      stats.push(statCard("Current RSI", normalizeNumber(data.current_rsi), cleanText(data.interpretation ?? "Relative strength"), { featured: true, tone: variationTone(data.interpretation) }));
      stats.push(statCard("Signal", cleanText(data.interpretation ?? "neutral"), `Period ${normalizeNumber(data.period ?? "--")}`, { tone: variationTone(data.interpretation) }));
    }
    if (data.current_sma !== undefined) stats.push(statCard("Moving average", formatMad(data.current_sma), cleanText(data.signal ?? "Computed average")));
    if (data.support !== undefined) stats.push(statCard("Support / resistance", `${normalizePrice(data.support)} / ${normalizePrice(data.resistance)} MAD`, "Detected levels"));
    if (data.volatility_pct !== undefined) stats.push(statCard("Volatility", normalizePercent(data.volatility_pct), `Vs market ${normalizePercent(data.market_avg_pct)}`, { tone: variationTone(data.volatility_pct) }));
    if (data.correlation !== undefined) stats.push(statCard("Correlation", normalizeNumber(data.correlation), cleanText(data.interpretation ?? "Pair relationship")));
    if (!stats.length) stats.push(statCard("Indicator", cleanText(data.signal ?? data.interpretation ?? "Ready"), "Analytics payload available.", { featured: true }));

    const series = Array.isArray(data.data) ? data.data.filter((row) => row.rsi !== null || row.sma !== null || row.price !== null) : [];
    const chartField = data.current_rsi !== undefined ? "rsi" : (data.current_sma !== undefined ? "sma" : "price");
    const chartTitle = data.current_rsi !== undefined ? "RSI curve" : "Indicator curve";
    const rows = Array.isArray(data.momentum)
      ? data.momentum.map((row) => ({
          title: `${normalizeNumber(row.period)} snapshots`,
          subtitle: "Lookback window",
          secondaryValue: normalizePercent(row.momentum_pct_display ?? row.momentum_pct),
          secondaryLabel: "Momentum",
          tertiaryValue: cleanText(row.signal ?? "--"),
          tertiaryLabel: "Signal",
          tone: variationTone(row.momentum_pct_display ?? row.momentum_pct),
        }))
      : series.slice(-8).reverse().map((row) => ({
          title: compactDate(row.fetched_at ?? row.timestamp ?? row.captured_at),
          subtitle: "Recorded value",
          secondaryValue: data.current_rsi !== undefined ? normalizeNumber(row.rsi) : formatMad(row.price),
          secondaryLabel: data.current_rsi !== undefined ? "RSI" : "Price",
          tertiaryValue: row.price != null ? formatMad(row.price) : "--",
          tertiaryLabel: "Underlying price",
          tone: data.current_rsi !== undefined ? variationTone(data.interpretation) : "neutral",
        }));

    return {
      kind: "analytics",
      surface: data.ui_mode_preference || (series.length > 8 || rows.length > 5 ? "fullscreen" : "pip"),
      eyebrow: "Analytics",
      title: data.current_rsi !== undefined ? `${cleanText(data.symbol)} RSI dashboard` : `${cleanText(data.symbol ?? "BVC")} indicator dashboard`,
      note: cleanText(data.interpretation ?? data.signal ?? "Indicator computed from stored historical data."),
      badge: cleanText(data.interpretation ?? data.signal ?? "Ready"),
      badgeTone: variationTone(data.interpretation ?? data.signal),
      stats,
      chart: series.length > 1 ? lineChart(series.map((row) => ({
        label: compactDate(row.fetched_at ?? row.timestamp ?? row.captured_at),
        value: row[chartField] ?? row.price,
      })), {
        title: chartTitle,
        tone: data.current_rsi !== undefined ? "accent" : "positive",
        formatValue: data.current_rsi !== undefined ? ((value) => normalizeNumber(value, 2)) : formatMad,
      }) : "",
      rows,
      rowsTitle: Array.isArray(data.momentum) ? "Momentum windows" : "Latest observations",
    };
  }

  if (data.result_type === "watchlist_list") {
    return {
      kind: "watchlist_list",
      surface: data.count > 4 ? "pip" : "inline",
      eyebrow: "Watchlists",
      title: "Saved watchlists",
      note: `${normalizeNumber(data.count)} persistent basket${data.count === 1 ? "" : "s"} available across sessions.`,
      badge: `${normalizeNumber(data.count)} lists`,
      badgeTone: "positive",
      rows: (data.watchlists || []).map((row) => ({
        title: cleanText(row.name),
        subtitle: longDate(row.created_at),
        secondaryValue: `${normalizeNumber(row.stock_count)} symbols`,
        secondaryLabel: "Coverage",
        tertiaryValue: compactDate(row.created_at),
        tertiaryLabel: "Created",
        tone: "positive",
      })),
      rowsTitle: "Persistent baskets",
    };
  }

  if (data.result_type === "watchlist_detail") {
    return {
      kind: "watchlist_detail",
      surface: data.count > 5 ? "fullscreen" : "pip",
      eyebrow: "Watchlist",
      title: cleanText(data.name),
      note: "Live monitoring view for your saved basket.",
      badge: `${normalizeNumber(data.count)} symbols`,
      badgeTone: "positive",
      stats: [
        statCard("Basket", cleanText(data.name), `${normalizeNumber(data.count)} tracked symbols`, { featured: true, tone: "positive" }),
        statCard("Session", longDate(data.session_timestamp), "Latest live quote refresh"),
      ],
      rows: (data.stocks || []).map((stock) => ({
        title: cleanText(stock.symbol),
        subtitle: cleanText(stock.name ?? stock.error ?? "Unavailable in live data"),
        secondaryValue: stock.error ? "Unavailable" : normalizePercent(stock.variation_display ?? stock.variation),
        secondaryLabel: stock.error ? "State" : "Variation",
        tertiaryValue: stock.error ? cleanText(stock.error) : formatMad(stock.price),
        tertiaryLabel: stock.error ? "Issue" : "Price",
        tone: stock.error ? "warning" : variationTone(stock.variation_display ?? stock.variation),
      })),
      rowsTitle: "Tracked symbols",
    };
  }

  if (data.result_type === "watchlist_create") {
    return {
      kind: "watchlist_create",
      surface: "inline",
      eyebrow: "Watchlist created",
      title: cleanText(data.name),
      note: cleanText(data.message || "Watchlist saved successfully."),
      badge: "Saved",
      badgeTone: "positive",
      stats: [
        statCard("Confirmed symbols", `${normalizeNumber(data.confirmed_count)} symbols`, (data.confirmed_symbols || []).join(", "), { featured: true, tone: "positive" }),
        statCard("Rejected symbols", `${normalizeNumber(data.rejected_count ?? 0)} rejected`, (data.rejected || []).join(", ") || "All requested symbols were accepted.", { tone: (data.rejected_count ?? 0) > 0 ? "warning" : "neutral" }),
      ],
    };
  }

  if (data.result_type === "watchlist_add") {
    return {
      kind: "watchlist_add",
      surface: "inline",
      eyebrow: "Symbol added",
      title: cleanText(data.watchlist),
      note: cleanText(data.message || "The symbol was added to the watchlist."),
      badge: "Updated",
      badgeTone: "positive",
      stats: [
        statCard("Added symbol", cleanText(data.symbol), `Now tracking ${normalizeNumber(data.confirmed_count)} symbols`, { featured: true, tone: "positive" }),
        statCard("Current basket", (data.confirmed_symbols || []).join(", "), "Persistent watchlist membership", { tone: "neutral", valueClass: "is-tight" }),
      ],
    };
  }

  if (data.result_type === "watchlist_remove") {
    return {
      kind: "watchlist_remove",
      surface: "inline",
      eyebrow: "Symbol removed",
      title: cleanText(data.watchlist),
      note: cleanText(data.message || "The symbol was removed from the watchlist."),
      badge: "Updated",
      badgeTone: "warning",
      stats: [
        statCard("Removed symbol", cleanText(data.removed_symbol ?? data.symbol), `${normalizeNumber(data.confirmed_count)} symbols remain`, { featured: true, tone: "warning" }),
        statCard("Remaining basket", (data.confirmed_symbols || []).join(", ") || "Watchlist is now empty.", "Current persisted members", { tone: "neutral", valueClass: "is-tight" }),
      ],
    };
  }

  if (data.result_type === "watchlist_delete") {
    return {
      kind: "watchlist_delete",
      surface: "inline",
      eyebrow: "Watchlist deleted",
      title: cleanText(data.watchlist),
      note: cleanText(data.message || "The watchlist was deleted."),
      badge: "Deleted",
      badgeTone: "negative",
      stats: [
        statCard("Deleted basket", cleanText(data.watchlist), `${normalizeNumber(data.deleted_count ?? 0)} symbol${data.deleted_count === 1 ? "" : "s"} removed from persistence`, { featured: true, tone: "negative" }),
      ],
    };
  }

  if (Array.isArray(data.gainers) || Array.isArray(data.losers) || Array.isArray(data.top_volume) || Array.isArray(data.breakout_candidates) || Array.isArray(data.top_performers) || Array.isArray(data.worst_performers) || Array.isArray(data.unusual_activity) || Array.isArray(data.stocks) || Array.isArray(data.segments) || Array.isArray(data.performance)) {
    const source = data.gainers || data.losers || data.top_volume || data.breakout_candidates || data.top_performers || data.worst_performers || data.unusual_activity || data.performance || data.stocks || data.segments || [];
    return {
      kind: "list",
      surface: source.length > 6 ? "fullscreen" : "pip",
      eyebrow: "Market set",
      title: "Market results",
      note: `${normalizeNumber(source.length)} rows returned from the latest BVC query.`,
      badge: `${normalizeNumber(source.length)} rows`,
      badgeTone: "neutral",
      rows: genericRows(source),
      rowsTitle: "Returned rows",
    };
  }

  if (data.tradeable_count !== undefined || data.is_open !== undefined) {
    return {
      kind: "market_status",
      surface: "inline",
      eyebrow: "Market state",
      title: "Casablanca market status",
      note: longDate(data.timestamp_french || data.timestamp) || "Latest exchange status.",
      badge: cleanText(data.status || (data.is_open ? "Open" : "Closed")),
      badgeTone: marketStateTone(data.status || (data.is_open ? "Open" : "Closed")),
      stats: [
        statCard("Current state", cleanText(data.status || (data.is_open ? "Open" : "Closed")), `${normalizeNumber(data.tradeable_count)} tradeable listings`, { featured: true, tone: marketStateTone(data.status || (data.is_open ? "Open" : "Closed")) }),
        statCard("Coverage", `${normalizeNumber(data.tradeable_count)} / ${normalizeNumber(data.total_count)}`, "Tradeable versus total listings"),
      ],
    };
  }

  if (data.market_state !== undefined || data.total_volume_mad !== undefined || data.top_gainer !== undefined) {
    return {
      kind: "market_summary",
      surface: "fullscreen",
      eyebrow: "Market overview",
      title: "BVC session summary",
      note: longDate(data.session_timestamp_french || data.session_timestamp || data.timestamp_french || data.timestamp) || "Latest market snapshot.",
      badge: cleanText(data.market_state || "Live market"),
      badgeTone: marketStateTone(data.market_state),
      stats: [
        statCard("Market state", cleanText(data.market_state || "--"), `${normalizeNumber(data.tradeable || data.tradeable_count || 0)} tradeable listings`, { featured: true, tone: marketStateTone(data.market_state) }),
        statCard("Breadth", `${normalizeNumber(data.gainers || 0)} / ${normalizeNumber(data.losers || 0)}`, `${normalizeNumber(data.unchanged || 0)} unchanged`),
        statCard("Total volume", formatMad(data.total_volume_mad), "Turnover in Moroccan dirhams"),
        statCard("Top gainer", cleanText(data.top_gainer?.symbol || "--"), `${normalizePercent(data.top_gainer?.variation_display || data.top_gainer?.variation)} · ${cleanText(data.top_gainer?.name || "")}`, { tone: "positive" }),
        statCard("Top loser", cleanText(data.top_loser?.symbol || "--"), `${normalizePercent(data.top_loser?.variation_display || data.top_loser?.variation)} · ${cleanText(data.top_loser?.name || "")}`, { tone: "negative" }),
        statCard("Most active", cleanText(data.top_volume?.symbol || "--"), `${formatMad(data.top_volume?.volume_mad)} · ${cleanText(data.top_volume?.name || "")}`),
      ],
    };
  }

  return {
    kind: "generic",
    surface: "inline",
    eyebrow: "Market output",
    title: "Structured result received",
    note: "The response was parsed, but it did not match a dedicated premium view yet.",
    badge: "Available",
    badgeTone: "neutral",
    stats: Object.entries(data).slice(0, 4).map(([key, value], index) => {
      let display = "Structured";
      if (["string", "number", "boolean"].includes(typeof value)) display = String(value);
      if (Array.isArray(value)) display = `${value.length} items`;
      return statCard(key.replaceAll("_", " "), display, index === 0 ? "Primary field" : "Additional field", { featured: index === 0 });
    }),
  };
}

function renderModel(model) {
  requestDisplayMode(model.surface);

  let body = "";
  if (model.hero) {
    body += `
      <section class="hero-panel">
        <div class="hero-copy-wrap">
          <div class="section-kicker">Workspace</div>
          <h3>${escapeHtml(model.hero.title)}</h3>
          <p>${escapeHtml(model.hero.copy)}</p>
        </div>
      </section>
    `;
  }

  if (model.stats?.length) {
    body += `<section class="stats-grid">${model.stats.join("")}</section>`;
  }
  if (model.chart) {
    body += model.chart;
  }
  if (model.rows?.length) {
    body += tableSection(model.rowsTitle || "Rows", model.rows, { kicker: model.rowsKicker || "Details" });
  }

  renderShell(model, body);
}

function shouldSkip(signature) {
  const now = Date.now();
  for (const [key, timestamp] of state.recentSignatures.entries()) {
    if (now - timestamp > 12000) state.recentSignatures.delete(key);
  }
  if (state.recentSignatures.has(signature)) return true;
  state.recentSignatures.set(signature, now);
  return false;
}

function scheduleRender(payload) {
  const data = unwrapPayload(payload);
  const model = normalizeModel(data);
  const signature = signatureOf({
    kind: model.kind,
    title: model.title,
    note: model.note,
    badge: model.badge,
    rows: model.rows,
    stats: model.stats,
  });

  if (signature === state.lastAppliedSignature || shouldSkip(signature)) return;

  if (state.pendingModel) {
    const pendingPriority = RENDER_PRIORITY[state.pendingModel.kind] ?? 0;
    const nextPriority = RENDER_PRIORITY[model.kind] ?? 0;
    if (nextPriority >= pendingPriority) state.pendingModel = { ...model, signature };
  } else {
    state.pendingModel = { ...model, signature };
  }

  if (state.pendingTimer) window.clearTimeout(state.pendingTimer);
  const delay = model.kind === "search" ? 950 : 420;
  state.pendingTimer = window.setTimeout(() => {
    state.pendingTimer = null;
    if (!state.pendingModel) return;
    const nextModel = state.pendingModel;
    state.pendingModel = null;
    state.lastAppliedSignature = nextModel.signature;
    renderModel(nextModel);
  }, delay);
}

window.addEventListener("message", (event) => {
  if (event.source !== window.parent) return;
  const message = event.data;
  if (!message || message.jsonrpc !== "2.0") return;
  if (message.method !== "ui/notifications/tool-result") return;
  scheduleRender(message.params);
}, { passive: true });

window.addEventListener("openai:set_globals", (event) => {
  const globals = event.detail?.globals ?? {};
  if (globals.toolOutput) scheduleRender({ structuredContent: globals.toolOutput });
}, { passive: true });

if (window.openai?.toolOutput) {
  scheduleRender({ structuredContent: window.openai.toolOutput });
}
