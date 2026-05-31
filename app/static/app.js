const state = {
  users: [],
  currentUser: "",
  recommendations: [],
  metrics: null,
};

const API_ORIGIN = ["5500", "5501", "5173"].includes(window.location.port)
  ? "http://127.0.0.1:8010"
  : "";

const titles = {
  recommendations: "Recommendations",
  pipeline: "Pipeline",
  about: "About",
};

document.addEventListener("DOMContentLoaded", init);

async function init() {
  try {
    document.getElementById("health-pill").textContent = "JS loaded";
    wireNavigation();
    wireControls();
    await checkHealth();
    await Promise.all([loadUsers(), loadMetrics()]);
    if (state.users.length) {
      state.currentUser = state.users[0];
      document.getElementById("rec-user").value = state.currentUser;
      await loadRecommendations();
    } else {
      showMessage("No users are available in recommendation_users.parquet. Rebuild recommendations first.", "warning");
    }
  } catch (error) {
    showMessage(`Frontend init failed: ${error.message}`, "error");
    const pill = document.getElementById("health-pill");
    pill.textContent = "UI error";
    pill.style.background = "#fee2e2";
    pill.style.color = "#991b1b";
  }
}

function wireNavigation() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".nav-item").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(button.dataset.view).classList.add("active");
      document.getElementById("page-title").textContent = titles[button.dataset.view];
    });
  });
}

function wireControls() {
  document.getElementById("refresh-recs").addEventListener("click", async () => {
    try {
      const value = document.getElementById("rec-user").value.trim();
      if (value) state.currentUser = value;
      await loadRecommendations();
    } catch (error) {
      showMessage(error.message, "error");
    }
  });

  ["category-filter", "brand-filter", "source-filter", "score-filter"].forEach((id) => {
    document.getElementById(id).addEventListener("input", () => {
      document.getElementById("score-label").textContent = Number(document.getElementById("score-filter").value).toFixed(2);
      renderRecommendations();
    });
  });
}

async function api(path, options = {}) {
  const response = await fetch(`${API_ORIGIN}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = data?.detail || response.statusText;
    throw new Error(`${response.status}: ${detail}`);
  }
  return data;
}

async function checkHealth() {
  const pill = document.getElementById("health-pill");
  try {
    const data = await api("/api/health");
    pill.textContent = data.data_loaded ? "API online" : "API warming up";
  } catch (error) {
    pill.textContent = "API unavailable";
    pill.style.background = "#fee2e2";
    pill.style.color = "#991b1b";
  }
}

async function loadUsers() {
  state.users = await api("/api/users?limit=250");
  const options = document.getElementById("user-options");
  options.innerHTML = state.users.map((user) => `<option value="${escapeHtml(user)}"></option>`).join("");
}

async function loadRecommendations() {
  if (!state.currentUser) return;
  showMessage(`Loading recommendations for user ${state.currentUser}...`, "neutral");
  state.recommendations = await api(
    `/api/users/${encodeURIComponent(state.currentUser)}/recommendations?top_k=20`,
  );
  populateFilters(state.recommendations);
  renderRecommendations();
}

async function loadMetrics() {
  state.metrics = await api("/api/model/metrics");
  renderPipeline(state.metrics);
}

function populateFilters(recs) {
  setSelect("category-filter", unique(recs.map((r) => r.category_code)));
  setSelect("brand-filter", unique(recs.map((r) => r.brand)));
  setSelect("source-filter", unique(recs.map((r) => r.source)));
}

function renderRecommendations() {
  const category = document.getElementById("category-filter").value;
  const brand = document.getElementById("brand-filter").value;
  const source = document.getElementById("source-filter").value;
  const minScore = Number(document.getElementById("score-filter").value);
  const filtered = state.recommendations.filter((r) => {
    return (!category || r.category_code === category)
      && (!brand || r.brand === brand)
      && (!source || r.source === source)
      && r.score >= minScore;
  });
  document.getElementById("recommendation-grid").innerHTML = filtered.map(recCard).join("")
    || `<div class="panel">No offline recommendation is available for this user or filter set.</div>`;
}

function showMessage(message, tone = "neutral") {
  const colors = {
    neutral: ["#eef2f7", "#334155"],
    warning: ["#fff3dc", "#8a4b00"],
    error: ["#fee2e2", "#991b1b"],
  };
  const [background, color] = colors[tone] || colors.neutral;
  document.getElementById("recommendation-grid").innerHTML = `
    <div class="panel" style="background:${background}; color:${color};">
      ${escapeHtml(message)}
    </div>
  `;
}

function renderPipeline(metrics) {
  document.getElementById("pipeline-flow").innerHTML = metrics.pipeline_steps.map((step, index) => `
    <div class="pipeline-step">
      <div class="step-index">${index + 1}</div>
      <div><b>${escapeHtml(step.title)}</b><span>${escapeHtml(step.description)}</span></div>
    </div>
  `).join("");

  document.getElementById("metrics-grid").innerHTML = metrics.metrics.length
    ? metrics.metrics.map((m) => `
      <div class="metric-row">
        <b>${escapeHtml(m.split)}</b>
        <div class="metric-values">
          <div><span>${Number(m.roc_auc).toFixed(3)}</span><small>ROC-AUC</small></div>
          <div><span>${Number(m.pr_auc).toFixed(3)}</span><small>PR-AUC</small></div>
          <div><span>${Number(m.logloss).toFixed(3)}</span><small>Logloss</small></div>
        </div>
        <small>${fmt(m.positives)} positives / ${fmt(m.samples)} samples</small>
      </div>
    `).join("")
    : `<div class="metric-row"><b>Serving only</b><small>Model metrics were not regenerated in this run.</small></div>`;

  document.getElementById("split-note").innerHTML = `
    <b>${escapeHtml(metrics.training_mode)}</b><br />
    ${escapeHtml(metrics.leakage_note)}<br />
    Train history: ${escapeHtml(metrics.splits.train_history || "")}. Train label: ${escapeHtml(metrics.splits.train_label || "")}.
    Validation label: ${escapeHtml(metrics.splits.validation_label || "")}. Test label: ${escapeHtml(metrics.splits.test_label || "")}.
    Serve history: ${escapeHtml(metrics.splits.serve_history || "")}.
  `;
  renderBars("feature-chart", metrics.feature_importance || [], true);
}

function recCard(rec) {
  return `
    <article class="rec-card">
      <div class="rec-head">
        <div>
          <div class="rank">#${rec.rank}</div>
          <div class="product-id">${escapeHtml(rec.product_id)}</div>
        </div>
        <span class="source-badge ${sourceClass(rec.source)}">${escapeHtml(sourceLabel(rec.source))}</span>
      </div>
      <div class="rec-meta">
        <b>${escapeHtml(rec.brand)}</b><br />
        ${escapeHtml(rec.category_code)}<br />
        ${money(rec.price)}
      </div>
      <div class="score-line">
        <div class="score-top"><span>Score ${rec.score.toFixed(3)}</span><span>Retrieval ${rec.retrieval_score.toFixed(3)}</span></div>
        <div class="progress"><div style="width:${Math.max(2, rec.score * 100)}%"></div></div>
      </div>
      <p class="explanation">${escapeHtml(rec.explanation)}</p>
    </article>
  `;
}

function renderBars(targetId, data, decimal = false) {
  const target = document.getElementById(targetId);
  if (!data.length) {
    target.innerHTML = `<div class="bar-row"><div class="bar-label">No feature importance available</div></div>`;
    return;
  }
  const max = Math.max(...data.map((d) => Number(d.value)), 1);
  target.innerHTML = data.map((d) => `
    <div class="bar-row">
      <div class="bar-label" title="${escapeHtml(d.name)}">${escapeHtml(d.name)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.max(2, Number(d.value) / max * 100)}%"></div></div>
      <div class="bar-value">${decimal ? Number(d.value).toFixed(2) : fmt(d.value)}</div>
    </div>
  `).join("");
}

function setSelect(id, values) {
  const select = document.getElementById(id);
  const current = select.value;
  select.innerHTML = `<option value="">All</option>${values.map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("")}`;
  if (values.includes(current)) select.value = current;
}

function unique(values) {
  return Array.from(new Set(values.filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function sourceClass(source) {
  const value = source.toLowerCase();
  if (value.includes("als") && value.includes("ann")) return "hybrid";
  if (value.includes("als")) return "als";
  if (value.includes("ann")) return "ann";
  if (value.includes("popular")) return "popular";
  return "";
}

function sourceLabel(source) {
  const value = source.toLowerCase();
  if (value.includes("als") && value.includes("ann")) return "Hybrid";
  if (value.includes("als")) return "ALS";
  if (value.includes("ann")) return "ANN";
  if (value.includes("popular")) return "Fallback";
  return source;
}

function fmt(value) {
  return new Intl.NumberFormat("en-US").format(Number(value));
}

function money(value) {
  return `$${Number(value || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
