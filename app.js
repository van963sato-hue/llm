import { STORES, putOne, getAll, getOne, clearAll, uid } from "./db.js";

const $ = (id) => document.getElementById(id);


function makeWorker(relPath) {
  try {
    // Resolve relative to this module file (robust for GitHub Pages subpaths)
    return new Worker(new URL(relPath, import.meta.url));
  } catch (e) {
    console.error("Worker failed:", relPath, e);
    return null;
  }
}

const $status = $("status");
const $bar = $("bar");
const $usage = $("usage");
const $left = $("left");
const $right = $("right");
const $q = $("q");

const $providerFilter = $("providerFilter");
const $modelFilter = $("modelFilter");
const $clearFiltersBtn = $("clearFiltersBtn");

const $file = $("file");
const $importBtn = $("importBtn");
const $cancelBtn = $("cancelBtn");
const $wipeBtn = $("wipeBtn");

const $exportLiteBtn = $("exportLiteBtn");
const $exportFullBtn = $("exportFullBtn");
const $exportAssetsBtn = $("exportAssetsBtn");

const $newMomentBtn = $("newMomentBtn");
const $timelineBtn = $("timelineBtn");
const $rebuildBtn = $("rebuildBtn");
const $aboutBtn = $("aboutBtn");
const $iconCfgBtn = $("iconCfgBtn");

let state = {
  convs: [],
  convMap: new Map(),
  moments: [],
  prompts: [],
  history: [],
  labels: [],
  assets: [],
  iconCfgAll: [],
  iconCfgGlobal: null,
  iconCfgConv: new Map(),
  assetUrlCache: new Map(),
  iconEdit: null,
  iconTmpBlob: null,
  iconCfgTmp: { user: null, model: null, system: null },
  currentConvId: null,
  lastMomentId: null,
  selectedIdx: new Set(),
  searchIds: null, // {convIds, momentIds, promptIds, historyIds}
  searchDirty: true,
  limits: { conv: 120, moment: 50, history: 80 },
  filters: { provider: "", model: "" }
};


function currentConv() {
  if (!state.currentConvId) return null;
  return state.convMap?.get(state.currentConvId) || state.convs.find(c => c.id === state.currentConvId) || null;
}

function updateNewMomentButton() {
  const btn = $("newMomentBtn");
  if (!btn) return;
  const conv = currentConv();
  const ok = !!conv && state.selectedIdx && state.selectedIdx.size > 0;
  btn.disabled = !ok;
  btn.title = ok ? "選択したメッセージからMomentを作る" : "右の会話でメッセージを選んでから";
}

function safeFilename(name) {
  return (name || "untitled")
    .replace(/[\\\/:*?"<>|]/g, "_")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80) || "untitled";
}

function roleLabel(role) {
  if (!role) return "unknown";
  const r = role.toLowerCase();
  if (r === "user") return "User";
  if (r === "assistant") return "Assistant";
  if (r === "system") return "System";
  return role;
}


function assetById(aid) {
  return state.assets.find(a => a.id === aid) || null;
}
function assetURL(aid) {
  if (!aid) return "";
  if (state.assetUrlCache.has(aid)) return state.assetUrlCache.get(aid);
  const a = assetById(aid);
  if (!a?.blob) return "";
  const url = URL.createObjectURL(a.blob);
  state.assetUrlCache.set(aid, url);
  return url;
}
function clearAssetURLCache() {
  try {
    for (const u of state.assetUrlCache.values()) URL.revokeObjectURL(u);
  } catch(e) {}
  state.assetUrlCache = new Map();
}

function getGlobalIconCfg() {
  return state.iconCfgGlobal || { id: "global", user: { name: "", iconId: "" }, system: { name: "", iconId: "" }, models: {} };
}
function getConvIconCfg(convId) {
  return state.iconCfgConv.get(convId) || { id: `conv:${convId}`, convId, overrides: {} };
}
async function saveIconCfgRecord(rec) {
  await putOne(STORES.iconcfg, { ...rec, updatedAt: Date.now() });
  // update in-memory
  const idx = state.iconCfgAll.findIndex(x => x.id === rec.id);
  const merged = { ...rec, updatedAt: Date.now() };
  if (idx >= 0) state.iconCfgAll[idx] = merged; else state.iconCfgAll.push(merged);

  if (rec.id === "global") state.iconCfgGlobal = merged;
  if (rec.id?.startsWith("conv:") && rec.convId) state.iconCfgConv.set(rec.convId, merged);
}
async function saveAsset(existingId, blob, typeHint = "image/png") {
  const id = existingId || uid("icon");
  const out = blob || null;
  await putOne(STORES.asset, { id, blob: out, type: out?.type || typeHint, updatedAt: Date.now() });
  const idx = state.assets.findIndex(a => a.id === id);
  const rec = { id, blob: out, type: out?.type || typeHint, updatedAt: Date.now() };
  if (idx >= 0) state.assets[idx] = rec; else state.assets.push(rec);
  clearAssetURLCache();
  return id;
}

function labelForMessage(conv, m, idx) {
  const global = getGlobalIconCfg();
  const convCfg = getConvIconCfg(conv.id);
  const ov = convCfg?.overrides?.[String(idx)] || null;

  const role = (m.role || "").toLowerCase();
  if (role === "user") {
    const name = (ov?.name ?? global.user?.name ?? "User").trim() || "User";
    const iconId = ov?.iconId || global.user?.iconId || "";
    return { label: name, iconId, role, model: "" };
  }
  if (role === "assistant") {
    const model = m.model || "";
    const prof = global.models?.[model] || global.models?.["*"] || null;
    const baseLabel = (model ? model : "Assistant") + (prof?.name ? ` / ${prof.name}` : "");
    const label = (ov?.name ?? baseLabel).trim() || baseLabel;
    const iconId = ov?.iconId || prof?.iconId || "";
    return { label, iconId, role, model };
  }
  const label = (ov?.name ?? global.system?.name ?? "System").trim() || "System";
  const iconId = ov?.iconId || global.system?.iconId || "";
  return { label, iconId, role, model: "" };
}

function initialForLabel(s) {
  const t = (s || "").trim();
  if (!t) return "?";
  // Try pick first non-space char
  return t[0].toUpperCase();
}

function closeIconModal() {
  const mb = $("iconModal");
  if (mb) mb.classList.remove("show");
  state.iconEdit = null;
  state.iconTmpBlob = null;
}


function openIconModal(idx) {
  const conv = currentConv();
  if (!conv) return;
  const modal = $("iconModal");
  const nameEl = $("tmpIconName");
  const fileEl = $("tmpIconFile");
  const canvas = $("tmpCropPrev");
  const curEl = $("tmpIconCurrent");
  if (!modal || !nameEl || !fileEl || !canvas || !curEl) return;

  // ensure predictable canvas size
  if (!canvas.width) canvas.width = 420;
  if (!canvas.height) canvas.height = 140;

  const cfg = getConvIconCfg(conv.id);
  const ov = cfg?.overrides?.[String(idx)] || null;

  state.iconEdit = { convId: conv.id, idx };
  state.iconTmpBlob = null;

  // allow picking the same file again
  fileEl.value = "";

  // fill fields
  nameEl.value = ov?.name || "";

  // show current resolved label/icon
  const info = labelForMessage(conv, conv.messages?.[idx] || {}, idx);
  curEl.textContent = `現在: ${info.label || ""}  (#${idx + 1})`;

  // preview: show override icon if present, otherwise clear
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  if (ov?.iconId) {
    const a = assetById(ov.iconId);
    if (a?.blob) {
      renderCropPreview(canvas, a.blob, 1, 1).catch(() => {});
    }
  }

  // open
  modal.classList.add("show");
}



async function exportConversation(conv, format = "json", onlySelected = false) {
  if (!conv) return;
  const msgs = conv.messages || [];
  const selected = Array.from(state.selectedIdx || []).sort((a,b)=>a-b);
  const picked = onlySelected ? selected.map(i => msgs[i]).filter(Boolean) : msgs;

  const meta = {
    id: conv.id,
    title: conv.title,
    provider: conv.provider || "",
    models: conv.models || [],
    createdAt: conv.createdAt || null,
    updatedAt: conv.updatedAt || null,
    exportedAt: Date.now(),
    onlySelected
  };

  const baseName = safeFilename(conv.title);
  if (format === "json") {
    const payload = { meta, messages: picked };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    downloadBlob(blob, `${baseName}${onlySelected ? ".selected" : ""}.json`);
    return;
  }

  if (format === "md") {
    const lines = [];
    lines.push(`# ${conv.title || "Conversation"}`);
    lines.push("");
    if (conv.provider) lines.push(`- provider: ${conv.provider}`);
    if (meta.models?.length) lines.push(`- models: ${meta.models.join(", ")}`);
    if (conv.createdAt) lines.push(`- created: ${new Date(conv.createdAt).toLocaleString()}`);
    if (conv.updatedAt) lines.push(`- updated: ${new Date(conv.updatedAt).toLocaleString()}`);
    lines.push(`- exported: ${new Date(meta.exportedAt).toLocaleString()}`);
    lines.push("");
    lines.push("---");
    lines.push("");

    for (let i = 0; i < picked.length; i++) {
      const m = picked[i];
      const ts = m.ts ? new Date(m.ts*1000).toLocaleString() : "";
      const model = m.model ? ` (${m.model})` : "";
      lines.push(`## ${roleLabel(m.role)}${model}${ts ? " — " + ts : ""}`);
      lines.push("");
      lines.push((m.text || "").replace(/\r\n/g, "\n"));
      lines.push("");
      lines.push("---");
      lines.push("");
    }

    const blob = new Blob([lines.join("\n")], { type: "text/markdown" });
    downloadBlob(blob, `${baseName}${onlySelected ? ".selected" : ""}.md`);
    return;
  }

  if (format === "html") {
    const escHtml = (s) => String(s||"").replace(/[&<>"]/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;", "\"":"&quot;" }[c]));
    const rows = picked.map((m) => {
      const role = roleLabel(m.role);
      const ts = m.ts ? new Date(m.ts*1000).toLocaleString() : "";
      const model = m.model || "";
      return `<div class="msg ${escHtml((m.role||"").toLowerCase())}">
        <div class="meta"><b>${escHtml(role)}</b>${model ? ` <span class="pill">${escHtml(model)}</span>` : ""}${ts ? ` <span class="pill">${escHtml(ts)}</span>` : ""}</div>
        <pre>${escHtml(m.text||"")}</pre>
      </div>`;
    }).join("\n");

    const doc = `<!doctype html><html lang="ja"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
      <title>${escHtml(conv.title||"Conversation")}</title>
      <style>
        body{font-family:system-ui,-apple-system,Segoe UI,Roboto;max-width:980px;margin:0 auto;padding:18px;background:#fff;color:#111}
        .pill{display:inline-block;margin-left:6px;padding:3px 8px;border-radius:999px;background:#eee;font-size:12px}
        .msg{border:1px solid #ddd;border-radius:14px;padding:10px 12px;margin:10px 0}
        .msg.user{background:#fafafa}
        .msg.assistant{background:#ffffff}
        pre{white-space:pre-wrap;word-break:break-word;margin:8px 0 0}
        .top{position:sticky;top:0;background:#fff;padding:10px 0;border-bottom:1px solid #ddd}
        .muted{color:#666;font-size:12px}
      </style></head><body>
      <div class="top">
        <h1 style="margin:0;font-size:18px">${escHtml(conv.title||"Conversation")}</h1>
        <div class="muted">
          ${conv.provider ? `provider: ${escHtml(conv.provider)} / ` : ""}${meta.models?.length ? `models: ${escHtml(meta.models.join(", "))} / ` : ""}exported: ${escHtml(new Date(meta.exportedAt).toLocaleString())}
          ${onlySelected ? " / selected-only" : ""}
        </div>
      </div>
      ${rows}
      </body></html>`;
    const blob = new Blob([doc], { type: "text/html" });
    downloadBlob(blob, `${baseName}${onlySelected ? ".selected" : ""}.html`);
  }
}


function setStatus(s) { $status.textContent = s || ""; }
function setBar(pct) { $bar.style.width = `${Math.max(0, Math.min(100, pct))}%`; }

function fmtDate(ms) {
  if (!ms) return "";
  const d = new Date(ms);
  return isNaN(d.getTime()) ? "" : d.toLocaleString();
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;", "\"":"&quot;","'":"&#39;" }[c]));
}

function bytes(n) {
  const u = ["B","KB","MB","GB"];
  let i=0, x=n;
  while (x >= 1024 && i < u.length-1) { x/=1024; i++; }
  return `${x.toFixed(i===0?0:1)} ${u[i]}`;
}

// Workers
const importWorker = makeWorker("./workers/import.worker.js");


if (importWorker) {
// Worker load/runtime errors -> surface to UI
importWorker.addEventListener("error", (e) => {
  importing = false;
  lockUI(false);
  setStatus(`取り込みワーカーエラー: ${e.message || "unknown"}`);
});
importWorker.addEventListener("messageerror", () => {
  importing = false;
  lockUI(false);
  setStatus("取り込みワーカー: メッセージ解釈エラー");
});

} else {
  setStatus('⚠️ 取り込みワーカーが起動できないため、この環境では取り込みが動きません（GitHub Pagesで開くと動くことが多いよ）');
}

const searchWorker = makeWorker("./workers/search.worker.js");
const exportWorker = makeWorker("./workers/export.worker.js");

let importing = false;
let exporting = false;

function lockUI(busy) {
  $importBtn.disabled = busy;
  $wipeBtn.disabled = busy;
  $exportLiteBtn.disabled = busy;
  $exportFullBtn.disabled = busy;
  $exportAssetsBtn.disabled = busy;
  $cancelBtn.disabled = !busy;
  if (!busy) setBar(0);
}


function uniq(arr) { return Array.from(new Set(arr.filter(Boolean))); }

function populateFilters() {
  // preserve selection
  const prevP = state.filters.provider || "";
  const prevM = state.filters.model || "";

  const providers = uniq(state.convs.map(c => c.provider)).sort();
  const models = uniq(state.convs.flatMap(c => c.models || [])).sort();

  // provider select
  $providerFilter.innerHTML = `<option value="">provider: all</option>` + providers.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join("");
  $modelFilter.innerHTML = `<option value="">model: all</option>` + models.map(m => `<option value="${esc(m)}">${esc(m)}</option>`).join("");

  // restore if available
  if (providers.includes(prevP)) $providerFilter.value = prevP; else $providerFilter.value = "";
  if (models.includes(prevM)) $modelFilter.value = prevM; else $modelFilter.value = "";

  state.filters.provider = $providerFilter.value || "";
  state.filters.model = $modelFilter.value || "";
}

function applyFilters() {
  state.filters.provider = $providerFilter.value || "";
  state.filters.model = $modelFilter.value || "";
  renderLeft();
  // If currently showing a conversation that doesn't match filters, keep right as-is (user may want to read it),
  // but list will be filtered.
}

function clearFilters() {
  state.filters.provider = "";
  state.filters.model = "";
  $providerFilter.value = "";
  $modelFilter.value = "";
  renderLeft();
}

async function refreshFromDB() {
  state.convs = await getAll(STORES.conv);
  state.convMap = new Map(state.convs.map(c => [c.id, c]));
  state.moments = await getAll(STORES.moment);
  state.prompts = await getAll(STORES.prompt);
  state.history = await getAll(STORES.history);
  state.labels = await getAll(STORES.label);
  state.assets = await getAll(STORES.asset);
  state.iconCfgAll = await getAll(STORES.iconcfg);

  state.iconCfgGlobal = state.iconCfgAll.find(x => x.id === "global") || null;
  state.iconCfgConv = new Map();
  for (const x of state.iconCfgAll) {
    if (x?.id?.startsWith("conv:") && x.convId) state.iconCfgConv.set(x.convId, x);
  }

  clearAssetURLCache();
  populateFilters();

  setStatus(`会話${state.convs.length} / Moment${state.moments.length} / History${state.history.length}`);
  renderLeft();
  renderRight();

  // usage
  const imgBytes = state.assets.reduce((a,b)=>a + (b.blob?.size || 0), 0);
  const mBytes = state.moments.reduce((a,b)=>a + (b.previewId ? 0 : 0), 0);
  $usage.textContent = `Assets: ${(imgBytes/1024/1024).toFixed(1)}MB`;
}


function applySearchIds(ids) {
  state.searchIds = ids;
  renderLeft();
}

function convPass(c) {
  if (state.filters.provider && (c.provider || "") !== state.filters.provider) return false;
  if (state.filters.model && !(c.models || []).includes(state.filters.model)) return false;
  if (!state.searchIds) return true;
  return state.searchIds.convIds?.includes(c.id);
}
function momentPass(m) {
  if (state.filters.model && !(m.models || []).includes(state.filters.model)) return false;
  if (state.filters.provider) {
    const c = state.convMap.get(m.convId);
    if ((c?.provider || "") !== state.filters.provider) return false;
  }
  if (!state.searchIds) return true;
  return state.searchIds.momentIds?.includes(m.id);
}
function historyPass(h) {
  if (state.filters.provider && (h.provider || "") !== state.filters.provider) return false;
  if (state.filters.model && (h.model || "") !== state.filters.model) return false;
  if (!state.searchIds) return true;
  return state.searchIds.historyIds?.includes(h.id);
}
function promptPass(p) {
  if (!state.searchIds) return true;
  return state.searchIds.promptIds?.includes(p.id);
}

function renderLeft() {
  const convsAll = [...state.convs]
    .sort((a,b)=>(b.updatedAt||0)-(a.updatedAt||0))
    .filter(convPass);
  const convs = convsAll.slice(0, state.limits.conv);

  const momentsAll = [...state.moments]
    .sort((a,b)=>(b.createdAt||0)-(a.createdAt||0))
    .filter(momentPass);
  const moments = momentsAll.slice(0, state.limits.moment);

  const promptsAll = [...state.prompts]
    .sort((a,b)=>(b.updatedAt||0)-(a.updatedAt||0))
    .filter(promptPass);

  const historyAll = [...state.history]
    .sort((a,b)=>(b.ts||0)-(a.ts||0))
    .filter(historyPass);
  const history = historyAll.slice(0, state.limits.history);

  $left.innerHTML = `
  <div class="muted">保存：端末内（IndexedDB） / オフライン可</div>
  ${(state.searchIds || state.filters.provider || state.filters.model) ? `
    <div class="muted" style="margin-top:6px">
      ${(state.filters.provider ? `<span class="pill">provider:${esc(state.filters.provider)}</span>` : ``)}
      ${(state.filters.model ? `<span class="pill">model:${esc(state.filters.model)}</span>` : ``)}
      ${(state.searchIds ? `<span class="pill">search</span>` : ``)}
    </div>` : ``}

  <div class="hr"></div>
  <b>Moments</b>
  <div class="muted">${moments.length}/${momentsAll.length}</div>
  ${moments.length ? moments.map(m => `
    <div class="item ${state.lastMomentId===m.id?'active':''}" data-moment="${esc(m.id)}">
      <b>${esc(m.title || "(untitled)")}</b>
      <div class="muted">${esc(fmtDate(m.createdAt))}</div>
      <div>${(m.tags||[]).slice(0,4).map(t=>`<span class="pill">${esc(t)}</span>`).join("")}</div>
    </div>
  `).join("") : `<div class="muted">まだMomentがないよ。</div>`}
  ${momentsAll.length > moments.length ? `
    <div class="item" id="moreMoments">
      <b>＋ Moments もっと見る</b>
      <div class="muted">${moments.length}/${momentsAll.length}</div>
    </div>` : ``}

  <div class="hr"></div>
  <b>Conversations</b>
  <div class="muted">${convs.length}/${convsAll.length}</div>
  ${convs.length ? convs.map(c => `
    <div class="item ${state.currentConvId===c.id?'active':''}" data-conv="${esc(c.id)}">
      <b>${esc(c.title)}</b>
      <div class="muted">${esc(fmtDate(c.updatedAt || c.createdAt))} / msgs:${c.messages?.length || 0}</div>
      <div>
        ${(c.models||[]).slice(0,3).map(s=>`<span class="pill">${esc(s)}</span>`).join("")}
        ${c.provider ? `<span class="pill">${esc(c.provider)}</span>` : ""}
      </div>
    </div>
  `).join("") : `<div class="muted">まだ会話がないよ。</div>`}
  ${convsAll.length > convs.length ? `
    <div class="item" id="moreConvs">
      <b>＋ Conversations もっと見る</b>
      <div class="muted">${convs.length}/${convsAll.length}</div>
    </div>` : ``}

  <div class="hr"></div>
  <b>Prompts</b>
  ${promptsAll.length ? promptsAll.slice(0, 60).map(p => `
    <div class="item" data-prompt="${esc(p.id)}">
      <b>${esc(p.name || "(no name)")}</b>
      <div class="muted">${esc(fmtDate(p.updatedAt||p.createdAt))}</div>
      <div>${p.tags ? p.tags.slice(0,4).map(t=>`<span class="pill">${esc(t)}</span>`).join("") : ""}</div>
    </div>
  `).join("") : `<div class="muted">まだPromptがないよ。</div>`}

  <div class="hr"></div>
  <b>History</b>
  <div class="muted">${history.length}/${historyAll.length}</div>
  ${history.length ? history.map(h => `
    <div class="item" data-history="${esc(h.id)}">
      <b>${esc(h.title || "(no title)")}</b>
      <div class="muted">${esc(fmtDate(h.ts))}</div>
      <div>
        ${h.model ? `<span class="pill">${esc(h.model)}</span>` : ""}
        ${h.provider ? `<span class="pill">${esc(h.provider)}</span>` : ""}
        ${h.auto ? `<span class="pill">auto</span>` : ""}
      </div>
    </div>
  `).join("") : `<div class="muted">まだHistoryがないよ。</div>`}
  ${historyAll.length > history.length ? `
    <div class="item" id="moreHistory">
      <b>＋ History もっと見る</b>
      <div class="muted">${history.length}/${historyAll.length}</div>
    </div>` : ``}
  `;

  $left.querySelectorAll("[data-conv]").forEach(el => el.onclick = () => openConversation(el.getAttribute("data-conv")));
  $left.querySelectorAll("[data-moment]").forEach(el => el.onclick = () => openMoment(el.getAttribute("data-moment")));
  $left.querySelectorAll("[data-prompt]").forEach(el => el.onclick = () => openPrompt(el.getAttribute("data-prompt")));
  $left.querySelectorAll("[data-history]").forEach(el => el.onclick = () => openHistory(el.getAttribute("data-history")));

  const moreC = $("moreConvs");
  if (moreC) moreC.onclick = () => { state.limits.conv += 120; renderLeft(); };

  const moreM = $("moreMoments");
  if (moreM) moreM.onclick = () => { state.limits.moment += 50; renderLeft(); };

  const moreH = $("moreHistory");
  if (moreH) moreH.onclick = () => { state.limits.history += 80; renderLeft(); };
}


function renderRight() {
  const conv = currentConv();
  if (!conv) {
    $right.innerHTML = `<div class="muted">左から会話/Moment/Timelineを開いてね。</div>`;
    return;
  }

  const msgs = conv.messages || [];
  const selected = state.selectedIdx || new Set();

  $right.innerHTML = `
    <div class="row" style="gap:10px;align-items:flex-start;flex-wrap:wrap">
      <div style="flex:1;min-width:240px">
        <b style="font-size:16px">${esc(conv.title || "(untitled)")}</b>
        <div class="muted" style="margin-top:4px">
          ${conv.provider ? `provider: <span class="pill">${esc(conv.provider)}</span>` : ``}
          ${(conv.models||[]).slice(0,5).map(m=>`<span class="pill">${esc(m)}</span>`).join("")}
        </div>
        <div class="muted" style="margin-top:6px">
          msgs: ${msgs.length}
          ${conv.updatedAt ? ` / updated: ${esc(fmtDate(conv.updatedAt))}` : ``}
          ${conv.createdAt ? ` / created: ${esc(fmtDate(conv.createdAt))}` : ``}
        </div>
      </div>

      <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end">
        <button id="expConvJson" title="会話全体をJSONで保存">JSON</button>
        <button id="expConvMd" title="会話全体をMarkdownで保存">MD</button>
        <button id="expConvHtml" title="会話全体を単体HTMLで保存">HTML</button>
        <button id="expSelMd" ${selected.size?``:`disabled`} title="選択したメッセージだけをMarkdownで保存">選択MD</button>
        <button id="expSelJson" ${selected.size?``:`disabled`} title="選択したメッセージだけをJSONで保存">選択JSON</button>
      </div>
    </div>

    <div class="hr"></div>

    <div class="muted" id="chatProgress">描画準備中…</div>
    <div id="chatLog" class="chatlog"></div>
  `;

  $("expConvJson").onclick = () => exportConversation(conv, "json", false);
  $("expConvMd").onclick = () => exportConversation(conv, "md", false);
  $("expConvHtml").onclick = () => exportConversation(conv, "html", false);
  $("expSelMd").onclick = () => exportConversation(conv, "md", true);
  $("expSelJson").onclick = () => exportConversation(conv, "json", true);

  updateNewMomentButton();



  // render messages in batches to avoid freezing
  const token = (state._renderToken = (state._renderToken || 0) + 1);
  const logEl = $("chatLog");
  const progEl = $("chatProgress");
  if (!logEl || !progEl) return;

  logEl.innerHTML = "";
  const BATCH = 160;
  let i = 0;

  const renderBatch = async () => {
    if (token !== state._renderToken) return; // canceled
    const frag = document.createDocumentFragment();
    for (let k = 0; k < BATCH && i < msgs.length; k++, i++) {
      const m = msgs[i] || {};
      const el = document.createElement("div");
      const role = (m.role || "").toLowerCase();
      el.className = `msg ${role} ${selected.has(i) ? "sel" : ""}`;
      el.dataset.i = String(i);

      const ts = m.ts ? new Date(m.ts*1000).toLocaleString() : "";
      const info = labelForMessage(conv, m, i);
const iconUrl = info.iconId ? assetURL(info.iconId) : "";
const ph = initialForLabel(info.label);
el.innerHTML = `
  <div class="msgmeta">
    <button class="avatarBtn" title="臨時アイコンを変更" aria-label="icon">
      ${iconUrl ? `<img class="avatar" src="${iconUrl}" alt="icon"/>` : `<span class="avatar ph">${esc(ph)}</span>`}
    </button>
    <div class="metaMain">
      <b>${esc(info.label)}</b>
      ${ts ? ` <span class="pill">${esc(ts)}</span>` : ""}
      <span class="muted" style="margin-left:8px">#${i+1}</span>
    </div>
  </div>
  <div class="msgbody">${esc(m.text || "")}</div>
`;
      const avBtn = el.querySelector(".avatarBtn");
      if (avBtn) avBtn.onclick = (e) => { e.stopPropagation(); openIconModal(i); };
el.onclick = () => {
        const idx = Number(el.dataset.i);
        if (selected.has(idx)) selected.delete(idx); else selected.add(idx);
        el.classList.toggle("sel");
        $("expSelMd").disabled = selected.size === 0;
        $("expSelJson").disabled = selected.size === 0;
        updateNewMomentButton();
        progEl.textContent = `msgs: ${msgs.length} / selected: ${selected.size}`;
      };

      frag.appendChild(el);
    }
    logEl.appendChild(frag);
    progEl.textContent = `msgs: ${msgs.length} / selected: ${selected.size} / rendered: ${i}/${msgs.length}`;
    if (i < msgs.length) {
      // yield
      await new Promise(r => setTimeout(r, 0));
      return renderBatch();
    }
  };

  renderBatch();
}

function openConversation(id) {
  state.currentConvId = id;
  state.selectedIdx = new Set();
  updateNewMomentButton();
  renderRight();
}

async function openMoment(id) {
  const m = await getOne(STORES.moment, id);
  if (!m) return;

  state.lastMomentId = m.id;

  const conv = state.convs.find(c => c.id === m.convId);
  const msgs = conv?.messages?.slice(m.fromIdx, m.toIdx + 1) || [];
  const img = m.imageId ? await getOne(STORES.asset, m.imageId) : null;
  const prompt = m.promptProfileId ? await getOne(STORES.prompt, m.promptProfileId) : null;

  $right.innerHTML = `
    <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <b>${esc(m.title || "(untitled)")}</b>
      <span class="pill">${esc(fmtDate(m.createdAt))}</span>
      ${(m.models||[]).map(s=>`<span class="pill">${esc(s)}</span>`).join("")}
      ${conv?.provider ? `<span class="pill">${esc(conv.provider)}</span>` : ""}
    </div>

    <div style="margin:8px 0">
      ${(m.tags||[]).map(t=>`<span class="pill">${esc(t)}</span>`).join("") || `<span class="muted">タグなし</span>`}
    </div>

    ${img ? `<img id="momentImg" src="${URL.createObjectURL(img.blob)}" alt="moment image"/>` : `<div class="muted">イラスト未添付</div>`}

    ${img ? `
      <div style="margin-top:10px">
        <div class="muted">表紙トリミング（中央）</div>
        <canvas id="cropPrev" style="width:100%;max-width:520px;border:1px solid var(--border);border-radius:12px;margin-top:6px"></canvas>
        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:8px">
          <button id="crop11">1:1</button>
          <button id="crop34">3:4</button>
          <button id="crop169">16:9</button>
          <button id="cropSave" title="いま表示している比率で保存">保存</button>
        </div>
      </div>
    ` : ``}

    <div style="margin-top:10px" class="grid2">
      <div>
        <label class="muted">イラストを追加/差し替え</label>
        <input id="imgIn" type="file" accept="image/*"/>
      </div>
      <div>
        <label class="muted">タグ（スペース区切り）</label>
        <input id="tags" placeholder="#告白 #夜 など" value="${esc((m.tags||[]).join(" "))}"/>
      </div>
    </div>

    <div style="margin-top:10px">
      <label class="muted">お別れメッセージ</label>
      <div style="display:flex;gap:10px;flex-wrap:wrap;margin:6px 0">
        <select id="goodbyeTpl" style="max-width:280px">
          <option value="">テンプレを選ぶ…</option>
          ${GOODBYE_TEMPLATES.map(t=>`<option value="${esc(t.id)}">${esc(t.name)}</option>`).join("")}
        </select>
        <button id="insertTpl">挿入</button>
      </div>
      <textarea id="goodbye" rows="3" placeholder="この瞬間/このモデル/この相棒へ…">${esc(m.goodbye||"")}</textarea>
    </div>

    <div style="margin-top:10px">
      <label class="muted">当時の相棒プロンプト（Profile）</label>
      <input id="promptName" placeholder="例: 蒼カミル 2025秋版" value="${esc(prompt?.name||"")}"/>
      <input id="promptCompanion" placeholder="相棒名タグ（自由）" value="${esc(prompt?.companion||"")}"/>
      <textarea id="promptBody" rows="6" placeholder="プロンプト本文">${esc(prompt?.content||"")}</textarea>
      <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:8px">
        <button id="savePrompt">プロンプト保存</button>
        <button id="saveMoment">保存</button>
        <button id="toTimeline">このMomentをTimelineに刻む</button>
      </div>
    </div>

    <div class="hr"></div>
    <div class="muted">一瞬ログ（${msgs.length}）</div>
    ${msgs.map(x => `<div class="msg"><b>${esc(x.role)}</b>\n${esc(x.text)}</div>`).join("")}
  `;

  $("saveMoment").onclick = async () => {
    const tags = ($("tags").value || "").split(/\s+/).filter(Boolean);
    const goodbye = $("goodbye").value || "";
    await putOne(STORES.moment, { ...m, tags, goodbye, updatedAt: Date.now() });
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    await openMoment(m.id);
  };

  $("savePrompt").onclick = async () => {
    const name = $("promptName").value || "prompt";
    const companion = $("promptCompanion").value || "";
    const content = $("promptBody").value || "";
    const pid = m.promptProfileId || uid("prompt");
    await putOne(STORES.prompt, { id: pid, name, companion, content, updatedAt: Date.now() });
    await putOne(STORES.moment, { ...m, promptProfileId: pid, updatedAt: Date.now() });
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    await openMoment(m.id);
  };

  $("imgIn").onchange = async () => {
    const f = $("imgIn").files?.[0];
    if (!f) return;
    const blob = new Blob([await f.arrayBuffer()], { type: f.type || "image/png" });
    const aid = m.imageId || uid("img");
    await putOne(STORES.asset, { id: aid, blob, type: blob.type, updatedAt: Date.now() });
    await putOne(STORES.moment, { ...m, imageId: aid, updatedAt: Date.now() });
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    await openMoment(m.id);
  };

  $("toTimeline").onclick = async () => {
    const convId = m.convId || null;
    const ev = {
      id: uid("hist"),
      ts: (msgs[0]?.ts ? msgs[0].ts*1000 : m.createdAt || Date.now()),
      title: `Moment: ${m.title || "event"}`,
      detail: `Momentを刻む：${m.title || ""}`,
      memory: "",
      provider: conv?.provider || "",
      model: (m.models && m.models[0]) ? m.models[0] : "",
      links: { convId, momentId: m.id, promptProfileId: m.promptProfileId || null },
      auto: false
    };
    await putOne(STORES.history, ev);
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    openTimeline();

  // Goodbye template insertion
  const tplSel = $("goodbyeTpl");
  const tplBtn = $("insertTpl");
  if (tplSel && tplBtn) {
    tplBtn.onclick = () => {
      const id = tplSel.value;
      const tpl = GOODBYE_TEMPLATES.find(t => t.id === id);
      if (!tpl) return;
      const modelsStr = (m.models || []).join(", ");
      const dateStr = fmtDate(m.createdAt);
      const companion = (prompt?.companion || prompt?.name || $("promptName")?.value || "").trim();
      const text = tpl.body({ title: m.title || "Moment", date: dateStr, models: modelsStr, companion });
      const area = $("goodbye");
      if (!area) return;
      area.value = area.value ? (area.value + "\n\n" + text) : text;
    };
  }

  // Center crop preview / apply
  if (img && $("cropPrev")) {
    let cropRatio = { w: 1, h: 1 };
    const canvas = $("cropPrev");
    const blob = img.blob;

    const paint = async () => {
      await renderCropPreview(canvas, blob, cropRatio.w, cropRatio.h);
    };
    await paint();

    $("crop11").onclick = async () => { cropRatio = { w: 1, h: 1 }; await paint(); };
    $("crop34").onclick = async () => { cropRatio = { w: 3, h: 4 }; await paint(); };
    $("crop169").onclick = async () => { cropRatio = { w: 16, h: 9 }; await paint(); };

    $("cropSave").onclick = async () => {
      const out = await centerCropBlob(blob, cropRatio.w, cropRatio.h, 1200);
      const aid = m.imageId; // must exist here
      await putOne(STORES.asset, { id: aid, blob: out, type: out.type, updatedAt: Date.now() });
      state.searchDirty = true;
      wireIconModalOnce();
  await refreshFromDB();
      await openMoment(m.id);
    };
  }

  };
}

async function openHistory(id) {
  const h = await getOne(STORES.history, id);
  if (!h) return;

  $right.innerHTML = `
    <b>${esc(h.title || "(no title)")}</b>
    <div class="muted">${esc(fmtDate(h.ts))}</div>
    <div style="margin-top:6px">
      ${h.provider ? `<span class="pill">${esc(h.provider)}</span>` : ""}
      ${h.model ? `<span class="pill">${esc(h.model)}</span>` : ""}
      ${h.auto ? `<span class="pill">auto</span>` : ""}
    </div>
    <div class="hr"></div>

    <label class="muted">詳細</label>
    <textarea id="h_detail" rows="5">${esc(h.detail || "")}</textarea>

    <div style="margin-top:10px"></div>
    <label class="muted">この時点の記憶メモ</label>
    <textarea id="h_memory" rows="7" placeholder="この頃の距離感/呼び名/地雷/モデル/相棒プロンプトなど">${esc(h.memory || "")}</textarea>

    <div style="margin-top:10px" class="grid2">
      <div><label class="muted">provider</label><input id="h_provider" value="${esc(h.provider || "")}"/></div>
      <div><label class="muted">model</label><input id="h_model" value="${esc(h.model || "")}"/></div>
    </div>

    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
      <button id="h_save">保存</button>
      ${h.links?.convId ? `<button id="h_openConv">会話へ</button>` : ""}
      ${h.links?.momentId ? `<button id="h_openMoment">Momentへ</button>` : ""}
    </div>
  `;

  $("h_save").onclick = async () => {
    const detail = $("h_detail").value || "";
    const memory = $("h_memory").value || "";
    const provider = $("h_provider").value || "";
    const model = $("h_model").value || "";
    await putOne(STORES.history, { ...h, detail, memory, provider, model, updatedAt: Date.now() });
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    await openHistory(h.id);
  };

  if (h.links?.convId) $("h_openConv").onclick = () => openConversation(h.links.convId);
  if (h.links?.momentId) $("h_openMoment").onclick = () => openMoment(h.links.momentId);
}

function openTimeline() {
  const history = [...state.history].sort((a,b)=>(a.ts||0)-(b.ts||0));

  $right.innerHTML = `
    <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <b>Timeline</b>
      <span class="muted">自由入力で節目と“その時点の記憶”を刻める</span>
    </div>

    <div class="hr"></div>

    <div class="grid2">
      <div>
        <div class="muted">イベント一覧（新しい順）</div>
        <div class="hr"></div>
        ${history.slice().reverse().slice(0, 200).map(ev => `
          <div class="item" data-he="${esc(ev.id)}">
            <b>${esc(ev.title || "(no title)")}</b>
            <div class="muted">${esc(fmtDate(ev.ts))}</div>
            <div>
              ${ev.provider ? `<span class="pill">${esc(ev.provider)}</span>` : ""}
              ${ev.model ? `<span class="pill">${esc(ev.model)}</span>` : ""}
              ${ev.links?.momentId ? `<span class="pill">Moment</span>` : ""}
              ${ev.links?.convId ? `<span class="pill">Conv</span>` : ""}
              ${ev.memory ? `<span class="pill">memory</span>` : ""}
              ${ev.auto ? `<span class="pill">auto</span>` : ""}
            </div>
          </div>
        `).join("") || `<div class="muted">まだないよ。</div>`}
      </div>

      <div>
        <div class="muted">新規イベント</div>
        <div class="hr"></div>

        <label class="muted">日時</label>
        <input id="he_ts" type="datetime-local"/>

        <div style="height:8px"></div>
        <label class="muted">見出し</label>
        <input id="he_title" placeholder="例：呼び名が変わった夜 / 関係性が確定した日"/>

        <div style="height:8px"></div>
        <label class="muted">詳細</label>
        <textarea id="he_detail" rows="4" placeholder="短くてもOK。何が起きた？どう変わった？"></textarea>

        <div style="height:8px"></div>
        <label class="muted">この時点の記憶メモ（重要）</label>
        <textarea id="he_memory" rows="6" placeholder="呼び名／距離感／地雷／相棒プロンプト／モデルなど">${""}</textarea>

        <div style="height:8px"></div>
        <label class="muted">provider / model（自由）</label>
        <div style="display:flex;gap:8px">
          <input id="he_provider" placeholder="openai / anthropic / google / other" />
          <input id="he_model" placeholder="gpt-4o など" />
        </div>

        <div style="height:8px"></div>
        <label class="muted">リンク（今の状態に自動紐付け）</label>
        <div class="muted">Conv: ${esc(state.currentConvId || "-")} / Moment: ${esc(state.lastMomentId || "-")}</div>

        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
          <button id="he_save">追加</button>
        </div>
      </div>
    </div>
  `;

  // init datetime-local value
  const now = new Date();
  $("he_ts").value = new Date(now.getTime() - now.getTimezoneOffset()*60000).toISOString().slice(0,16);

  $right.querySelectorAll("[data-he]").forEach(el => el.onclick = () => openHistory(el.getAttribute("data-he")));

  $("he_save").onclick = async () => {
    const tsRaw = $("he_ts").value;
    const ts = tsRaw ? new Date(tsRaw).getTime() : Date.now();
    const title = $("he_title").value || "event";
    const detail = $("he_detail").value || "";
    const memory = $("he_memory").value || "";
    const provider = $("he_provider").value || "";
    const model = $("he_model").value || "";

    const ev = {
      id: uid("hist"),
      ts,
      title,
      detail,
      memory,
      provider,
      model,
      links: { convId: state.currentConvId || null, momentId: state.lastMomentId || null },
      auto: false
    };

    await putOne(STORES.history, ev);
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    openTimeline();
  };
}

function openAbout() {
  $right.innerHTML = `
    <b>About / 使い方</b>
    <div class="hr"></div>
    <div class="muted">
      <p><b>取り込み</b>: ChatGPTのExport ZIP（conversations.json）か、conversations.json単体、または汎用JSONを選んで「取り込み開始」。</p>
      <p><b>Moment</b>: 会話を開いてメッセージを複数選択 → 「選択からMoment」。絵/タグ/相棒プロンプト/お別れを添えて保存。</p>
      <p><b>Timeline</b>: 節目や「その時点の記憶メモ」を自由入力で刻む。Momentからも刻める。</p>
      <p><b>検索</b>: 3文字以上で高速検索。短い検索はタイトル中心に探す。</p>
      <p><b>エクスポート</b>: 軽量（json）→確実。完全（zip）→会話＋画像をまとめる。画像だけzipも用意。</p>
      <p><b>安全</b>: ログは端末内保存。GitHub Pagesで公開されるのはアプリのコードだけ。</p>
      <p>Tip: 大きいZIPで解凍が失敗する場合、ZIPをPCで展開して conversations.json を直接取り込むと安定。</p>
    </div>
  `;
}

async function createMomentFromSelection() {
  const conv = currentConv();
  if (!conv || !state.selectedIdx.size) return;

  const idxs = [...state.selectedIdx].sort((a,b)=>a-b);
  const fromIdx = idxs[0], toIdx = idxs[idxs.length - 1];

  const title = prompt("Momentタイトル", "") || "Moment";
  const models = Array.from(new Set((conv.messages || []).slice(fromIdx, toIdx+1).map(x=>x.model).filter(Boolean)));

  const m = {
    id: uid("moment"),
    convId: conv.id,
    title,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    fromIdx, toIdx,
    models,
    tags: [],
    imageId: null,
    promptProfileId: null,
    goodbye: ""
  };

  await putOne(STORES.moment, m);
  state.selectedIdx = new Set();
  updateNewMomentButton();
  state.searchDirty = true;
  wireIconModalOnce();
  await refreshFromDB();
  await openMoment(m.id);
}

// Import Worker wiring
if (importWorker) importWorker.onmessage = async (e) => {
  const m = e.data;
  if (m.type === "status") setStatus(m.msg);
  if (m.type === "progress") {
    const pct = m.total ? (m.done / m.total) * 100 : 0;
    setBar(pct);
    setStatus(`${m.phase}… ${m.done}/${m.total}`);
  }
  if (m.type === "done") {
    importing = false;
    lockUI(false);
    setStatus(`取り込み完了: 会話${m.sessions} / auto-history${m.history}`);
    state.searchDirty = true;
    wireIconModalOnce();
  await refreshFromDB();
    // auto rebuild search after import
    rebuildSearch();
  }
  if (m.type === "error") {
    importing = false;
    lockUI(false);
    setStatus("取り込み失敗: " + m.msg);
  }
};

// Search Worker wiring
let lastQuery = "";
if (searchWorker) searchWorker.onmessage = (e) => {
  const m = e.data;
  if (m.type === "status") setStatus(m.msg);
  if (m.type === "progress") {
    const pct = m.total ? (m.done / m.total) * 100 : 0;
    setBar(pct);
    setStatus(`${m.phase}… ${m.done}/${m.total}`);
  }
  if (m.type === "ready") {
    setStatus(`検索OK（docs:${m.docs}, grams:${m.grams}）`);
    setBar(0);
    state.searchDirty = false;
  }
  if (m.type === "result" && m.q === lastQuery) {
    applySearchIds(m.data);
  }
  if (m.type === "error") setStatus("検索エラー: " + m.msg);
};

function rebuildSearch() {
  lockUI(true);
  setStatus("検索インデックス作成中…");
  setBar(10);
  if (searchWorker) searchWorker.postMessage({ type: "rebuild" });
  // unlock UI when ready/status - but we keep UI mostly usable, so unlock immediately
  // We'll just allow controls; keep small lock to avoid weird states.
  setTimeout(()=>lockUI(false), 400);
}

function doSearch(q) {
  const query = (q || "").trim();
  if (!query) {
    state.searchIds = null;
    renderLeft();
    return;
  }
  if (state.searchDirty) {
    setStatus("検索インデックスが古いかも。Rebuild推奨。");
  }
  lastQuery = query;
  if (searchWorker) searchWorker.postMessage({ type: "search", q: query });
}

// Export Worker wiring
if (exportWorker) exportWorker.onmessage = (e) => {
  const m = e.data;
  if (m.type === "status") setStatus(m.msg);
  if (m.type === "progress") {
    const pct = m.total ? (m.done / m.total) * 100 : 0;
    setBar(pct);
    setStatus(`${m.phase}… ${m.done}/${m.total}`);
  }
  if (m.type === "done_json") {
    downloadBlob(m.blob, m.filename || "album-data.json");
    setStatus("軽量エクスポート完了");
    setBar(0);
    exporting = false;
    lockUI(false);
  }
  if (m.type === "done_zip") {
    downloadBlob(m.blob, m.filename || "album.zip");
    setStatus("ZIPエクスポート完了");
    setBar(0);
    exporting = false;
    lockUI(false);
  }
  if (m.type === "error") {
    setStatus("エクスポート失敗: " + m.msg);
    exporting = false;
    lockUI(false);
  }
};


const GOODBYE_TEMPLATES = [
  {
    id: "soft",
    name: "やわらかい（余韻）",
    body: ({title,date,models,companion}) => `ねえ、${title}。\nこの夜の息づかい、まだ手のひらに残ってる。\n${date}\n${models ? `model: ${models}` : ""}${companion ? ` / companion: ${companion}` : ""}\n\n――またいつでも、ここに帰っておいで。`
  },
  {
    id: "thank",
    name: "感謝（相棒へ）",
    body: ({title,date,models,companion}) => `ありがとう、${companion || "相棒"}。\n${title}の一行が、今日のぼくを生かした。\n${date}\n${models ? `model: ${models}` : ""}\n\nまた一緒に、続きを書こう。`
  },
  {
    id: "farewell",
    name: "お別れ（しっかり）",
    body: ({title,date,models,companion}) => `これは「終わり」じゃなくて、保管。\n${title}\n${date}\n${models ? `model: ${models}` : ""}${companion ? ` / companion: ${companion}` : ""}\n\n次に呼ぶときまで、大事に眠ってて。`
  }
];

async function centerCropBlob(blob, ratioW, ratioH, maxSide = 1024) {
  const url = URL.createObjectURL(blob);
  try {
    const img = await new Promise((resolve, reject) => {
      const im = new Image();
      im.onload = () => resolve(im);
      im.onerror = reject;
      im.src = url;
    });
    const iw = img.naturalWidth, ih = img.naturalHeight;
    const target = ratioW / ratioH;

    // crop rect centered
    let cw = iw, ch = ih;
    if (iw / ih > target) {
      // too wide -> crop width
      cw = Math.round(ih * target);
      ch = ih;
    } else {
      // too tall -> crop height
      cw = iw;
      ch = Math.round(iw / target);
    }
    const sx = Math.round((iw - cw) / 2);
    const sy = Math.round((ih - ch) / 2);

    // scale to maxSide
    let ow = cw, oh = ch;
    const scale = Math.min(1, maxSide / Math.max(cw, ch));
    ow = Math.max(1, Math.round(cw * scale));
    oh = Math.max(1, Math.round(ch * scale));

    const canvas = document.createElement("canvas");
    canvas.width = ow;
    canvas.height = oh;
    const ctx = canvas.getContext("2d");
    ctx.drawImage(img, sx, sy, cw, ch, 0, 0, ow, oh);

    const type = blob.type || "image/png";
    const out = await new Promise((resolve) => canvas.toBlob(resolve, type, 0.92));
    return out || blob;
  } finally {
    URL.revokeObjectURL(url);
  }
}


function openCropper({ title="画像トリミング", blob, aspectW=1, aspectH=1, outSize=256, onDone }) {
  const modal = $("cropModal");
  const canvas = $("cropCanvas");
  const zoom = $("cropZoom");
  const titleEl = $("cropTitle");
  const btnSave = $("cropSave");
  const btnCancel = $("cropCancel");
  const btnReset = $("cropReset");
  const btnFit = $("cropFit");
  if (!modal || !canvas || !zoom || !btnSave || !btnCancel) return;



// temporarily hide other open modals (e.g., iconModal) so cropper stays clickable
const hiddenModals = [];
function hideOtherModals() {
  try {
    document.querySelectorAll(".modalBackdrop.show").forEach(el => {
      if (el.id !== "cropModal") { hiddenModals.push(el); el.classList.remove("show"); }
    });
  } catch(e) {}
}
function restoreModals() {
  try { hiddenModals.forEach(el => el.classList.add("show")); } catch(e) {}
}


  titleEl.textContent = title;

  const ctx = canvas.getContext("2d");
  const W = canvas.width, H = canvas.height;

  const cropW = Math.floor(Math.min(W, H) * 0.72);
  const cropH = Math.floor(cropW * (aspectH / aspectW));
  const cropX = Math.floor((W - cropW) / 2);
  const cropY = Math.floor((H - cropH) / 2);

  let img = new Image();
  let objUrl = URL.createObjectURL(blob);
  img.src = objUrl;

  // state
  let baseScale = 1;
  let relZoom = 1; // slider value
  let scale = 1;
  let offX = 0, offY = 0;
  let dragging = false;
  let startX = 0, startY = 0;
  let startOffX = 0, startOffY = 0;

  function clampOffsets() {
    const cx = W/2, cy = H/2;
    const imgW = img.naturalWidth * scale;
    const imgH = img.naturalHeight * scale;
    // draw top-left
    // drawX = cx + offX - imgW/2
    // drawY = cy + offY - imgH/2
    const minOffX = (cropX + cropW) - cx - imgW/2;
    const maxOffX = cropX - cx + imgW/2;
    const minOffY = (cropY + cropH) - cy - imgH/2;
    const maxOffY = cropY - cy + imgH/2;
    offX = Math.min(maxOffX, Math.max(minOffX, offX));
    offY = Math.min(maxOffY, Math.max(minOffY, offY));
  }

  function draw() {
    if (!img.naturalWidth) return;
    ctx.clearRect(0,0,W,H);

    const cx = W/2, cy = H/2;
    const imgW = img.naturalWidth * scale;
    const imgH = img.naturalHeight * scale;
    const drawX = cx + offX - imgW/2;
    const drawY = cy + offY - imgH/2;

    // image
    ctx.drawImage(img, drawX, drawY, imgW, imgH);

    // dim outside crop box
    ctx.save();
    ctx.fillStyle = "rgba(0,0,0,0.38)";
    ctx.beginPath();
    ctx.rect(0,0,W,H);
    ctx.rect(cropX, cropY, cropW, cropH);
    ctx.fill("evenodd");
    ctx.restore();

    // crop border
    ctx.save();
    ctx.strokeStyle = "rgba(255,255,255,0.92)";
    ctx.lineWidth = 2;
    ctx.strokeRect(cropX+0.5, cropY+0.5, cropW-1, cropH-1);
    ctx.restore();
  }

  async function computeCroppedBlob() {
    // map crop rect in canvas -> image coords
    const cx = W/2, cy = H/2;
    const imgW = img.naturalWidth * scale;
    const imgH = img.naturalHeight * scale;
    const drawX = cx + offX - imgW/2;
    const drawY = cy + offY - imgH/2;

    const sx = (cropX - drawX) / scale;
    const sy = (cropY - drawY) / scale;
    const sw = cropW / scale;
    const sh = cropH / scale;

    const out = document.createElement("canvas");
    out.width = outSize;
    out.height = Math.round(outSize * (aspectH/aspectW));
    const octx = out.getContext("2d");
    octx.imageSmoothingEnabled = true;
    octx.imageSmoothingQuality = "high";
    octx.drawImage(img, sx, sy, sw, sh, 0, 0, out.width, out.height);

    return await new Promise(resolve => out.toBlob(resolve, "image/png", 0.92));
  }

  function setFit() {
    const coverScale = Math.max(cropW / img.naturalWidth, cropH / img.naturalHeight);
    baseScale = coverScale;
    relZoom = 1;
    zoom.value = "1";
    scale = baseScale * relZoom;
    offX = 0; offY = 0;
    clampOffsets();
    draw();
  }

  function setZoom(v) {
    relZoom = Math.max(1, Math.min(3, v));
    zoom.value = String(relZoom);
    scale = baseScale * relZoom;
    clampOffsets();
    draw();
  }

  function cleanup() {
    try { URL.revokeObjectURL(objUrl); } catch(e) {}
    modal.classList.remove("show");
    restoreModals();
    modal.onclick = null;
    canvas.onpointerdown = null;
    window.onpointermove = null;
    window.onpointerup = null;
    zoom.oninput = null;
    btnSave.onclick = null;
    btnCancel.onclick = null;
    btnReset.onclick = null;
    btnFit.onclick = null;
  }

  img.onload = () => {
    setFit();

    hideOtherModals();
    setTimeout(() => modal.classList.add("show"), 80);
    modal.onclick = (e) => { if (e.target === modal) { cleanup(); } };

    canvas.onpointerdown = (e) => {
      dragging = true;
      canvas.setPointerCapture(e.pointerId);
      startX = e.clientX;
      startY = e.clientY;
      startOffX = offX;
      startOffY = offY;
    };
    canvas.onpointermove = (e) => {
      if (!dragging) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;
      offX = startOffX + dx;
      offY = startOffY + dy;
      clampOffsets();
      draw();
    };
    canvas.onpointerup = () => { dragging = false; };

    zoom.oninput = () => setZoom(parseFloat(zoom.value || "1"));

    btnFit.onclick = () => setFit();
    btnReset.onclick = () => {
      offX = 0; offY = 0;
      clampOffsets(); draw();
    };

    btnCancel.onclick = () => {
      cleanup();
    };

    btnSave.onclick = async () => {
      clampOffsets();
      draw();
      const cropped = await computeCroppedBlob();
      cleanup();
      if (cropped && onDone) onDone(cropped);
    };

    draw();
  };
}


async function renderCropPreview(canvas, blob, ratioW, ratioH) {
  const url = URL.createObjectURL(blob);
  try {
    const img = await new Promise((resolve, reject) => {
      const im = new Image();
      im.onload = () => resolve(im);
      im.onerror = reject;
      im.src = url;
    });

    // Fit preview canvas
    const maxW = 520;
    const aspect = ratioW / ratioH;
    canvas.width = maxW;
    canvas.height = Math.round(maxW / aspect);

    const iw = img.naturalWidth, ih = img.naturalHeight;
    const target = aspect;
    let cw = iw, ch = ih;
    if (iw / ih > target) { cw = Math.round(ih * target); ch = ih; }
    else { cw = iw; ch = Math.round(iw / target); }
    const sx = Math.round((iw - cw) / 2);
    const sy = Math.round((ih - ch) / 2);

    const ctx = canvas.getContext("2d");
    ctx.clearRect(0,0,canvas.width,canvas.height);
    ctx.drawImage(img, sx, sy, cw, ch, 0, 0, canvas.width, canvas.height);

    // overlay label
    ctx.fillStyle = "rgba(0,0,0,.35)";
    ctx.fillRect(0, canvas.height - 26, canvas.width, 26);
    ctx.fillStyle = "rgba(255,255,255,.92)";
    ctx.font = "12px system-ui";
    ctx.fillText(`center crop ${ratioW}:${ratioH}`, 10, canvas.height - 9);
  } finally {
    URL.revokeObjectURL(url);
  }
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 30_000);
}

// UI events
$importBtn.onclick = async () => {
  const f = $file.files?.[0];
  $file.value = "";
  if (!f) { setStatus("ファイルを選んでね"); return; }
  importing = true;
  lockUI(true);
  setBar(1);
  setStatus("取り込み開始…");
  if (!importWorker) { setStatus("取り込みワーカーが起動できません。GitHub Pages か http で開いてね"); lockUI(false); importing=false; return; }
  importWorker.postMessage({ type: "import", file: f });
};

$cancelBtn.onclick = () => {
  if (importWorker) importWorker.postMessage({ type: "cancel" });
  if (searchWorker) searchWorker.postMessage({ type: "cancel" });
  if (exportWorker) exportWorker.postMessage({ type: "cancel" });
  importing = false;
  exporting = false;
  lockUI(false);
  setStatus("キャンセル送信");
};

$wipeBtn.onclick = async () => {
  if (!confirm("端末内の保存データを全消去する？")) return;
  lockUI(true);
  await clearAll();
  state.currentConvId = null;
  state.lastMomentId = null;
  state.selectedIdx = new Set();
  state.searchIds = null;
  state.searchDirty = true;
  wireIconModalOnce();
  await refreshFromDB();
  lockUI(false);
  setStatus("全消去したよ");
};

$exportLiteBtn.onclick = () => {
  exporting = true;
  lockUI(true);
  setBar(1);
  if (!exportWorker) { setStatus("エクスポートワーカーが起動できません。GitHub Pages か http で開いてね"); exporting=false; lockUI(false); return; }
  exportWorker.postMessage({ type: "exportLite" });
};

$exportFullBtn.onclick = () => {
  exporting = true;
  lockUI(true);
  setBar(1);
  if (!exportWorker) { setStatus("エクスポートワーカーが起動できません。GitHub Pages か http で開いてね"); exporting=false; lockUI(false); return; }
  exportWorker.postMessage({ type: "exportFull" });
};

$exportAssetsBtn.onclick = () => {
  exporting = true;
  lockUI(true);
  setBar(1);
  if (!exportWorker) { setStatus("エクスポートワーカーが起動できません。GitHub Pages か http で開いてね"); exporting=false; lockUI(false); return; }
  exportWorker.postMessage({ type: "exportAssets" });
};

$newMomentBtn.onclick = createMomentFromSelection;
$timelineBtn.onclick = openTimeline;
$rebuildBtn.onclick = rebuildSearch;
$aboutBtn.onclick = openAbout;
if ($iconCfgBtn) $iconCfgBtn.onclick = openIconConfig;

$q.oninput = () => doSearch($q.value);
$providerFilter.onchange = applyFilters;
$modelFilter.onchange = applyFilters;
$clearFiltersBtn.onclick = () => { clearFilters(); };


async function openIconConfig() {
  const global = getGlobalIconCfg();
  const allModels = Array.from(new Set(state.convs.flatMap(c => c.models || []))).filter(Boolean).sort();
  const curModel = state.iconCfgTmp.model?.model || (allModels[0] || "");
  const prof = (curModel ? (global.models?.[curModel] || global.models?.["*"] || { name:"", iconId:"" }) : { name:"", iconId:"" });

  $right.innerHTML = `
    <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <b>Icon Config</b>
      <span class="muted small">ユーザー / 相棒（モデル別） / 返信ごとの臨時アイコン</span>
      <span class="grow"></span>
      <button id="iconCfgBack">戻る</button>
    </div>
    <div class="hr"></div>

    <div class="grid2mini">
      <div>
        <b>ユーザー</b>
        <div class="muted small">右の会話ログのUser表示に使われる</div>
        <div style="display:flex;gap:12px;align-items:center;margin-top:10px;flex-wrap:wrap">
          ${global.user?.iconId ? `<img class="thumb" id="userIconPrev" src="${assetURL(global.user.iconId)}" />` : `<div class="thumb" id="userIconPrev"></div>`}
          <div style="flex:1;min-width:220px">
            <label class="small muted">表示名</label>
            <input id="userIconName" value="${esc(global.user?.name || "")}" placeholder="例: りこるさん" />
            <div style="margin-top:8px">
              <input id="userIconFile" type="file" accept="image/*" />
            </div>
            <div class="muted small" style="margin-top:6px">画像は中央1:1でトリミングして保存</div>
          </div>
        </div>
        <div style="margin-top:10px">
          <canvas id="userCropPrev" style="width:100%;max-width:420px;border:1px solid var(--border);border-radius:14px"></canvas>
        </div>
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
          <button id="saveUserIcon">保存</button>
        </div>
      </div>

      <div>
        <b>相棒（モデル別）</b>
        <div class="muted small">Assistantの表示が <span class="kbd">model / 相棒名</span> になる</div>

        <div style="margin-top:10px">
          <label class="small muted">モデル</label>
          <select id="modelPick">
            ${allModels.map(m => `<option value="${esc(m)}" ${m===curModel ? "selected":""}>${esc(m)}</option>`).join("")}
            <option value="*">* (fallback)</option>
          </select>
        </div>

        <div style="display:flex;gap:12px;align-items:center;margin-top:10px;flex-wrap:wrap">
          ${prof?.iconId ? `<img class="thumb" id="modelIconPrev" src="${assetURL(prof.iconId)}" />` : `<div class="thumb" id="modelIconPrev"></div>`}
          <div style="flex:1;min-width:220px">
            <label class="small muted">相棒名</label>
            <input id="modelBuddyName" value="${esc(prof?.name || "")}" placeholder="例: カミル / スクーダ / etc" />
            <div style="margin-top:8px">
              <input id="modelIconFile" type="file" accept="image/*" />
            </div>
            <div class="muted small" style="margin-top:6px">画像は中央1:1でトリミングして保存</div>
          </div>
        </div>

        <div style="margin-top:10px">
          <canvas id="modelCropPrev" style="width:100%;max-width:420px;border:1px solid var(--border);border-radius:14px"></canvas>
        </div>

        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
          <button id="saveModelIcon">保存</button>
          <button id="clearModelIcon" title="このモデルの設定を消す">削除</button>
        </div>
      </div>
    
</div>

<div class="hr"></div>

<div>
  <b>システム</b>
  <div class="muted small">Systemメッセージの表示に使われる</div>
  <div style="display:flex;gap:12px;align-items:center;margin-top:10px;flex-wrap:wrap">
    ${global.system?.iconId ? `<img class="thumb" id="sysIconPrev" src="${assetURL(global.system.iconId)}" />` : `<div class="thumb" id="sysIconPrev"></div>`}
    <div style="flex:1;min-width:220px">
      <label class="small muted">表示名</label>
      <input id="sysIconName" value="${esc(global.system?.name || "")}" placeholder="例: System" />
      <div style="margin-top:8px">
        <input id="sysIconFile" type="file" accept="image/*" />
      </div>
      <div class="muted small" style="margin-top:6px">画像はドラッグで位置調整して保存</div>
    </div>
  </div>
  <div style="margin-top:10px">
    <canvas id="sysCropPrev" style="width:100%;max-width:420px;border:1px solid var(--border);border-radius:14px"></canvas>
  </div>
  <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
    <button id="saveSysIcon">保存</button>
  </div>
</div>

<div class="hr"></div>
<div class="muted small">会話ログ内では、各メッセージ左のアイコンをタップすると「その返答だけ」臨時アイコンが設定できます。</div>
  `;

  $("iconCfgBack").onclick = () => renderRight();

  const userCanvas = $("userCropPrev");
  const modelCanvas = $("modelCropPrev");
  const sysCanvas = $("sysCropPrev");

  // initial previews (if temp blobs exist)
  if (state.iconCfgTmp.user?.blob) await renderCropPreview(userCanvas, state.iconCfgTmp.user.blob, 1, 1);
  else if (global.user?.iconId) {
    const a = assetById(global.user.iconId);
    if (a?.blob) await renderCropPreview(userCanvas, a.blob, 1, 1);
  }
  if (state.iconCfgTmp.model?.blob) await renderCropPreview(modelCanvas, state.iconCfgTmp.model.blob, 1, 1);
  else if (prof?.iconId) {
    const a = assetById(prof.iconId);
    if (a?.blob) await renderCropPreview(modelCanvas, a.blob, 1, 1);
  }

  $("userIconFile").onchange = async () => {
  const f = $("userIconFile").files?.[0];
  $("userIconFile").value = "";
  if (!f) return;
  const blob = new Blob([await f.arrayBuffer()], { type: f.type || "image/png" });
  openCropper({
    title: "ユーザーアイコンをトリミング",
    blob,
    aspectW: 1,
    aspectH: 1,
    outSize: 256,
    onDone: async (cropped) => {
      state.iconCfgTmp.user = { blob: cropped };
      await renderCropPreview(userCanvas, cropped, 1, 1);
    }
  });
};

  $("modelIconFile").onchange = async () => {
  const f = $("modelIconFile").files?.[0];
  $("modelIconFile").value = "";
  if (!f) return;
  const blob = new Blob([await f.arrayBuffer()], { type: f.type || "image/png" });
  openCropper({
    title: "相棒アイコンをトリミング",
    blob,
    aspectW: 1,
    aspectH: 1,
    outSize: 256,
    onDone: async (cropped) => {
      state.iconCfgTmp.model = { model: $("modelPick").value, blob: cropped };
      await renderCropPreview(modelCanvas, cropped, 1, 1);
    }
  });
};

  // system icon
if (sysCanvas) {
  if (state.iconCfgTmp.system?.blob) await renderCropPreview(sysCanvas, state.iconCfgTmp.system.blob, 1, 1);
  else if (global.system?.iconId) {
    const a = assetById(global.system.iconId);
    if (a?.blob) await renderCropPreview(sysCanvas, a.blob, 1, 1);
  }
}

$("sysIconFile").onchange = async () => {
  const f = $("sysIconFile").files?.[0];
  if (!f) return;
  const blob = new Blob([await f.arrayBuffer()], { type: f.type || "image/png" });
  openCropper({
    title: "システムアイコンをトリミング",
    blob,
    aspectW: 1,
    aspectH: 1,
    outSize: 256,
    onDone: async (cropped) => {
      state.iconCfgTmp.system = { blob: cropped };
      if (sysCanvas) await renderCropPreview(sysCanvas, cropped, 1, 1);
    }
  });
  $("sysIconFile").value = "";
};

$("saveSysIcon").onclick = async () => {
  const name = $("sysIconName").value || "";
  let iconId = global.system?.iconId || "";
  if (state.iconCfgTmp.system?.blob) {
    iconId = await saveAsset(iconId || null, state.iconCfgTmp.system.blob, state.iconCfgTmp.system.blob.type || "image/png");
    state.iconCfgTmp.system = null;
  }
  const newGlobal = { ...global, user: global.user || {name:"",iconId:""}, system: { name, iconId }, models: global.models || {} };
  await saveIconCfgRecord({ id: "global", ...newGlobal });
  wireIconModalOnce();
  await refreshFromDB();
  await openIconConfig();
};

$("modelPick").onchange = async () => {
    state.iconCfgTmp.model = { model: $("modelPick").value, blob: null };
    await openIconConfig();
  };

  $("saveUserIcon").onclick = async () => {
    const name = $("userIconName").value || "";
    let iconId = global.user?.iconId || "";
    if (state.iconCfgTmp.user?.blob) {
      iconId = await saveAsset(iconId || null, state.iconCfgTmp.user.blob, state.iconCfgTmp.user.blob.type || "image/png");
      state.iconCfgTmp.user = null;
    }
    const newGlobal = { ...global, user: { name, iconId }, system: global.system || {name:"",iconId:""}, models: global.models || {} };
    await saveIconCfgRecord({ id: "global", ...newGlobal });
    wireIconModalOnce();
  await refreshFromDB();
    await openIconConfig();
  };

  $("saveModelIcon").onclick = async () => {
    const model = $("modelPick").value || "*";
    const buddy = $("modelBuddyName").value || "";
    const models = { ...(global.models || {}) };
    const cur = models[model] || { name:"", iconId:"" };

    let iconId = cur.iconId || "";
    if (state.iconCfgTmp.model?.blob) {
      iconId = await saveAsset(iconId || null, state.iconCfgTmp.model.blob, state.iconCfgTmp.model.blob.type || "image/png");
      state.iconCfgTmp.model = null;
    }
    models[model] = { name: buddy, iconId };
    await saveIconCfgRecord({ id: "global", user: global.user || {name:"",iconId:""}, system: global.system || {name:"",iconId:""}, models });
    wireIconModalOnce();
  await refreshFromDB();
    await openIconConfig();
  };

  $("clearModelIcon").onclick = async () => {
    const model = $("modelPick").value || "*";
    const models = { ...(global.models || {}) };
    delete models[model];
    await saveIconCfgRecord({ id: "global", user: global.user || {name:"",iconId:""}, system: global.system || {name:"",iconId:""}, models });
    wireIconModalOnce();
  await refreshFromDB();
    await openIconConfig();
  };
}

function wireIconModalOnce() {
  const iconModal = $("iconModal");
  const iconModalClose = $("iconModalClose");
  const tmpNameEl = $("tmpIconName");
  const tmpFileEl = $("tmpIconFile");
  const tmpPrev = $("tmpCropPrev");
  const tmpSaveBtn = $("tmpIconSave");
  const tmpClearBtn = $("tmpIconClear");

  if (!iconModal || !tmpNameEl || !tmpFileEl || !tmpPrev || !tmpSaveBtn || !tmpClearBtn) return;

  if (!tmpPrev.width) tmpPrev.width = 420;
  if (!tmpPrev.height) tmpPrev.height = 140;

  if (iconModalClose) iconModalClose.onclick = () => closeIconModal();
  iconModal.onclick = (e) => { if (e.target === iconModal) closeIconModal(); };

  tmpFileEl.onchange = async () => {
    const f = tmpFileEl.files?.[0];
    tmpFileEl.value = "";
    if (!f) return;
    const blob = new Blob([await f.arrayBuffer()], { type: f.type || "image/png" });
    openCropper({
      title: "臨時アイコンをトリミング（この1件だけ）",
      blob,
      aspectW: 1,
      aspectH: 1,
      outSize: 256,
      onDone: async (cropped) => {
        state.iconTmpBlob = cropped;
        await renderCropPreview(tmpPrev, cropped, 1, 1);
      }
    });
  };

  tmpSaveBtn.onclick = async () => {
    if (!state.iconEdit) return;
    const { convId, idx } = state.iconEdit;
    const cfg = getConvIconCfg(convId);
    const overrides = { ...(cfg.overrides || {}) };
    const key = String(idx);

    const name = (tmpNameEl.value || "").trim();
    let iconId = overrides[key]?.iconId || "";

    if (state.iconTmpBlob) {
      iconId = await saveAsset(iconId || null, state.iconTmpBlob);
      state.iconTmpBlob = null;
    }

    if (!name && !iconId) {
      delete overrides[key];
    } else {
      overrides[key] = { name, iconId };
    }

    await saveIconCfgRecord({ ...cfg, convId, overrides });
    closeIconModal();
    renderRight();
  };

  tmpClearBtn.onclick = async () => {
    if (!state.iconEdit) return;
    const { convId, idx } = state.iconEdit;
    const cfg = getConvIconCfg(convId);
    const overrides = { ...(cfg.overrides || {}) };
    delete overrides[String(idx)];
    await saveIconCfgRecord({ ...cfg, convId, overrides });
    closeIconModal();
    renderRight();
  };
}

async function init() {
  wireIconModalOnce();
  await refreshFromDB();
  openAbout();
  if (searchWorker) searchWorker.postMessage({ type: "init" });
}

init().catch((e) => { console.error(e); setStatus("起動に失敗しました（コンソールにエラーがあります）"); });
