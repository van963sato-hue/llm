/* Search Worker: builds a simple 3-gram inverted index (works for JP/EN).
   - No external deps.
   - Indexes: conversations (chunked), moments, prompts, history (including memory).
*/
const DB_NAME = "llm_memory_album_v2";
const DB_VER = 3;

const STORES = {
  conv: "conversations",
  moment: "moments",
  prompt: "prompt_profiles",
  history: "history_events",
  iconcfg: "icon_config"
};

let cancelled = false;

let gramMap = new Map(); // gram -> Int32Array of docIdx (stored as JS arrays during build)
let gramLists = new Map(); // gram -> Array<number> for building
let docs = []; // {id, kind, refId, textPreview?}
let ready = false;

function postStatus(msg) { postMessage({ type: "status", msg }); }
function postProg(done, total, phase="build") { postMessage({ type: "progress", phase, done, total }); }

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = () => {
      const db = req.result;
      for (const st of Object.values(STORES)) {
        if (!db.objectStoreNames.contains(st)) db.createObjectStore(st, { keyPath: "id" });
      }
      try {
        const conv = req.transaction.objectStore(STORES.conv);
        if (conv && !conv.indexNames.contains("byUpdatedAt")) conv.createIndex("byUpdatedAt", "updatedAt", { unique: false });
      } catch (e) {}
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function getAll(db, store) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const st = tx.objectStore(store);
    const req = st.getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

function norm(s) {
  return String(s || "")
    .normalize("NFKC")
    .toLowerCase();
}

function gramsOf(s) {
  const t = norm(s);
  const out = [];
  if (t.length < 3) return out;
  for (let i = 0; i <= t.length - 3; i++) out.push(t.slice(i, i + 3));
  // Unique to reduce index size per doc
  return Array.from(new Set(out));
}

function chunkText(text, maxLen = 6000) {
  const chunks = [];
  let buf = "";
  for (const line of String(text).split("\n")) {
    if ((buf.length + line.length + 1) > maxLen) { chunks.push(buf); buf = ""; }
    buf += (buf ? "\n" : "") + line;
  }
  if (buf.trim()) chunks.push(buf);
  return chunks;
}

async function buildIndex() {
  cancelled = false;
  ready = false;
  gramLists = new Map();
  docs = [];

  postStatus("検索インデックス作成中…");
  const db = await openDB();
  const convs = await getAll(db, STORES.conv);
  const moments = await getAll(db, STORES.moment);
  const prompts = await getAll(db, STORES.prompt);
  const history = await getAll(db, STORES.history);

  let docIdx = 0;

  // conversations: chunked
  for (let i = 0; i < convs.length; i++) {
    if (cancelled) return;
    const c = convs[i];
    const title = c.title || "";
    const body = (c.messages || []).map(m => `${m.role}\n${m.text}`).join("\n\n");
    const chunks = chunkText(title + "\n" + body, 6000);

    for (let k = 0; k < chunks.length; k++) {
      const id = `c:${c.id}:${k}`;
      docs.push({ id, kind: "conv", refId: c.id, preview: title });
      const grams = gramsOf(chunks[k]);
      for (const g of grams) {
        let arr = gramLists.get(g);
        if (!arr) { arr = []; gramLists.set(g, arr); }
        arr.push(docIdx);
      }
      docIdx++;
    }
    if (i % 10 === 0) postProg(i, convs.length, "conv");
    await new Promise(r => setTimeout(r, 0));
  }

  // moments
  for (const m of moments) {
    if (cancelled) return;
    const id = `m:${m.id}`;
    const text = `${m.title||""}\n${(m.tags||[]).join(" ")}\n${m.goodbye||""}`;
    docs.push({ id, kind: "moment", refId: m.id, preview: m.title||"Moment" });
    for (const g of gramsOf(text)) {
      let arr = gramLists.get(g);
      if (!arr) { arr = []; gramLists.set(g, arr); }
      arr.push(docIdx);
    }
    docIdx++;
  }

  // prompts
  for (const p of prompts) {
    if (cancelled) return;
    const id = `p:${p.id}`;
    const text = `${p.name||""}\n${p.content||""}\n${p.companion||""}`;
    docs.push({ id, kind: "prompt", refId: p.id, preview: p.name||"Prompt" });
    for (const g of gramsOf(text)) {
      let arr = gramLists.get(g);
      if (!arr) { arr = []; gramLists.set(g, arr); }
      arr.push(docIdx);
    }
    docIdx++;
  }

  // history (include memory)
  for (const h of history) {
    if (cancelled) return;
    const id = `h:${h.id}`;
    const text = `${h.title||""}\n${h.detail||""}\n${h.memory||""}\n${h.model||""}\n${h.provider||""}`;
    docs.push({ id, kind: "history", refId: h.id, preview: h.title||"History" });
    for (const g of gramsOf(text)) {
      let arr = gramLists.get(g);
      if (!arr) { arr = []; gramLists.set(g, arr); }
      arr.push(docIdx);
    }
    docIdx++;
  }

  // Convert arrays to typed arrays (sorted & unique)
  gramMap = new Map();
  let n = 0;
  for (const [g, arr] of gramLists.entries()) {
    arr.sort((a,b)=>a-b);
    // unique
    const uniq = [];
    let last = -1;
    for (const v of arr) { if (v !== last) { uniq.push(v); last = v; } }
    gramMap.set(g, new Int32Array(uniq));
    n++;
    if (n % 5000 === 0) postProg(n, gramLists.size, "finalize");
    if (n % 2000 === 0) await new Promise(r => setTimeout(r, 0));
  }

  ready = true;
  postMessage({ type: "ready", docs: docs.length, grams: gramMap.size });
  postStatus("検索準備OK");
}

function search(q) {
  const query = norm(q).trim();
  if (!ready || !query) return { convIds: [], momentIds: [], promptIds: [], historyIds: [] };

  // For very short query, fallback to scanning doc previews (cheap) + chunk heuristic (still okay)
  if (query.length < 3) {
    const hit = { convIds: new Set(), momentIds: new Set(), promptIds: new Set(), historyIds: new Set() };
    for (const d of docs) {
      if ((d.preview || "").toLowerCase().includes(query)) {
        if (d.kind === "conv") hit.convIds.add(d.refId);
        if (d.kind === "moment") hit.momentIds.add(d.refId);
        if (d.kind === "prompt") hit.promptIds.add(d.refId);
        if (d.kind === "history") hit.historyIds.add(d.refId);
      }
    }
    return {
      convIds: Array.from(hit.convIds).slice(0, 50),
      momentIds: Array.from(hit.momentIds).slice(0, 50),
      promptIds: Array.from(hit.promptIds).slice(0, 50),
      historyIds: Array.from(hit.historyIds).slice(0, 50),
    };
  }

  const qgrams = gramsOf(query);
  if (!qgrams.length) return { convIds: [], momentIds: [], promptIds: [], historyIds: [] };

  // Get postings lists, pick smallest to seed
  const lists = qgrams.map(g => gramMap.get(g)).filter(Boolean);
  if (!lists.length) return { convIds: [], momentIds: [], promptIds: [], historyIds: [] };

  lists.sort((a,b)=>a.length - b.length);

  const counts = new Map(); // docIdx -> count
  const needed = lists.length;

  // Seed from smallest list
  for (const v of lists[0]) counts.set(v, 1);

  for (let i = 1; i < lists.length; i++) {
    const arr = lists[i];
    // Mark membership quickly using pointer walk (since both are sorted)
    // We'll increment count for docs that exist in counts.
    let j = 0;
    for (const key of counts.keys()) {
      // counts keys iteration order is insertion; not sorted. We'll do membership check by binary search instead.
      // (Counts size is bounded by smallest list length, so this is OK.)
    }
    // Better: iterate arr and bump if exists
    for (const v of arr) {
      const c = counts.get(v);
      if (c) counts.set(v, c + 1);
    }
    // prune counts (keep only those with i+1 hits)
    for (const [k, c] of counts.entries()) {
      if (c < (i + 1)) counts.delete(k);
    }
    if (counts.size === 0) break;
  }

  const hit = { convIds: new Set(), momentIds: new Set(), promptIds: new Set(), historyIds: new Set() };
  for (const k of counts.keys()) {
    const d = docs[k];
    if (!d) continue;
    if (d.kind === "conv") hit.convIds.add(d.refId);
    if (d.kind === "moment") hit.momentIds.add(d.refId);
    if (d.kind === "prompt") hit.promptIds.add(d.refId);
    if (d.kind === "history") hit.historyIds.add(d.refId);
  }

  return {
    convIds: Array.from(hit.convIds).slice(0, 50),
    momentIds: Array.from(hit.momentIds).slice(0, 50),
    promptIds: Array.from(hit.promptIds).slice(0, 50),
    historyIds: Array.from(hit.historyIds).slice(0, 50),
  };
}

onmessage = async (e) => {
  const { type, q } = e.data || {};
  if (type === "cancel") { cancelled = true; postStatus("キャンセルしました"); return; }
  if (type === "init" || type === "rebuild") {
    try { await buildIndex(); }
    catch (err) { postMessage({ type: "error", msg: String(err?.message || err) }); }
  } else if (type === "search") {
    postMessage({ type: "result", q: String(q||""), data: search(String(q||"")) });
  }
};
