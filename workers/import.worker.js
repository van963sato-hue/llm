/* Import Worker: reads ChatGPT Export ZIP (conversations.json inside) OR JSON directly.
   - No external deps: includes a minimal ZIP reader for a single file + deflate via DecompressionStream.
   - Writes normalized sessions into IndexedDB in batches.
   - Generates basic auto History events: first_contact and model_started.
*/
const DB_NAME = "llm_memory_album_v2";
const DB_VER = 3;

const STORES = {
  conv: "conversations",
  moment: "moments",
  prompt: "prompt_profiles",
  asset: "assets",
  label: "model_labels",
  history: "history_events",
  meta: "meta",
  iconcfg: "icon_config"
};

let cancelled = false;

function postStatus(msg) { postMessage({ type: "status", msg }); }
function postProg(done, total, phase = "import") { postMessage({ type: "progress", phase, done, total }); }

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

function putBatch(db, store, items) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readwrite");
    const st = tx.objectStore(store);
    for (const it of items) st.put(it);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

function uid(prefix="id") {
  return `${prefix}_${crypto.getRandomValues(new Uint32Array(2)).join("")}_${Date.now()}`;
}

function u8view(ab) { return new Uint8Array(ab); }

function findEOCD(u8) {
  // EOCD signature 0x06054b50. Search backwards.
  for (let i = u8.length - 22; i >= 0 && i >= u8.length - 70000; i--) {
    if (u8[i] === 0x50 && u8[i+1] === 0x4b && u8[i+2] === 0x05 && u8[i+3] === 0x06) return i;
  }
  return -1;
}

function readU16(u8, off) { return u8[off] | (u8[off+1] << 8); }
function readU32(u8, off) { return (u8[off]) | (u8[off+1] << 8) | (u8[off+2] << 16) | (u8[off+3] << 24) >>> 0; }

async function readBlobSlice(file, start, end) {
  const blob = file.slice(start, end);
  return await blob.arrayBuffer();
}

async function unzipFindFile(file, targetNames) {
  // Read tail to find EOCD
  const tailSize = Math.min(70000, file.size);
  const tailAb = await readBlobSlice(file, file.size - tailSize, file.size);
  const tail = u8view(tailAb);
  const eocdRel = findEOCD(tail);
  if (eocdRel < 0) throw new Error("ZIPのEOCDが見つからない");

  const eocd = (file.size - tailSize) + eocdRel;

  // EOCD structure
  // offset 12: cd size, 16: cd offset
  // But we are reading from tail buffer; compute absolute offsets
  const cdSize = readU32(tail, eocdRel + 12);
  const cdOffset = readU32(tail, eocdRel + 16);

  // Read central directory
  const cdAb = await readBlobSlice(file, cdOffset, cdOffset + cdSize);
  const cd = u8view(cdAb);

  const dec = new TextDecoder("utf-8");
  let p = 0;
  while (p + 46 <= cd.length) {
    // central file header signature 0x02014b50
    if (!(cd[p] === 0x50 && cd[p+1] === 0x4b && cd[p+2] === 0x01 && cd[p+3] === 0x02)) break;

    const compMethod = readU16(cd, p + 10);
    const crc32 = readU32(cd, p + 16);
    const compSize = readU32(cd, p + 20);
    const uncompSize = readU32(cd, p + 24);
    const nameLen = readU16(cd, p + 28);
    const extraLen = readU16(cd, p + 30);
    const commentLen = readU16(cd, p + 32);
    const localOff = readU32(cd, p + 42);

    const name = dec.decode(cd.slice(p + 46, p + 46 + nameLen));
    const next = p + 46 + nameLen + extraLen + commentLen;

    if (targetNames.includes(name)) {
      // Read local header to find data start
      const lhAb = await readBlobSlice(file, localOff, localOff + 30);
      const lh = u8view(lhAb);
      if (!(lh[0] === 0x50 && lh[1] === 0x4b && lh[2] === 0x03 && lh[3] === 0x04)) throw new Error("ZIP local headerが不正");
      const lNameLen = readU16(lh, 26);
      const lExtraLen = readU16(lh, 28);
      const dataStart = localOff + 30 + lNameLen + lExtraLen;

      // IMPORTANT: do NOT materialize the whole file in memory.
      // Return a ReadableStream of (optionally) decompressed bytes.
      const compBlob = file.slice(dataStart, dataStart + compSize);

      if (compMethod === 0) {
        return { name, stream: compBlob.stream(), crc32, uncompSize };
      }
      if (compMethod === 8) {
        if (typeof DecompressionStream === "undefined") throw new Error("このブラウザはdeflate-raw解凍に非対応です（ZIPを解凍してjsonを入れてね）");
        const ds = new DecompressionStream("deflate-raw");
        return { name, stream: compBlob.stream().pipeThrough(ds), crc32, uncompSize };
      }
      throw new Error("未対応の圧縮方式: " + compMethod);
    }
    p = next;
  }

  throw new Error("ZIP内に対象ファイルが見つからない（conversations.json など）");
}

// Stream parser for gigantic ChatGPT export conversations.json.
// Expected shape: [ {conversation}, {conversation}, ... ]
// This avoids JSON.parse() on the whole 1GB file.
async function* streamParseTopLevelArrayOfObjects(byteStream, totalBytes, onBytes) {
  const reader = byteStream.getReader();
  const dec = new TextDecoder("utf-8");
  let buf = "";
  let bytes = 0;

  // scanning state
  let started = false;  // saw '['
  let inString = false;
  let esc = false;
  let depth = 0;        // counts both {} and [] inside one object
  let objStart = -1;

  function isWS(ch) { return ch === " " || ch === "\n" || ch === "\r" || ch === "\t"; }

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    bytes += value.byteLength;
    if (onBytes) onBytes(bytes, totalBytes);
    buf += dec.decode(value, { stream: true });

    // We scan buf incrementally. When we fully consume a prefix, we slice it away.
    let i = 0;

    // Find '[' first (start of top-level array)
    if (!started) {
      while (i < buf.length && isWS(buf[i])) i++;
      if (i < buf.length && buf[i] === "[") { started = true; i++; }
      // drop leading prefix
      if (!started) {
        // buffer doesn't contain '[' yet; keep it small
        if (buf.length > 1024 * 1024) buf = buf.slice(-1024 * 1024);
        continue;
      }
    }

    for (; i < buf.length; i++) {
      const ch = buf[i];

      if (objStart < 0) {
        // waiting for start of an object or end of array
        if (isWS(ch) || ch === ",") continue;
        if (ch === "]") {
          // finished
          // consume everything
          buf = "";
          return;
        }
        if (ch === "{") {
          objStart = i;
          depth = 1;
          inString = false;
          esc = false;
          continue;
        }
        // Unexpected token; keep scanning.
        continue;
      }

      // inside object scanning
      if (inString) {
        if (esc) { esc = false; continue; }
        if (ch === "\\") { esc = true; continue; }
        if (ch === '"') { inString = false; continue; }
        continue;
      } else {
        if (ch === '"') { inString = true; continue; }
        if (ch === "{" || ch === "[") { depth++; continue; }
        if (ch === "}" || ch === "]") { depth--; }

        if (depth === 0) {
          // object ends at i
          const jsonStr = buf.slice(objStart, i + 1);
          let obj;
          try {
            obj = JSON.parse(jsonStr);
          } catch (e) {
            throw new Error("巨大JSONの分割パースに失敗しました（JSONが壊れている可能性）: " + (e?.message || e));
          }
          yield obj;

          // drop consumed prefix up to i+1, reset scanning
          buf = buf.slice(i + 1);
          i = -1;
          objStart = -1;
          depth = 0;
          inString = false;
          esc = false;
          started = true; // still in array
        }
      }
    }

    // keep buffer from growing unbounded when we haven't found end yet
    // (it will normally be cut whenever we complete an object)
    if (buf.length > 64 * 1024 * 1024 && objStart < 0) {
      buf = buf.slice(-4 * 1024 * 1024);
    }
  }

  // flush decoder tail
  buf += dec.decode();
  // if we reach here without closing ']', it's malformed but we might have parsed most objects already
}

function normalizeChatGPTConversation(c) {
  const mapping = c.mapping || {};
  let nodeId = c.current_node;
  const msgs = [];

  while (nodeId) {
    const node = mapping[nodeId];
    if (!node) break;
    const msg = node.message;
    if (msg?.author?.role && msg?.content?.parts) {
      const parts = msg.content.parts;
      const text = parts.filter(p => typeof p === "string").join("\n");
      if (text && text.trim()) {
        msgs.push({
          role: msg.author.role,
          text,
          ts: msg.create_time || null,
          model: msg?.metadata?.model_slug || msg?.metadata?.model || null
        });
      }
    }
    nodeId = node.parent;
  }
  msgs.reverse();

  if (!c.id || !msgs.length) return null;

  const provider = "openai";
  const id = `${provider}:${c.id}`;
  const models = Array.from(new Set(msgs.map(m => m.model).filter(Boolean)));
  return {
    id,
    provider,
    rawId: c.id,
    title: c.title || "(no title)",
    createdAt: c.create_time ? (c.create_time * 1000) : Date.now(),
    updatedAt: c.update_time ? (c.update_time * 1000) : Date.now(),
    models,
    messages: msgs
  };
}

function safeStr(x) { return (typeof x === "string") ? x : ""; }

function normalizeChatGPTConversations(data) {
  const arr = Array.isArray(data) ? data : (data?.conversations || []);
  const out = [];

  for (const c of arr) {
    const mapping = c.mapping || {};
    let nodeId = c.current_node;
    const msgs = [];

    while (nodeId) {
      const node = mapping[nodeId];
      if (!node) break;
      const msg = node.message;
      if (msg?.author?.role && msg?.content?.parts) {
        const parts = msg.content.parts;
        const text = parts.filter(p => typeof p === "string").join("\n");
        if (text && text.trim()) {
          msgs.push({
            role: msg.author.role,
            text,
            ts: msg.create_time || null,
            model: msg?.metadata?.model_slug || msg?.metadata?.model || null
          });
        }
      }
      nodeId = node.parent;
    }
    msgs.reverse();

    if (!c.id || !msgs.length) continue;

    const provider = "openai";
    const id = `${provider}:${c.id}`;

    const models = Array.from(new Set(msgs.map(m => m.model).filter(Boolean)));
    out.push({
      id,
      provider,
      rawId: c.id,
      title: c.title || "(no title)",
      createdAt: c.create_time ? (c.create_time * 1000) : Date.now(),
      updatedAt: c.update_time ? (c.update_time * 1000) : Date.now(),
      models,
      messages: msgs
    });
  }
  return out;
}

function normalizeGenericSessions(data) {
  // Accept:
  // { sessions: [...] } or [...]
  // session: { id?, provider?, title?, createdAt?, updatedAt?, messages:[{role,text,ts?,model?}] }
  const sessions = Array.isArray(data) ? data : (data?.sessions || data?.conversations || []);
  const out = [];

  for (const s of sessions) {
    const provider = safeStr(s.provider) || "other";
    const rawId = safeStr(s.sessionId || s.id) || uid("sess");
    const id = `${provider}:${rawId}`;
    const msgs = Array.isArray(s.messages) ? s.messages.map(m => ({
      role: safeStr(m.role) || "user",
      text: safeStr(m.text),
      ts: m.ts || m.create_time || null,
      model: safeStr(m.model) || null
    })).filter(m => m.text.trim()) : [];
    if (!msgs.length) continue;

    const createdAt = s.createdAt ? Number(s.createdAt) : Date.now();
    const updatedAt = s.updatedAt ? Number(s.updatedAt) : createdAt;
    const models = Array.from(new Set(msgs.map(m => m.model).filter(Boolean)));
    out.push({
      id, provider, rawId,
      title: safeStr(s.title) || "(no title)",
      createdAt, updatedAt, models,
      messages: msgs
    });
  }
  return out;
}

function buildAutoHistory(convs) {
  const events = [];
  let earliest = null;

  // first_contact: earliest user message + earliest assistant message (same session)
  for (const c of convs) {
    const msgs = c.messages || [];
    const firstUser = msgs.find(m => m.role === "user");
    const firstAsst = msgs.find(m => m.role === "assistant");
    const ts = (firstUser?.ts ? firstUser.ts*1000 : c.createdAt) || Date.now();

    if (!earliest || ts < earliest.ts) {
      earliest = { conv: c, firstUser, firstAsst, ts };
    }
  }
  if (earliest) {
    events.push({
      id: uid("hist"),
      ts: earliest.ts,
      title: "初めての一言",
      detail: [
        earliest.firstUser ? `あなた: ${earliest.firstUser.text.slice(0, 180)}` : "",
        earliest.firstAsst ? `相手: ${earliest.firstAsst.text.slice(0, 180)}` : ""
      ].filter(Boolean).join("\n"),
      memory: "",
      provider: earliest.conv.provider || "",
      model: (earliest.conv.models && earliest.conv.models[0]) ? earliest.conv.models[0] : "",
      links: { convId: earliest.conv.id },
      auto: true
    });
  }

  // model_started: for each provider+model, first seen timestamp
  const seen = new Map(); // key -> {ts, convId}
  for (const c of convs) {
    for (const m of (c.messages || [])) {
      if (!m.model) continue;
      const key = `${c.provider}:${m.model}`;
      const ts = m.ts ? m.ts*1000 : c.createdAt;
      const prev = seen.get(key);
      if (!prev || ts < prev.ts) seen.set(key, { ts, convId: c.id });
    }
  }
  for (const [key, v] of seen.entries()) {
    const [provider, model] = key.split(":");
    events.push({
      id: uid("hist"),
      ts: v.ts,
      title: `モデル開始: ${model}`,
      detail: `このモデルが最初に登場したタイミング。`,
      memory: "",
      provider,
      model,
      links: { convId: v.convId },
      auto: true
    });
  }

  return events.sort((a,b)=>a.ts-b.ts);
}

async function handleFile(file) {
  cancelled = false;
  const name = (file.name || "").toLowerCase();

  postStatus("読み込み中…");
  const db = await openDB();

  // We'll stream-parse gigantic ChatGPT export arrays to avoid JSON.parse(text) OOM.
  // While importing, compute auto-history stats incrementally (no need to hold all sessions in memory).
  let sessionsCount = 0;
  let earliest = null; // { conv, firstUser, firstAsst, ts }
  const seen = new Map(); // key -> {ts, convId}

  function updateAutoHistoryStats(conv) {
    const msgs = conv.messages || [];
    const firstUser = msgs.find(m => m.role === "user");
    const firstAsst = msgs.find(m => m.role === "assistant");
    const ts = (firstUser?.ts ? firstUser.ts * 1000 : conv.createdAt) || Date.now();
    if (!earliest || ts < earliest.ts) earliest = { conv, firstUser, firstAsst, ts };

    for (const m of msgs) {
      if (!m.model) continue;
      const key = `${conv.provider}:${m.model}`;
      const t = m.ts ? m.ts * 1000 : conv.createdAt;
      const prev = seen.get(key);
      if (!prev || t < prev.ts) seen.set(key, { ts: t, convId: conv.id });
    }
  }

  function buildAutoHistoryFromStats() {
    const events = [];
    if (earliest) {
      events.push({
        id: uid("hist"),
        ts: earliest.ts,
        title: "初めての一言",
        detail: [
          earliest.firstUser ? `あなた: ${earliest.firstUser.text.slice(0, 180)}` : "",
          earliest.firstAsst ? `相手: ${earliest.firstAsst.text.slice(0, 180)}` : ""
        ].filter(Boolean).join("\n"),
        memory: "",
        provider: earliest.conv.provider || "",
        model: (earliest.conv.models && earliest.conv.models[0]) ? earliest.conv.models[0] : "",
        links: { convId: earliest.conv.id },
        auto: true
      });
    }
    for (const [key, v] of seen.entries()) {
      const [provider, model] = key.split(":");
      events.push({
        id: uid("hist"),
        ts: v.ts,
        title: `モデル開始: ${model}`,
        detail: "このモデルが最初に登場したタイミング。",
        memory: "",
        provider,
        model,
        links: { convId: v.convId },
        auto: true
      });
    }
    return events.sort((a, b) => a.ts - b.ts);
  }

  // Batch write
  const B = 8;
  let batch = [];
  async function flushBatch() {
    if (!batch.length) return;
    await putBatch(db, STORES.conv, batch);
    batch = [];
    // Let UI breathe
    await new Promise(r => setTimeout(r, 0));
  }

  // Decide input stream + total bytes for progress bar
  let byteStream = null;
  let totalBytes = file.size || 0;
  let isChatGPT = true;

  if (name.endsWith(".zip")) {
    postStatus("ZIP解析中…");
    const found = await unzipFindFile(file, ["conversations.json", "conversations.json.txt"]);
    if (cancelled) return;
    byteStream = found.stream;
    totalBytes = found.uncompSize || totalBytes;
    isChatGPT = true;
  } else {
    // Peek small header to choose parser without reading the whole file.
    const head = await file.slice(0, Math.min(64 * 1024, file.size)).text();
    const first = head.match(/\S/)?.[0] || "";
    if (first === "[") {
      isChatGPT = /\"mapping\"\s*:|\"current_node\"\s*:/.test(head);
      byteStream = file.stream();
    } else {
      // Not an array -> likely small; fall back to normal JSON.parse
      isChatGPT = false;
      byteStream = null;
    }
  }

  if (cancelled) return;

  if (byteStream && isChatGPT) {
    postStatus("JSON分割パース中…");
    const onBytes = (done, total) => {
      // show parse progress as bytes read
      postProg(done, total || totalBytes, "parse");
    };

    for await (const rawConv of streamParseTopLevelArrayOfObjects(byteStream, totalBytes, onBytes)) {
      if (cancelled) return;
      const conv = normalizeChatGPTConversation(rawConv);
      if (!conv) continue;

      updateAutoHistoryStats(conv);
      batch.push(conv);
      sessionsCount++;

      if (batch.length >= B) {
        postStatus(`保存中…（${sessionsCount}件）`);
        await flushBatch();
      }

      // yield occasionally
      if ((sessionsCount % 25) === 0) await new Promise(r => setTimeout(r, 0));
    }
    await flushBatch();
  } else {
    // Fallback: small JSON
    postStatus("JSON読み込み中…");
    const text = await file.text();
    const data = JSON.parse(text);
    const sessions = normalizeGenericSessions(data);
    postStatus(`保存中…（${sessions.length}件）`);
    for (const s of sessions) {
      if (cancelled) return;
      batch.push(s);
      sessionsCount++;
      if (batch.length >= B) await flushBatch();
    }
    await flushBatch();
  }

  if (cancelled) return;

  // Auto history (small)
  const hist = buildAutoHistoryFromStats();
  if (hist.length) await putBatch(db, STORES.history, hist);

  postMessage({ type: "done", sessions: sessionsCount, history: hist.length });
}

onmessage = async (e) => {
  const { type, file } = e.data || {};
  if (type === "cancel") { cancelled = true; postStatus("キャンセルしました"); return; }
  if (type === "import" && file) {
    try {
      await handleFile(file);
    } catch (err) {
      postMessage({ type: "error", msg: String(err?.message || err) });
    }
  }
};
