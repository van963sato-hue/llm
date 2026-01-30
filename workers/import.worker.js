/* Import Worker v2: チェックポイント機能・重複スキップ対応
   - File input から conversations.json をストリーミングパース
   - 途中で止まっても続きから再開可能（checkpoint）
   - 既存の会話はid+update_timeで重複判定してスキップ
   - バッチ書き込み（25件）で高速化
*/
const DB_NAME = "llm_memory_album_v2";
const DB_VER = 4;

const STORES = {
  conv: "conversations",
  moment: "moments",
  prompt: "prompt_profiles",
  asset: "assets",
  label: "model_labels",
  history: "history_events",
  meta: "meta",
  iconcfg: "icon_config",
  importState: "import_state",
  convIndex: "conv_index"
};

let cancelled = false;

// === メッセージ送信ヘルパー ===
function postStatus(msg) { postMessage({ type: "status", msg }); }
function postProg(data) {
  // data: { bytes, totalBytes, processed, saved, skipped, phase }
  postMessage({ type: "progress", ...data });
}
function postCheckpointFound(checkpoint, fileSignature) {
  // 再開可能なチェックポイントが見つかった
  postMessage({ type: "checkpoint_found", checkpoint, fileSignature });
}

// === IndexedDB操作 ===
function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = (event) => {
      const db = req.result;
      const tx = req.transaction;
      const oldVersion = event.oldVersion;

      for (const st of Object.values(STORES)) {
        if (!db.objectStoreNames.contains(st)) db.createObjectStore(st, { keyPath: "id" });
      }
      try {
        const conv = tx.objectStore(STORES.conv);
        if (conv && !conv.indexNames.contains("byUpdatedAt")) conv.createIndex("byUpdatedAt", "updatedAt", { unique: false });
      } catch (e) {}

      // v4マイグレーション: 既存convからconv_indexを構築
      if (oldVersion < 4 && oldVersion > 0) {
        try {
          const convStore = tx.objectStore(STORES.conv);
          const idxStore = tx.objectStore(STORES.convIndex);
          const cursor = convStore.openCursor();
          cursor.onsuccess = function() {
            const c = cursor.result;
            if (c) {
              const conv = c.value;
              idxStore.put({ id: conv.id, updateTime: conv.updatedAt || 0 });
              c.continue();
            }
          };
        } catch (e) {
          console.warn("conv_index migration:", e);
        }
      }
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

function putOne(db, store, item) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readwrite");
    const st = tx.objectStore(store);
    st.put(item);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

function getOne(db, store, id) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const st = tx.objectStore(store);
    const req = st.get(id);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
}

function deleteOne(db, store, id) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readwrite");
    const st = tx.objectStore(store);
    st.delete(id);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// 複数IDを一度に取得（重複チェック用）
function getMany(db, store, ids) {
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const st = tx.objectStore(store);
    const results = new Map();
    let pending = ids.length;
    if (pending === 0) { resolve(results); return; }
    for (const id of ids) {
      const req = st.get(id);
      req.onsuccess = () => {
        if (req.result) results.set(id, req.result);
        if (--pending === 0) resolve(results);
      };
      req.onerror = () => {
        if (--pending === 0) resolve(results);
      };
    }
  });
}

function uid(prefix="id") {
  return `${prefix}_${crypto.getRandomValues(new Uint32Array(2)).join("")}_${Date.now()}`;
}

// === ファイル署名生成 ===
async function generateFileSignature(file) {
  // 基本署名: name:size:lastModified
  const base = `${file.name}:${file.size}:${file.lastModified}`;

  // 追加: 先頭8KBと末尾8KBのハッシュで強化
  try {
    const headSize = Math.min(8192, file.size);
    const tailSize = Math.min(8192, file.size);
    const headSlice = await file.slice(0, headSize).arrayBuffer();
    const tailSlice = await file.slice(Math.max(0, file.size - tailSize), file.size).arrayBuffer();

    // 簡易ハッシュ（SubtleCryptoが使えない環境用のフォールバック付き）
    let hash = "";
    if (crypto.subtle) {
      const combined = new Uint8Array(headSlice.byteLength + tailSlice.byteLength);
      combined.set(new Uint8Array(headSlice), 0);
      combined.set(new Uint8Array(tailSlice), headSlice.byteLength);
      const hashBuf = await crypto.subtle.digest("SHA-256", combined);
      hash = Array.from(new Uint8Array(hashBuf)).slice(0, 8).map(b => b.toString(16).padStart(2, "0")).join("");
    } else {
      // フォールバック: 先頭・末尾バイトの簡易チェックサム
      const h = new Uint8Array(headSlice);
      const t = new Uint8Array(tailSlice);
      let sum = 0;
      for (let i = 0; i < h.length; i++) sum = (sum + h[i]) & 0xffffffff;
      for (let i = 0; i < t.length; i++) sum = (sum + t[i]) & 0xffffffff;
      hash = sum.toString(16);
    }
    return `${base}:${hash}`;
  } catch (e) {
    return base;
  }
}

// === ZIP処理（既存と同様） ===
function u8view(ab) { return new Uint8Array(ab); }

function findEOCD(u8) {
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
  const tailSize = Math.min(70000, file.size);
  const tailAb = await readBlobSlice(file, file.size - tailSize, file.size);
  const tail = u8view(tailAb);
  const eocdRel = findEOCD(tail);
  if (eocdRel < 0) throw new Error("ZIPのEOCDが見つからない");

  const cdSize = readU32(tail, eocdRel + 12);
  const cdOffset = readU32(tail, eocdRel + 16);

  const cdAb = await readBlobSlice(file, cdOffset, cdOffset + cdSize);
  const cd = u8view(cdAb);

  const dec = new TextDecoder("utf-8");
  let p = 0;
  while (p + 46 <= cd.length) {
    if (!(cd[p] === 0x50 && cd[p+1] === 0x4b && cd[p+2] === 0x01 && cd[p+3] === 0x02)) break;

    const compMethod = readU16(cd, p + 10);
    const compSize = readU32(cd, p + 20);
    const uncompSize = readU32(cd, p + 24);
    const nameLen = readU16(cd, p + 28);
    const extraLen = readU16(cd, p + 30);
    const commentLen = readU16(cd, p + 32);
    const localOff = readU32(cd, p + 42);

    const name = dec.decode(cd.slice(p + 46, p + 46 + nameLen));
    const next = p + 46 + nameLen + extraLen + commentLen;

    if (targetNames.includes(name)) {
      const lhAb = await readBlobSlice(file, localOff, localOff + 30);
      const lh = u8view(lhAb);
      if (!(lh[0] === 0x50 && lh[1] === 0x4b && lh[2] === 0x03 && lh[3] === 0x04)) throw new Error("ZIP local headerが不正");
      const lNameLen = readU16(lh, 26);
      const lExtraLen = readU16(lh, 28);
      const dataStart = localOff + 30 + lNameLen + lExtraLen;

      const compBlob = file.slice(dataStart, dataStart + compSize);

      if (compMethod === 0) {
        return { name, stream: compBlob.stream(), uncompSize, dataStart };
      }
      if (compMethod === 8) {
        if (typeof DecompressionStream === "undefined") throw new Error("このブラウザはdeflate-raw解凍に非対応です");
        const ds = new DecompressionStream("deflate-raw");
        return { name, stream: compBlob.stream().pipeThrough(ds), uncompSize, dataStart };
      }
      throw new Error("未対応の圧縮方式: " + compMethod);
    }
    p = next;
  }
  throw new Error("ZIP内に対象ファイルが見つからない（conversations.json など）");
}

// === ストリーミングJSONパーサ（offset対応版） ===
// yieldするたびに { obj, endOffset } を返す
// endOffset = この会話オブジェクトの終端バイト位置（チェックポイント用）
async function* streamParseTopLevelArrayOfObjects(byteStream, totalBytes, startOffset, onProgress) {
  const reader = byteStream.getReader();
  const dec = new TextDecoder("utf-8");
  let buf = "";
  let bytes = startOffset; // 累積バイト数
  let lastObjEndOffset = startOffset; // 最後に完了したオブジェクトの終端

  // scanning state
  let started = false;
  let inString = false;
  let esc = false;
  let depth = 0;
  let objStart = -1;
  let objStartBytes = 0; // オブジェクト開始時のバイト位置

  function isWS(ch) { return ch === " " || ch === "\n" || ch === "\r" || ch === "\t"; }

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    const chunkSize = value.byteLength;
    bytes += chunkSize;
    if (onProgress) onProgress(bytes, totalBytes);
    buf += dec.decode(value, { stream: true });

    let i = 0;

    if (!started) {
      while (i < buf.length && isWS(buf[i])) i++;
      if (i < buf.length && buf[i] === "[") { started = true; i++; }
      if (!started) {
        if (buf.length > 1024 * 1024) buf = buf.slice(-1024 * 1024);
        continue;
      }
    }

    for (; i < buf.length; i++) {
      const ch = buf[i];

      if (objStart < 0) {
        if (isWS(ch) || ch === ",") continue;
        if (ch === "]") {
          buf = "";
          return;
        }
        if (ch === "{") {
          objStart = i;
          objStartBytes = bytes - (buf.length - i); // このオブジェクトの開始バイト位置
          depth = 1;
          inString = false;
          esc = false;
          continue;
        }
        continue;
      }

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
          const jsonStr = buf.slice(objStart, i + 1);
          let obj;
          try {
            obj = JSON.parse(jsonStr);
          } catch (e) {
            throw new Error("JSONパースエラー: " + (e?.message || e));
          }

          // このオブジェクトの終端バイト位置
          const endOffset = bytes - (buf.length - i - 1);
          lastObjEndOffset = endOffset;

          yield { obj, endOffset };

          buf = buf.slice(i + 1);
          i = -1;
          objStart = -1;
          depth = 0;
          inString = false;
          esc = false;
          started = true;
        }
      }
    }

    if (buf.length > 64 * 1024 * 1024 && objStart < 0) {
      buf = buf.slice(-4 * 1024 * 1024);
    }
  }

  buf += dec.decode();
}

// === ChatGPT会話の正規化 ===
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

function normalizeGenericSessions(data) {
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

// === 自動ヒストリー生成 ===
function buildAutoHistoryFromStats(earliest, seen) {
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

// === メイン処理 ===
const BATCH_SIZE = 25;           // バッチ書き込みサイズ
const CHECKPOINT_INTERVAL = 25;  // N件ごとにチェックポイント
const CHECKPOINT_BYTES = 2 * 1024 * 1024; // または Mバイトごと

async function handleFile(file, options = {}) {
  cancelled = false;
  const { resumeFromCheckpoint = false, skipCheckpointPrompt = false } = options;
  const name = (file.name || "").toLowerCase();

  postStatus("ファイル署名を生成中…");
  const db = await openDB();
  const fileSignature = await generateFileSignature(file);

  // チェックポイント確認
  let checkpoint = await getOne(db, STORES.importState, fileSignature);

  if (checkpoint && !resumeFromCheckpoint && !skipCheckpointPrompt) {
    // チェックポイントが見つかった場合、メインスレッドに確認を求める
    postCheckpointFound(checkpoint, fileSignature);
    return; // メインスレッドからの指示を待つ
  }

  // 統計
  let stats = {
    processed: checkpoint?.processedCount || 0,
    saved: checkpoint?.savedCount || 0,
    skipped: checkpoint?.skippedCount || 0,
    bytes: checkpoint?.offsetBytes || 0
  };
  let startOffset = checkpoint?.offsetBytes || 0;
  let lastCheckpointBytes = startOffset;

  // 自動ヒストリー用
  let earliest = null;
  const seen = new Map();

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

  // バッチ処理
  let batch = [];
  let batchIndex = [];  // conv_index用

  async function flushBatch() {
    if (!batch.length) return;
    await putBatch(db, STORES.conv, batch);
    await putBatch(db, STORES.convIndex, batchIndex);
    batch = [];
    batchIndex = [];
    await new Promise(r => setTimeout(r, 0));
  }

  // チェックポイント保存
  async function saveCheckpoint(offsetBytes) {
    const cp = {
      id: fileSignature,
      offsetBytes,
      processedCount: stats.processed,
      savedCount: stats.saved,
      skippedCount: stats.skipped,
      updatedAt: Date.now()
    };
    await putOne(db, STORES.importState, cp);
    lastCheckpointBytes = offsetBytes;
  }

  // 重複チェック（バッチ単位で効率化）
  async function checkDuplicates(convs) {
    const ids = convs.map(c => c.id);
    const existing = await getMany(db, STORES.convIndex, ids);
    return convs.map(conv => {
      const ex = existing.get(conv.id);
      if (!ex) return { conv, isDuplicate: false };
      // 既存のupdateTimeと比較: 同じか既存が新しければスキップ
      if (ex.updateTime >= conv.updatedAt) {
        return { conv, isDuplicate: true };
      }
      return { conv, isDuplicate: false }; // 新しいデータなので更新
    });
  }

  // ストリーム準備
  let byteStream = null;
  let totalBytes = file.size || 0;
  let isChatGPT = true;
  let isZip = name.endsWith(".zip");

  if (isZip) {
    // ZIP の場合、offset再開は非対応（圧縮済みのため）
    if (startOffset > 0) {
      postStatus("ZIP形式は途中再開に非対応です。最初から開始します…");
      startOffset = 0;
      stats = { processed: 0, saved: 0, skipped: 0, bytes: 0 };
    }
    postStatus("ZIP解析中…");
    const found = await unzipFindFile(file, ["conversations.json", "conversations.json.txt"]);
    if (cancelled) return;
    byteStream = found.stream;
    totalBytes = found.uncompSize || totalBytes;
    isChatGPT = true;
  } else {
    // JSON直接
    const head = await file.slice(0, Math.min(64 * 1024, file.size)).text();
    const first = head.match(/\S/)?.[0] || "";
    if (first === "[") {
      isChatGPT = /\"mapping\"\s*:|\"current_node\"\s*:/.test(head);
      // offset再開: ファイルの途中からスライス
      const slicedFile = startOffset > 0 ? file.slice(startOffset) : file;
      byteStream = slicedFile.stream();
      totalBytes = file.size;
    } else {
      isChatGPT = false;
      byteStream = null;
    }
  }

  if (cancelled) return;

  if (byteStream && isChatGPT) {
    postStatus(startOffset > 0 ? `${stats.processed}件目から再開中…` : "JSON分割パース中…");

    const onProgress = (bytes, total) => {
      stats.bytes = bytes;
      postProg({
        bytes,
        totalBytes: total || totalBytes,
        processed: stats.processed,
        saved: stats.saved,
        skipped: stats.skipped,
        phase: "parse"
      });
    };

    let pendingConvs = [];

    for await (const { obj: rawConv, endOffset } of streamParseTopLevelArrayOfObjects(byteStream, totalBytes, startOffset, onProgress)) {
      if (cancelled) {
        // キャンセル時にチェックポイント保存
        await flushBatch();
        await saveCheckpoint(endOffset);
        postStatus(`キャンセル: ${stats.processed}件処理済み（次回続きから再開可能）`);
        postMessage({ type: "cancelled", ...stats });
        return;
      }

      const conv = normalizeChatGPTConversation(rawConv);
      if (!conv) continue;

      pendingConvs.push({ conv, endOffset });
      stats.processed++;

      // バッチ単位で重複チェック＆保存
      if (pendingConvs.length >= BATCH_SIZE) {
        const checked = await checkDuplicates(pendingConvs.map(p => p.conv));
        for (let i = 0; i < checked.length; i++) {
          const { conv: c, isDuplicate } = checked[i];
          if (isDuplicate) {
            stats.skipped++;
          } else {
            updateAutoHistoryStats(c);
            batch.push(c);
            batchIndex.push({ id: c.id, updateTime: c.updatedAt });
            stats.saved++;
          }
        }
        const lastEndOffset = pendingConvs[pendingConvs.length - 1].endOffset;
        pendingConvs = [];

        if (batch.length >= BATCH_SIZE) {
          postStatus(`保存中…（${stats.saved}件保存 / ${stats.skipped}件スキップ）`);
          await flushBatch();
        }

        // チェックポイント保存（N件ごと or Mバイトごと）
        if (stats.processed % CHECKPOINT_INTERVAL === 0 ||
            (lastEndOffset - lastCheckpointBytes) >= CHECKPOINT_BYTES) {
          await saveCheckpoint(lastEndOffset);
        }
      }

      // UIスレッドに余裕を
      if (stats.processed % 50 === 0) await new Promise(r => setTimeout(r, 0));
    }

    // 残りの処理
    if (pendingConvs.length > 0) {
      const checked = await checkDuplicates(pendingConvs.map(p => p.conv));
      for (let i = 0; i < checked.length; i++) {
        const { conv: c, isDuplicate } = checked[i];
        if (isDuplicate) {
          stats.skipped++;
        } else {
          updateAutoHistoryStats(c);
          batch.push(c);
          batchIndex.push({ id: c.id, updateTime: c.updatedAt });
          stats.saved++;
        }
      }
    }
    await flushBatch();

  } else {
    // 小さいJSON（非ChatGPT形式など）
    postStatus("JSON読み込み中…");
    const text = await file.text();
    const data = JSON.parse(text);
    const sessions = normalizeGenericSessions(data);

    // 重複チェック
    const checked = await checkDuplicates(sessions);

    for (const { conv, isDuplicate } of checked) {
      if (cancelled) break;
      if (isDuplicate) {
        stats.skipped++;
      } else {
        updateAutoHistoryStats(conv);
        batch.push(conv);
        batchIndex.push({ id: conv.id, updateTime: conv.updatedAt });
        stats.saved++;
      }
      stats.processed++;

      if (batch.length >= BATCH_SIZE) {
        postStatus(`保存中…（${stats.saved}件保存 / ${stats.skipped}件スキップ）`);
        await flushBatch();
      }
    }
    await flushBatch();
  }

  if (cancelled) return;

  // 完了: チェックポイント削除
  await deleteOne(db, STORES.importState, fileSignature);

  // 自動ヒストリー
  const hist = buildAutoHistoryFromStats(earliest, seen);
  if (hist.length) await putBatch(db, STORES.history, hist);

  postMessage({
    type: "done",
    sessions: stats.saved,
    skipped: stats.skipped,
    processed: stats.processed,
    history: hist.length
  });
}

// === メッセージハンドラ ===
onmessage = async (e) => {
  const { type, file, resumeFromCheckpoint, skipCheckpointPrompt } = e.data || {};

  if (type === "cancel") {
    cancelled = true;
    postStatus("キャンセル中…（チェックポイント保存します）");
    return;
  }

  if (type === "import" && file) {
    try {
      await handleFile(file, { resumeFromCheckpoint, skipCheckpointPrompt });
    } catch (err) {
      postMessage({ type: "error", msg: String(err?.message || err) });
    }
  }

  // チェックポイントからの再開指示
  if (type === "resume" && file) {
    try {
      await handleFile(file, { resumeFromCheckpoint: true });
    } catch (err) {
      postMessage({ type: "error", msg: String(err?.message || err) });
    }
  }

  // 最初からやり直し指示
  if (type === "restart" && file) {
    try {
      const db = await openDB();
      const sig = await generateFileSignature(file);
      await deleteOne(db, STORES.importState, sig);
      await handleFile(file, { skipCheckpointPrompt: true });
    } catch (err) {
      postMessage({ type: "error", msg: String(err?.message || err) });
    }
  }
};
