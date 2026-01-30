/* Export Worker:
   - Always produces a lightweight JSON export first (fast & reliable).
   - Can also create ZIP (store method, no compression) without external libs.
   - For very large assets, an "assets-only zip" option exists.
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
function postProg(done, total, phase="export") { postMessage({ type: "progress", phase, done, total }); }

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

function u16le(n) { return new Uint8Array([n & 255, (n >> 8) & 255]); }
function u32le(n) { return new Uint8Array([n & 255, (n >> 8) & 255, (n >> 16) & 255, (n >> 24) & 255]); }

function textU8(s) { return new TextEncoder().encode(String(s)); }

// CRC32 (incremental)
const CRC_TABLE = (() => {
  const tbl = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    tbl[i] = c >>> 0;
  }
  return tbl;
})();

function crc32Update(crc, u8) {
  let c = crc ^ 0xFFFFFFFF;
  for (let i = 0; i < u8.length; i++) c = CRC_TABLE[(c ^ u8[i]) & 0xFF] ^ (c >>> 8);
  return (c ^ 0xFFFFFFFF) >>> 0;
}

function dosTimeDate(ms) {
  const d = new Date(ms);
  const year = Math.max(1980, d.getFullYear());
  const month = d.getMonth() + 1;
  const day = d.getDate();
  const hour = d.getHours();
  const min = d.getMinutes();
  const sec = Math.floor(d.getSeconds() / 2);

  const time = (hour << 11) | (min << 5) | sec;
  const date = ((year - 1980) << 9) | (month << 5) | day;
  return { time, date };
}

async function crc32OfBlob(blob) {
  const reader = blob.stream().getReader();
  let crc = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    crc = crc32Update(crc, value);
    if (cancelled) return { crc: 0, size: 0 };
  }
  return { crc, size: blob.size };
}

// ZIP writer with "store" method + data descriptor (stream friendly)
class ZipWriter {
  constructor() {
    this.parts = [];
    this.central = [];
    this.offset = 0;
    this.fileCount = 0;
  }

  async addFile(path, blobOrU8, mtimeMs = Date.now()) {
    const nameU8 = textU8(path);
    const { time, date } = dosTimeDate(mtimeMs);

    const isBlob = (blobOrU8 instanceof Blob);
    const size = isBlob ? blobOrU8.size : blobOrU8.length;

    // Local header (with data descriptor flag set, sizes/crc=0 here)
    // signature 0x04034b50
    const hdr = [];
    hdr.push(u32le(0x04034b50));
    hdr.push(u16le(20));            // version needed
    hdr.push(u16le(0x0008));        // gp flag: data descriptor
    hdr.push(u16le(0));             // compression: store
    hdr.push(u16le(time));
    hdr.push(u16le(date));
    hdr.push(u32le(0));             // crc placeholder
    hdr.push(u32le(0));             // comp size placeholder
    hdr.push(u32le(0));             // uncomp size placeholder
    hdr.push(u16le(nameU8.length));
    hdr.push(u16le(0));             // extra len
    hdr.push(nameU8);

    const localHeader = new Blob(hdr);
    this.parts.push(localHeader);
    const localHeaderOffset = this.offset;
    this.offset += localHeader.size;

    // File data
    let crc = 0;
    if (isBlob) {
      // stream to compute crc while appending blob
      // We'll compute crc by streaming separately (can't stream-read and append simultaneously without cloning),
      // so we do a crc pass first, then append blob as-is.
      const meta = await crc32OfBlob(blobOrU8);
      crc = meta.crc;
      if (cancelled) return;
      this.parts.push(blobOrU8);
      this.offset += blobOrU8.size;
    } else {
      crc = crc32Update(0, blobOrU8);
      this.parts.push(new Blob([blobOrU8]));
      this.offset += blobOrU8.length;
    }

    // Data descriptor (signature 0x08074b50)
    const dd = new Blob([
      u32le(0x08074b50),
      u32le(crc),
      u32le(size),
      u32le(size)
    ]);
    this.parts.push(dd);
    this.offset += dd.size;

    // Central directory header
    const cdh = [];
    cdh.push(u32le(0x02014b50));    // signature
    cdh.push(u16le(20));            // version made by
    cdh.push(u16le(20));            // version needed
    cdh.push(u16le(0x0008));        // gp flag
    cdh.push(u16le(0));             // compression
    cdh.push(u16le(time));
    cdh.push(u16le(date));
    cdh.push(u32le(crc));
    cdh.push(u32le(size));
    cdh.push(u32le(size));
    cdh.push(u16le(nameU8.length));
    cdh.push(u16le(0));             // extra
    cdh.push(u16le(0));             // comment
    cdh.push(u16le(0));             // disk start
    cdh.push(u16le(0));             // internal attrs
    cdh.push(u32le(0));             // external attrs
    cdh.push(u32le(localHeaderOffset));
    cdh.push(nameU8);

    const cdBlob = new Blob(cdh);
    this.central.push(cdBlob);
    this.fileCount++;
  }

  finalize() {
    const cdOffset = this.offset;
    let cdSize = 0;
    for (const b of this.central) { this.parts.push(b); cdSize += b.size; this.offset += b.size; }

    // EOCD
    const eocd = new Blob([
      u32le(0x06054b50),
      u16le(0), u16le(0),
      u16le(this.fileCount), u16le(this.fileCount),
      u32le(cdSize),
      u32le(cdOffset),
      u16le(0)
    ]);
    this.parts.push(eocd);
    this.offset += eocd.size;

    return new Blob(this.parts, { type: "application/zip" });
  }
}

async function exportLite() {
  cancelled = false;
  postStatus("軽量エクスポート準備中…");
  const db = await openDB();

  const moments = await getAll(db, STORES.moment);
  const prompts = await getAll(db, STORES.prompt);
  const labels = await getAll(db, STORES.label);
  const history = await getAll(db, STORES.history);

  const album = {
    version: 2,
    exportedAt: new Date().toISOString(),
    includeConversations: false,
    history,
    moments,
    prompts,
    labels
  };

  // Chunk JSON to avoid one huge string allocation
  const jsonText = JSON.stringify(album);
  const CHUNK = 1_000_000;
  const parts = [];
  for (let i = 0; i < jsonText.length; i += CHUNK) {
    if (cancelled) return;
    parts.push(jsonText.slice(i, i + CHUNK));
    if (i % (CHUNK * 3) === 0) postProg(i, jsonText.length, "json");
    await new Promise(r => setTimeout(r, 0));
  }

  const blob = new Blob(parts, { type: "application/json" });
  postMessage({ type: "done_json", blob, filename: "album-data.json" });
  postStatus("軽量エクスポート完了");
}

async function exportAssetsOnly() {
  cancelled = false;
  postStatus("画像ZIP作成中…");
  const db = await openDB();
  const assets = await getAll(db, STORES.asset);

  const zw = new ZipWriter();
  for (let i = 0; i < assets.length; i++) {
    if (cancelled) return;
    const a = assets[i];
    const ext = (a.type || a.blob?.type || "").split("/")[1] || "bin";
    await zw.addFile(`assets/${a.id}.${ext}`, a.blob, a.updatedAt || Date.now());
    if (i % 5 === 0) postProg(i, assets.length, "assets");
    await new Promise(r => setTimeout(r, 0));
  }
  const zip = zw.finalize();
  postMessage({ type: "done_zip", blob: zip, filename: "assets.zip" });
  postStatus("画像ZIP完了");
}

async function exportFullZip() {
  cancelled = false;
  postStatus("完全エクスポート準備中…");
  const db = await openDB();

  const convs = await getAll(db, STORES.conv);
  const moments = await getAll(db, STORES.moment);
  const prompts = await getAll(db, STORES.prompt);
  const labels = await getAll(db, STORES.label);
  const history = await getAll(db, STORES.history);
  const assets = await getAll(db, STORES.asset);

  const album = {
    version: 2,
    exportedAt: new Date().toISOString(),
    includeConversations: true,
    history,
    moments,
    prompts,
    labels,
    conversations: convs,
    assets: assets.map(a => ({ id: a.id, type: a.type || a.blob?.type || "" }))
  };

  // Zip
  const zw = new ZipWriter();
  postStatus("ZIPに album.json を追加中…");
  const jsonU8 = textU8(JSON.stringify(album));
  await zw.addFile("album/album.json", jsonU8, Date.now());

  postStatus("ZIPに画像を追加中…");
  for (let i = 0; i < assets.length; i++) {
    if (cancelled) return;
    const a = assets[i];
    const ext = (a.type || a.blob?.type || "").split("/")[1] || "bin";
    await zw.addFile(`album/assets/${a.id}.${ext}`, a.blob, a.updatedAt || Date.now());
    if (i % 5 === 0) postProg(i, assets.length, "assets");
    await new Promise(r => setTimeout(r, 0));
  }

  const zip = zw.finalize();
  postMessage({ type: "done_zip", blob: zip, filename: "album-full.zip" });
  postStatus("完全エクスポート完了");
}

onmessage = async (e) => {
  const { type } = e.data || {};
  if (type === "cancel") { cancelled = true; postStatus("キャンセルしました"); return; }
  try {
    if (type === "exportLite") await exportLite();
    if (type === "exportAssets") await exportAssetsOnly();
    if (type === "exportFull") await exportFullZip();
  } catch (err) {
    postMessage({ type: "error", msg: String(err?.message || err) });
  }
};
