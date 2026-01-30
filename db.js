const DB_NAME = "llm_memory_album_v2";
const DB_VER = 4;

export const STORES = {
  conv: "conversations",
  moment: "moments",
  prompt: "prompt_profiles",
  asset: "assets",
  label: "model_labels",
  history: "history_events",
  meta: "meta",
  iconcfg: "icon_config",
  // v4: チェックポイント機能
  importState: "import_state",  // key=fileSignature, value={offsetBytes, processedCount, savedCount, skippedCount, updatedAt}
  convIndex: "conv_index"       // key=id, value={updateTime} - 重複検出用軽量インデックス
};

export const DB = { DB_NAME, DB_VER };

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = (event) => {
      const db = req.result;
      const tx = req.transaction;
      const oldVersion = event.oldVersion;

      // 基本ストア作成（既存）
      for (const s of Object.values(STORES)) {
        if (!db.objectStoreNames.contains(s)) {
          db.createObjectStore(s, { keyPath: "id" });
        }
      }

      // Lightweight indexes for UI sorting/filtering (optional)
      try {
        const conv = tx.objectStore(STORES.conv);
        if (!conv.indexNames.contains("byUpdatedAt")) conv.createIndex("byUpdatedAt", "updatedAt", { unique: false });
      } catch {}
      try {
        const moment = tx.objectStore(STORES.moment);
        if (!moment.indexNames.contains("byCreatedAt")) moment.createIndex("byCreatedAt", "createdAt", { unique: false });
      } catch {}
      try {
        const hist = tx.objectStore(STORES.history);
        if (!hist.indexNames.contains("byTs")) hist.createIndex("byTs", "ts", { unique: false });
      } catch {}

      // v4: 既存データからconv_indexを構築（マイグレーション）
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

export async function putMany(store, items) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readwrite");
    const st = tx.objectStore(store);
    for (const it of items) st.put(it);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

export async function putOne(store, item) {
  return putMany(store, [item]);
}

export async function getAll(store) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const st = tx.objectStore(store);
    const req = st.getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

export async function getOne(store, id) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, "readonly");
    const st = tx.objectStore(store);
    const req = st.get(id);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
}

export async function clearAll() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(Object.values(STORES), "readwrite");
    for (const s of Object.values(STORES)) tx.objectStore(s).clear();
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

export function uid(prefix = "id") {
  return `${prefix}_${crypto.getRandomValues(new Uint32Array(2)).join("")}_${Date.now()}`;
}
