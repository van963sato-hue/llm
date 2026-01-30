const CACHE = "album-static-v3";
const ASSETS = [
  "./",
  "./index.html",
  "./app.js",
  "./db.js",
  "./workers/import.worker.js",
  "./workers/search.worker.js",
  "./workers/export.worker.js",
  "./pwa/manifest.webmanifest",
  "./pwa/icons/icon-192.png",
  "./pwa/icons/icon-512.png",
  "./sw.js"
];

self.addEventListener("install", (e) => {
  e.waitUntil(
    caches.open(CACHE).then((c) => c.addAll(ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)));
      await self.clients.claim();
    })()
  );
});

self.addEventListener("fetch", (e) => {
  const req = e.request;
  const url = new URL(req.url);

  if (url.origin !== location.origin) return;

  // Cache-first for same-origin GET; network fallback.
  if (req.method !== "GET") return;

  e.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((res) => {
        if (res && res.ok) {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(req, copy));
        }
        return res;
      });
    })
  );
});
