const CACHE = "album-static-v3";
const ASSETS = [
  "./index.html",
  "./app.js",
  "./db.js",
  "./workers/import.worker.js",
  "./workers/search.worker.js",
  "./workers/export.worker.js",
  "./pwa/manifest.webmanifest",
  "./pwa/icons/icon-192.png",
  "./pwa/icons/icon-512.png"
];

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)));
});

self.addEventListener("activate", (e) => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)));
  })());
});

self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (url.origin !== location.origin) return;
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request).catch(() => r))
  );
});
