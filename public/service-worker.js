// VarshaNet service worker
//
// Job here is narrow on purpose: make the app SHELL (this page's HTML/CSS/JS
// + manifest/icons) available with zero connectivity, so a citizen in a
// rural/low-signal area can still open VarshaNet and fill out the "Report an
// Event" form. It deliberately does NOT try to queue or replay report
// submissions itself — that logic lives in public/report-offline-queue.js
// (plain page JS + IndexedDB), because every submission needs a CAPTCHA
// solved by a human (server/middleware/captcha.js, 5-minute token expiry) —
// something a background service worker has no way to do. A silent
// background-sync retry would just fail the CAPTCHA check every time.
const CACHE_VERSION = "varshanet-shell-v1";
const SHELL_URLS = [
  "/",
  "/index.html",
  "/manifest.json",
  "/icons/icon-192.png",
  "/icons/icon-512.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => cache.addAll(SHELL_URLS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_VERSION).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // Never touch API calls or non-GET requests — /api/reports, /api/weather,
  // report submissions (POST, multipart), etc. must always hit the network
  // (or fail loudly so the page's own offline-queue code can catch it and
  // save the report locally). Caching or intercepting these would silently
  // return stale data instead of a real report submission result.
  if (req.method !== "GET" || url.pathname.startsWith("/api/")) {
    return;
  }

  // App-shell navigations: try the network first (so people online always
  // get the latest build), fall back to the cached shell when offline.
  if (req.mode === "navigate") {
    event.respondWith(
      fetch(req)
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE_VERSION).then((cache) => cache.put("/index.html", copy));
          return res;
        })
        .catch(() => caches.match("/index.html"))
    );
    return;
  }

  // Static assets (manifest, icons, cdn scripts this page loads): cache-first,
  // fall back to network, and cache whatever the network returns for next time.
  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((res) => {
        if (res && res.status === 200 && url.origin === self.location.origin) {
          const copy = res.clone();
          caches.open(CACHE_VERSION).then((cache) => cache.put(req, copy));
        }
        return res;
      }).catch(() => cached);
    })
  );
});
