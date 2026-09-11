// Offline report queue — plain IndexedDB, no framework. Kept separate from
// the service worker on purpose: replaying a queued report needs a human to
// solve a fresh CAPTCHA (server/middleware/captcha.js tokens expire after 5
// minutes, so a captcha solved while filling the form offline would already
// be stale by the time connectivity returns) — something only page JS with
// a visible UI can do, not a background service worker.
(function (global) {
  var DB_NAME = "varshanet-offline";
  var DB_VERSION = 1;
  var STORE = "queued-reports";
  var dbPromise = null;

  function openDb() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise(function (resolve, reject) {
      if (!("indexedDB" in global)) { reject(new Error("IndexedDB unsupported")); return; }
      var req = global.indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = function () {
        var db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: "id", autoIncrement: true });
        }
      };
      req.onsuccess = function () { resolve(req.result); };
      req.onerror = function () { reject(req.error); };
    });
    return dbPromise;
  }

  function withStore(mode, fn) {
    return openDb().then(function (db) {
      return new Promise(function (resolve, reject) {
        var tx = db.transaction(STORE, mode);
        var store = tx.objectStore(STORE);
        var result;
        Promise.resolve(fn(store)).then(function (r) { result = r; }).catch(reject);
        tx.oncomplete = function () { resolve(result); };
        tx.onerror = function () { reject(tx.error); };
        tx.onabort = function () { reject(tx.error); };
      });
    });
  }

  // fields: plain object of form values (category, description, city, state,
  // lat, lng, website, formLoadedAt). file: the raw File object or null —
  // stored as-is since IndexedDB can hold Blobs/Files directly.
  function enqueue(fields, file) {
    var record = {
      fields: fields,
      file: file || null,
      fileName: file ? file.name : null,
      fileType: file ? file.type : null,
      queuedAt: Date.now(),
    };
    return withStore("readwrite", function (store) {
      return new Promise(function (resolve, reject) {
        var req = store.add(record);
        req.onsuccess = function () { resolve(req.result); };
        req.onerror = function () { reject(req.error); };
      });
    });
  }

  function list() {
    return withStore("readonly", function (store) {
      return new Promise(function (resolve, reject) {
        var req = store.getAll();
        req.onsuccess = function () { resolve(req.result || []); };
        req.onerror = function () { reject(req.error); };
      });
    });
  }

  function remove(id) {
    return withStore("readwrite", function (store) {
      return new Promise(function (resolve, reject) {
        var req = store.delete(id);
        req.onsuccess = function () { resolve(); };
        req.onerror = function () { reject(req.error); };
      });
    });
  }

  function count() {
    return list().then(function (items) { return items.length; });
  }

  global.ReportOfflineQueue = { enqueue: enqueue, list: list, remove: remove, count: count };
})(window);
