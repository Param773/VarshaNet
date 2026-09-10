// searchRouter.js — DROP-IN full-text + geo search for VarshaNet reports.
//
// Fixes: every existing dashboard filter only ever looks at a capped
// recent-window of reports fetched into the browser (GET /api/reports),
// filtered client-side. This file adds a NEW endpoint,
// GET /api/reports/search, that queries the WHOLE MongoDB collection
// directly using Mongo's built-in full-text ($text) index — so search
// actually scales past whatever's currently loaded in a browser tab.
//
// Zero new npm packages needed (uses the `mongodb` driver already in this
// project's package.json). Doesn't touch server/db.js, server/routes,
// or public/index.html — just add this one file, then wire it in with
// the two lines shown at the bottom of this comment.
//
// ---------------------------------------------------------------------
// HOW TO USE (2-line wiring in server/index.js — nothing else to change):
//
//   const attachReportSearch = require("./searchRouter");
//   attachReportSearch(app);
//
// Put those two lines right after this existing line in server/index.js:
//   app.use("/api/reports", reportsRouter);
//
// That's it. Restart the server. Then:
//   GET /api/reports/search?q=flooded+market
//   GET /api/reports/search?q=rain&city=Mumbai&status=verified
//   GET /api/reports/search?event=flood&from=1690000000000&to=1699999999000
//   GET /api/reports/search?lat=19.07&lng=72.87&radiusKm=25
// ---------------------------------------------------------------------

const express = require("express");
const { MongoClient } = require("mongodb");

const router = express.Router();

let reportsCollection = null;
let connectPromise = null;

async function getCollection() {
  if (reportsCollection) return reportsCollection;
  if (!connectPromise) {
    connectPromise = (async () => {
      const uri = process.env.MONGODB_URI;
      if (!uri) throw new Error("MONGODB_URI is not set.");
      const client = new MongoClient(uri);
      await client.connect();
      const col = client.db("varshanet").collection("reports");

      // Text index across the three fields worth free-text searching.
      // Safe to call every boot — Mongo no-ops if it already exists, and
      // this is what makes ?q= search the WHOLE collection efficiently
      // instead of scanning every document with a regex.
      await col
        .createIndex(
          { text: "text", city: "text", state: "text" },
          { name: "varshanet_search_text_idx", weights: { text: 3, city: 2, state: 1 } }
        )
        .catch((e) => console.error("searchRouter: text index setup failed:", e.message));

      reportsCollection = col;
      return col;
    })();
  }
  return connectPromise;
}

// Cheap bounding-box radius filter (no geo index required) — good enough
// for "reports near this point" without reshaping existing lat/lng fields
// into GeoJSON. 1 degree latitude ≈ 111km; longitude degrees are scaled by
// latitude so the box is roughly square in real-world distance.
function boundingBox(lat, lng, radiusKm) {
  const latDelta = radiusKm / 111;
  const lngDelta = radiusKm / (111 * Math.cos((lat * Math.PI) / 180) || 1);
  return {
    lat: { $gte: lat - latDelta, $lte: lat + latDelta },
    lng: { $gte: lng - lngDelta, $lte: lng + lngDelta },
  };
}

router.get("/search", async (req, res) => {
  try {
    const col = await getCollection();
    const { q, city, state, event, status } = req.query;
    const from = req.query.from ? parseInt(req.query.from, 10) : null;
    const to = req.query.to ? parseInt(req.query.to, 10) : null;
    const lat = req.query.lat !== undefined ? parseFloat(req.query.lat) : null;
    const lng = req.query.lng !== undefined ? parseFloat(req.query.lng) : null;
    const radiusKm = req.query.radiusKm ? parseFloat(req.query.radiusKm) : null;
    const limit = Math.min(Math.max(1, parseInt(req.query.limit, 10) || 200), 1000);

    const query = {};
    if (q && q.trim()) query.$text = { $search: q.trim() };
    if (city) query.city = new RegExp(`^${city.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i");
    if (state && state !== "all") query.state = state;
    if (event && event !== "all") query.event = event;
    if (status && status !== "all") query.status = status;
    if (from || to) {
      query.ts = {};
      if (from) query.ts.$gte = from;
      if (to) query.ts.$lte = to;
    }
    if (lat !== null && lng !== null && radiusKm) {
      Object.assign(query, boundingBox(lat, lng, radiusKm));
    }

    const projection = { mediaHash: 0, perceptualHash: 0 };
    const cursor = col.find(query, { projection });

    // Relevance-sort when there's a free-text query, otherwise newest first.
    if (q && q.trim()) {
      cursor.project({ ...projection, score: { $meta: "textScore" } }).sort({ score: { $meta: "textScore" } });
    } else {
      cursor.sort({ ts: -1 });
    }

    const docs = await cursor.limit(limit).toArray();
    const reports = docs.map(({ _id, score, ...rest }) => rest);

    res.json({ reports, total: reports.length, engine: "mongodb-text-search" });
  } catch (err) {
    console.error("searchRouter: search failed:", err);
    res.status(500).json({ error: "Search failed. Please try again." });
  }
});

module.exports = function attachReportSearch(app) {
  app.use("/api/reports", router);
  console.log("searchRouter: GET /api/reports/search is live");
};
