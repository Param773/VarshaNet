// MongoDB-backed data store (migrated from the original JSON-file store).
//
// Every other file in this project only talks to the functions exported
// here — none of them know or care that reports live in MongoDB instead of
// a JSON file on disk. That's deliberate: it's the same function names and
// shapes as before, just async now, so the migration didn't touch scoring,
// routes, or the ingestion job's actual logic — only added `await`.
//
// This also fixes a real limitation of the old JSON-file store: on
// Render's free tier the disk is ephemeral, so every redeploy/restart wiped
// all citizen-submitted and live-ingested reports back to just the seed
// data. A real external database persists across restarts.
//
// Needs MONGODB_URI set in the environment — a free MongoDB Atlas cluster
// is enough for this scale.

const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = "varshanet";

let client = null;
let dbHandle = null;
let reportsCollection = null;
let countersCollection = null;
let adminsCollection = null;
let connectPromise = null;

async function connect() {
  if (dbHandle) return dbHandle;
  if (connectPromise) return connectPromise;

  connectPromise = (async () => {
    if (!MONGODB_URI) {
      throw new Error("MONGODB_URI is not set — add it to your environment variables.");
    }
    client = new MongoClient(MONGODB_URI);
    await client.connect();
    dbHandle = client.db(DB_NAME);
    reportsCollection = dbHandle.collection("reports");
    countersCollection = dbHandle.collection("counters");
    adminsCollection = dbHandle.collection("admins");
    await reportsCollection.createIndex({ id: 1 }, { unique: true });
    await reportsCollection.createIndex({ mediaHash: 1 });
    await adminsCollection.createIndex({ username: 1 }, { unique: true });
    return dbHandle;
  })();

  return connectPromise;
}

async function nextSequence() {
  const result = await countersCollection.findOneAndUpdate(
    { _id: "reportId" },
    { $inc: { seq: 1 } },
    { upsert: true, returnDocument: "after" }
  );
  return result.seq;
}

function stripMongoId(doc) {
  if (!doc) return null;
  const { _id, ...rest } = doc;
  return rest;
}

const DEFAULT_LIST_LIMIT = 500;
const MAX_LIST_LIMIT = 1000;

async function getAllReports({ limit = DEFAULT_LIST_LIMIT } = {}) {
  await connect();
  const cappedLimit = Math.min(Math.max(1, limit), MAX_LIST_LIMIT);
  const docs = await reportsCollection
    .find({}, { projection: { mediaHash: 0, perceptualHash: 0 } })
    .sort({ id: -1 })
    .limit(cappedLimit)
    .toArray();
  return docs.reverse().map(stripMongoId);
}

async function getReportsCount() {
  await connect();
  return reportsCollection.countDocuments();
}

// True, collection-wide stats for the admin dashboard (total count, avg
// trust, top event/state/source, reports-by-source breakdown, media %).
// Computed entirely inside MongoDB via aggregation ($facet) — the raw
// documents never come back to Node, only these small summary numbers, so
// this stays cheap and accurate no matter how large the collection grows
// (unlike computing these client-side from the capped getAllReports() list,
// which only reflects the most recent DEFAULT_LIST_LIMIT reports).
async function getReportStats() {
  await connect();

  const [facet] = await reportsCollection
    .aggregate([
      {
        $facet: {
          totalCount: [{ $count: "count" }],
          pendingCount: [{ $match: { status: "pending" } }, { $count: "count" }],
          flaggedCount: [{ $match: { status: "flagged" } }, { $count: "count" }],
          avgTrust: [
            { $match: { status: { $in: ["verified", "pending", "flagged"] } } },
            { $group: { _id: null, avg: { $avg: "$trust" } } },
          ],
          bySource: [
            { $match: { status: { $ne: "rejected" } } },
            { $group: { _id: "$source", count: { $sum: 1 } } },
          ],
          topEvent: [
            { $group: { _id: "$event", count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 1 },
          ],
          topState: [
            { $group: { _id: "$state", count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 1 },
          ],
          topSource: [
            { $group: { _id: "$source", count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 1 },
          ],
          withMediaCount: [
            { $match: { $or: [{ hasPhoto: true }, { hasVideo: true }] } },
            { $count: "count" },
          ],
        },
      },
    ])
    .toArray();

  const first = (arr) => (arr && arr[0]) || null;
  const total = first(facet.totalCount)?.count || 0;

  const bySource = {};
  (facet.bySource || []).forEach((row) => {
    bySource[row._id || "Unknown"] = row.count;
  });

  const DAY_MS = 24 * 60 * 60 * 1000;
  const trendWindowStart = Date.now() - 8 * DAY_MS;
  const recentDocs = await reportsCollection
    .find({ ts: { $gte: trendWindowStart } }, { projection: { ts: 1 } })
    .toArray();

  return {
    total,
    pending: first(facet.pendingCount)?.count || 0,
    flagged: first(facet.flaggedCount)?.count || 0,
    avgTrust: Math.round(first(facet.avgTrust)?.avg || 0),
    bySource,
    topEvent: first(facet.topEvent)?._id || null,
    topState: first(facet.topState)?._id || null,
    topSource: first(facet.topSource)?._id || null,
    mediaPct: total ? Math.round(((first(facet.withMediaCount)?.count || 0) / total) * 100) : 0,
    recentTimestamps: recentDocs.map((d) => d.ts),
  };
}

async function addReport(reportWithoutId) {
  await connect();
  const id = await nextSequence();
  const report = { id, ...reportWithoutId };
  await reportsCollection.insertOne(report);
  return stripMongoId(report);
}

async function updateReportStatus(id, status) {
  await connect();
  const update = { $set: { status } };
  if (status === "verified") update.$set.duplicateOf = null;
  const updated = await reportsCollection.findOneAndUpdate({ id }, update, {
    returnDocument: "after",
  });
  return stripMongoId(updated);
}

async function findByMediaHash(hash) {
  if (!hash) return null;
  await connect();
  const doc = await reportsCollection.findOne({ mediaHash: hash });
  return stripMongoId(doc);
}

const NEAR_DUPLICATE_SCAN_LIMIT = 2000;

async function findNearDuplicateByPerceptualHash(hash, maxDistance) {
  if (!hash) return null;
  await connect();
  const { hammingDistance } = require("./perceptualHash");
  const candidates = await reportsCollection
    .find(
      { perceptualHash: { $exists: true, $ne: null } },
      { projection: { id: 1, perceptualHash: 1 } }
    )
    .sort({ id: -1 })
    .limit(NEAR_DUPLICATE_SCAN_LIMIT)
    .toArray();

  let best = null;
  let bestDist = Infinity;
  candidates.forEach((r) => {
    const dist = hammingDistance(hash, r.perceptualHash);
    if (dist <= maxDistance && dist < bestDist) {
      bestDist = dist;
      best = r;
    }
  });
  return stripMongoId(best);
}

async function getAdminByUsername(username) {
  await connect();
  const doc = await adminsCollection.findOne({ username });
  return stripMongoId(doc);
}

async function createAdmin(username, passwordHash) {
  await connect();
  const existing = await adminsCollection.findOne({ username });
  if (existing) throw new Error("An admin with that username already exists.");
  const admin = { username, passwordHash, createdAt: Date.now() };
  await adminsCollection.insertOne(admin);
  return stripMongoId(admin);
}

async function listAdminUsernames() {
  await connect();
  const docs = await adminsCollection
    .find({}, { projection: { username: 1, createdAt: 1 } })
    .sort({ createdAt: 1 })
    .toArray();
  return docs.map((d) => ({ username: d.username, createdAt: d.createdAt }));
}

async function seedDefaultAdminIfEmpty(username, passwordHash) {
  await connect();
  const count = await adminsCollection.countDocuments();
  if (count > 0) return false;
  if (!username || !passwordHash) return false;
  await adminsCollection.insertOne({ username, passwordHash, createdAt: Date.now() });
  return true;
}

async function bulkSeed(reports) {
  await connect();
  const count = await reportsCollection.countDocuments();
  if (count > 0) return false;
  if (!reports.length) return false;

  let nextId = 0;
  const withIds = reports.map((r) => ({ id: nextId++, ...r }));
  await reportsCollection.insertMany(withIds);
  await countersCollection.updateOne(
    { _id: "reportId" },
    { $set: { seq: nextId - 1 } },
    { upsert: true }
  );
  return true;
}

module.exports = {
  connect,
  getAllReports,
  getReportsCount,
  getReportStats,
  addReport,
  updateReportStatus,
  findByMediaHash,
  findNearDuplicateByPerceptualHash,
  bulkSeed,
  getAdminByUsername,
  createAdmin,
  listAdminUsernames,
  seedDefaultAdminIfEmpty,
};