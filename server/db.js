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
let auditLogsCollection = null;
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
    auditLogsCollection = dbHandle.collection("auditLogs");
    await reportsCollection.createIndex({ id: 1 }, { unique: true });
    await reportsCollection.createIndex({ mediaHash: 1 });
    // contentHash — same idea as mediaHash but for TEXT reports (see
    // server/textDedup.js): lets the worker catch the same alert being
    // re-published under a new guid before it becomes a second row in
    // the review queue.
    await reportsCollection.createIndex({ contentHash: 1 });
    // status: 1 — the auto-resolve sweep's first query is always "give me
    // every pending report" (server/autoResolve.js); city+event+ts — its
    // second query, per pending report, is "how many other reports share
    // this city+event within the corroboration window" (also what
    // dashboard filters query on, so it pulls double duty).
    await reportsCollection.createIndex({ status: 1 });
    await reportsCollection.createIndex({ city: 1, event: 1, ts: 1 });
    await adminsCollection.createIndex({ username: 1 }, { unique: true });
    await auditLogsCollection.createIndex({ ts: -1 });
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
//
// topEvent/topState/topSource (and bySource) all exclude status "rejected"
// so these "most active" figures match what's actually visible to users
// elsewhere in the app (e.g. the Live Dashboard's non-rejected tallies) —
// otherwise rejected/duplicate reports could make an admin-only stat like
// "Most active state" disagree with the public-facing one.
// Small, honest set of aggregate numbers safe to show on the PUBLIC
// landing page (no admin auth) — deliberately coarser than
// getReportStats() below, which is admin-only and has richer breakdowns
// not appropriate to expose without login. Computed via Mongo aggregation
// so it stays accurate as the collection grows, unlike computing these
// from the capped getAllReports() list the dashboard uses (only reflects
// the most recent DEFAULT_LIST_LIMIT reports — fine for a live feed,
// wrong for a headline "total reports ingested" claim).
async function getPublicStats() {
  await connect();
  const [facet] = await reportsCollection
    .aggregate([
      {
        $facet: {
          total: [{ $count: "count" }],
          cities: [{ $group: { _id: "$city" } }, { $count: "count" }],
          verified: [{ $match: { status: "verified" } }, { $count: "count" }],
          // Genuinely suspicious/rejected content only — excludes confirmed
          // duplicates (duplicateOf set). A duplicate is the SAME real
          // government alert re-published under a new guid (IMD/SACHET
          // both do this on every ingestion run — see worker.js), not a
          // fake or low-quality report; lumping it in here would call a
          // second copy of a genuine alert "filtered out as fake", which
          // isn't true and understates real data quality.
          trulyFlagged: [
            { $match: { status: { $in: ["flagged", "rejected"] }, duplicateOf: null } },
            { $count: "count" },
          ],
          // Same exclusion for the rate's denominator — a duplicate was
          // never itself judged genuine-vs-suspicious, it was just
          // deduplicated, so it shouldn't count either way toward
          // "verification rate".
          decided: [
            { $match: { status: { $ne: "pending" }, duplicateOf: null } },
            { $count: "count" },
          ],
        },
      },
    ])
    .toArray();

  const first = (arr) => (arr && arr[0] && arr[0].count) || 0;
  const decided = first(facet.decided);

  return {
    total: first(facet.total),
    cities: first(facet.cities),
    filtered: first(facet.trulyFlagged),
    // % of reports that have left "pending" and were confirmed genuine
    // (i.e. NOT flagged/rejected), excluding confirmed duplicates from
    // both sides of the ratio. 0 rather than null when nothing's been
    // decided yet, so the landing page shows "0%", not a broken counter.
    verificationRatePct: decided ? Math.round((first(facet.verified) / decided) * 100) : 0,
  };
}

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
            { $match: { status: { $ne: "rejected" } } },
            { $group: { _id: "$event", count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 1 },
          ],
          topState: [
            { $match: { status: { $ne: "rejected" } } },
            { $group: { _id: "$state", count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 1 },
          ],
          topSource: [
            { $match: { status: { $ne: "rejected" } } },
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
  // decidedBy/corroborationCount default to "not yet decided by anyone" —
  // callers that already know the outcome (initial ML score landed outside
  // the pending band, or the auto-resolve sweep) pass real values via the
  // spread below. See server/autoResolve.js and README's "Explainability"
  // notes for what the decidedBy values mean.
  const report = { decidedBy: null, corroborationCount: 0, ...reportWithoutId, id };
  await reportsCollection.insertOne(report);
  return stripMongoId(report);
}

// Admin/moderator action from the review queue — always applies,
// overriding whatever an AI decision (or lack of one) was before it. A
// human clicking Approve/Reject is meant to be the final word even if the
// auto-resolve sweep already verified/flagged the same report.
async function updateReportStatus(id, status, decidedBy) {
  await connect();
  const update = { $set: { status } };
  if (decidedBy !== undefined) update.$set.decidedBy = decidedBy;
  if (status === "verified") update.$set.duplicateOf = null;
  const updated = await reportsCollection.findOneAndUpdate({ id }, update, {
    returnDocument: "after",
  });
  return stripMongoId(updated);
}

// One-off migration support (see server/scripts/rescoreOfficialReports.js):
// fetch every existing report from a given set of sources so a fixed
// scoring rule can be re-applied to reports that were already scored under
// the old logic, without touching anything else in the document.
async function getReportsBySource(sources) {
  await connect();
  const docs = await reportsCollection.find({ source: { $in: sources } }).toArray();
  return docs.map(stripMongoId);
}

// Companion to getReportsBySource — writes back a freshly computed
// trust/status pair for one report by its (non-Mongo) sequential id.
async function updateReportTrust(id, trust, status) {
  await connect();
  await reportsCollection.updateOne({ id }, { $set: { trust, status } });
}

// Bulk version of the above — sends every changed report's new trust/
// status in a single round-trip to MongoDB instead of one request per
// report. Doing thousands of individual updateOne() calls in a row (the
// original approach) was slow enough on a large existing dataset to risk
// Render's free-tier proxy timing the request out before it finished;
// bulkWrite batches them all into one network operation. updates is an
// array of { id, trust, status }; ordered:false lets MongoDB run them in
// any order and keep going even if one somehow fails, rather than
// aborting the whole batch on the first error.
async function bulkUpdateReportTrust(updates) {
  await connect();
  if (!updates.length) return { modifiedCount: 0 };
  const ops = updates.map((u) => ({
    updateOne: {
      filter: { id: u.id },
      update: { $set: { trust: u.trust, status: u.status } },
    },
  }));
  const result = await reportsCollection.bulkWrite(ops, { ordered: false });
  return { modifiedCount: result.modifiedCount || 0 };
}

// Auto-resolve-only version of the above: ONLY applies if the report is
// still "pending" right now. This is what makes the sweep safe to run
// concurrently — from multiple worker.js instances (see its consumer-group
// scaling story), or a timeout-pass racing a corroboration-pass in the same
// sweep. Two writers can both decide "resolve report #42", but only the one
// whose findOneAndUpdate still finds status:"pending" actually changes
// anything; the other gets null back and just moves on. An admin action via
// updateReportStatus() above is unaffected either way — it doesn't check
// current status, so a human always has the final word.
async function resolveIfPending(id, fields) {
  await connect();
  const update = { $set: fields };
  if (fields.status === "verified") update.$set.duplicateOf = null;
  const updated = await reportsCollection.findOneAndUpdate(
    { id, status: "pending" },
    update,
    { returnDocument: "after" }
  );
  return stripMongoId(updated); // null means someone else resolved it first — not an error
}

// Every currently-pending report, for the auto-resolve sweep to walk.
// Unbounded by design (unlike getAllReports' capped window) — the whole
// point is to catch every report stuck in "pending", not just recent ones.
async function getPendingReports() {
  await connect();
  const docs = await reportsCollection
    .find({ status: "pending" }, { projection: { mediaHash: 0, perceptualHash: 0 } })
    .toArray();
  return docs.map(stripMongoId);
}

// Unbounded (same reasoning as getPendingReports above) — the Admin
// Console's Review Queue tabs (Needs review / Low trust / Flagged / All)
// need to see and act on EVERY report still awaiting a decision, not just
// whichever ones happen to fall inside getAllReports()'s capped recent-500
// window. Before this, "Pending review: 25" (a true whole-collection count
// from getReportStats) and the "Needs review" tab (computed client-side
// from that capped window) could disagree — e.g. show 25 vs 11 — because
// older pending/flagged reports had aged out of the recent-500 window and
// were then simply unreachable: not visible, not approvable, not
// rejectable, only ever resolvable automatically by autoResolve.js.
//
// This mirrors renderAdmin()'s isNeedsReview / isLowTrust / isFlagged
// predicates in public/index.html — keep the two in sync if those
// definitions ever change:
//   isNeedsReview: status === "pending" && !duplicateOf
//   isLowTrust:    status === "flagged" && !duplicateOf
//   isFlagged:     duplicateOf is set && status !== "rejected"
// The union of all three is exactly: status in [pending, flagged], OR
// duplicateOf is set and status isn't "rejected".
async function getQueueReports() {
  await connect();
  const docs = await reportsCollection
    .find(
      {
        $or: [
          { status: { $in: ["pending", "flagged"] } },
          { duplicateOf: { $ne: null }, status: { $ne: "rejected" } },
        ],
      },
      { projection: { mediaHash: 0, perceptualHash: 0 } }
    )
    .toArray();
  return docs.map(stripMongoId);
}

// Every report (any status except "rejected" — a report someone already
// rejected shouldn't be able to lend credibility to a sibling) for the same
// city+event whose timestamp falls within `windowMs` of `ts` in either
// direction. This is corroboration: independently-sourced reports agreeing
// on the same event, not the same report counted twice.
async function findCorroborationCluster(city, event, ts, windowMs) {
  await connect();
  const docs = await reportsCollection
    .find(
      {
        city,
        event,
        ts: { $gte: ts - windowMs, $lte: ts + windowMs },
        status: { $in: ["pending", "verified"] },
      },
      { projection: { id: 1, status: 1, ts: 1 } }
    )
    .toArray();
  return docs.map(stripMongoId);
}

async function findByMediaHash(hash) {
  if (!hash) return null;
  await connect();
  const doc = await reportsCollection.findOne({ mediaHash: hash });
  return stripMongoId(doc);
}

// Text equivalent of findByMediaHash — used by worker.js right before
// inserting a new report so a re-issued alert (same city+event+wording,
// new guid) links back to the original instead of becoming a fresh
// "needs review" row. Only looks at recent reports (windowMs) and skips
// anything already rejected, so an old, since-cleared report can't keep
// matching forever.
async function findRecentDuplicateByContentHash(hash, windowMs) {
  if (!hash) return null;
  await connect();
  const doc = await reportsCollection.findOne(
    {
      contentHash: hash,
      ts: { $gte: Date.now() - windowMs },
      status: { $ne: "rejected" },
    },
    { sort: { ts: -1 }, projection: { id: 1, ts: 1, status: 1 } }
  );
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

// --- Admin accounts ---
// Every admin now has a `role`: "admin" (full access — approve/reject,
// pull live data, manage other admins), or "moderator" (approve/reject,
// pull live data, but can't manage admins). Older accounts created before
// roles existed are treated as "admin" by getAdminByUsername so nobody
// already using the console gets silently locked out.

async function getAdminByUsername(username) {
  await connect();
  const doc = await adminsCollection.findOne({ username });
  if (!doc) return null;
  const admin = stripMongoId(doc);
  if (!admin.role) admin.role = "admin"; // backfill pre-RBAC accounts
  return admin;
}

async function createAdmin(username, passwordHash, role = "moderator") {
  await connect();
  const existing = await adminsCollection.findOne({ username });
  if (existing) throw new Error("An admin with that username already exists.");
  const admin = { username, passwordHash, role, createdAt: Date.now() };
  await adminsCollection.insertOne(admin);
  return stripMongoId(admin);
}

async function listAdminUsernames() {
  await connect();
  const docs = await adminsCollection
    .find({}, { projection: { username: 1, role: 1, createdAt: 1 } })
    .sort({ createdAt: 1 })
    .toArray();
  return docs.map((d) => ({
    username: d.username,
    role: d.role || "admin", // backfill pre-RBAC accounts
    createdAt: d.createdAt,
  }));
}

async function deleteAdmin(username) {
  await connect();
  const result = await adminsCollection.deleteOne({ username });
  return result.deletedCount > 0;
}

// Bootstraps the very first admin from the ADMIN_USERNAME/ADMIN_PASSWORD_HASH
// env vars (the original single-admin setup), so existing deployments keep
// working without any manual migration step. No-ops once any admin exists.
// The bootstrap account always gets the full "admin" role.
async function seedDefaultAdminIfEmpty(username, passwordHash) {
  await connect();
  const count = await adminsCollection.countDocuments();
  if (count > 0) return false;
  if (!username || !passwordHash) return false;
  await adminsCollection.insertOne({ username, passwordHash, role: "admin", createdAt: Date.now() });
  return true;
}

// --- Audit log ---
// A permanent, append-only record of moderation actions — who approved or
// rejected which report, who created a new admin account, who triggered a
// manual data pull, and when. This is what the old "verified this session"
// counter couldn't do: it reset on every page reload and told you nothing
// about *who* acted or *when*, which is what a real moderation team (and
// a judge asking "how do you know who verified this?") actually needs.
const AUDIT_LIST_LIMIT = 200;

async function addAuditLog(entry) {
  await connect();
  const doc = { ts: Date.now(), ...entry };
  await auditLogsCollection.insertOne(doc);
  return stripMongoId(doc);
}

async function getAuditLogs({ limit = AUDIT_LIST_LIMIT } = {}) {
  await connect();
  const cappedLimit = Math.min(Math.max(1, limit), AUDIT_LIST_LIMIT);
  const docs = await auditLogsCollection
    .find({})
    .sort({ ts: -1 })
    .limit(cappedLimit)
    .toArray();
  return docs.map(stripMongoId);
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
  getPublicStats,
  addReport,
  updateReportStatus,
  resolveIfPending,
  getPendingReports,
  getQueueReports,
  findCorroborationCluster,
  findByMediaHash,
  findNearDuplicateByPerceptualHash,
  findRecentDuplicateByContentHash,
  bulkSeed,
  getAdminByUsername,
  createAdmin,
  listAdminUsernames,
  deleteAdmin,
  seedDefaultAdminIfEmpty,
  addAuditLog,
  getAuditLogs,
  getReportsBySource,
  updateReportTrust,
  bulkUpdateReportTrust,
};