// One-time cleanup: server/worker.js now catches text-duplicate reports
// (same city+event+wording — e.g. the same SACHET/IMD bulletin
// re-published under a new guid) the moment they're ingested, see
// server/textDedup.js. That only protects reports ingested AFTER this
// change shipped — anything already sitting in the "pending" queue from
// before then (like several "Patna, Bihar" rows with identical text) was
// never checked and needs a one-time pass.
//
// This script re-runs that same content-hash check across every currently
// "pending" report, oldest first. For each duplicate cluster it finds, the
// EARLIEST report is left alone (it's the original) and every later one
// is re-stamped:
//   status:     "flagged"              (visible but dimmed, never deleted)
//   decidedBy:  "AI (duplicate content, backfill)"
//   duplicateOf: <id of the earliest report in its cluster>
//
// Nothing is ever deleted or rejected outright — same "flag, don't hide"
// principle as autoResolve.js. An admin can still Approve or Reject each
// one from the Review Queue's "Flagged only" tab afterwards.
//
// Usage (from the repo root, with MONGODB_URI available in your
// environment or a .env file):
//   node scripts/backfill-duplicate-reports.js
//
// Safe to re-run — already-flagged reports are excluded from the scan.

require("dotenv").config();
const { MongoClient } = require("mongodb");
const { buildContentHash } = require("../server/textDedup");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = "varshanet";

async function main() {
  if (!MONGODB_URI) {
    console.error("MONGODB_URI is not set — add it to your environment or .env file.");
    process.exit(1);
  }

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const reports = client.db(DB_NAME).collection("reports");

  const pending = await reports
    .find({ status: "pending" }, { projection: { mediaHash: 0, perceptualHash: 0 } })
    .sort({ ts: 1 }) // oldest first, so the first one seen in each cluster is treated as the original
    .toArray();

  console.log(`Scanning ${pending.length} pending report(s) for text duplicates...`);

  const seenByHash = new Map(); // contentHash -> earliest report's id in this cluster
  let flaggedCount = 0;

  for (const r of pending) {
    const hash = buildContentHash(r.city, r.event, r.text);
    const originalId = seenByHash.get(hash);

    if (originalId === undefined) {
      seenByHash.set(hash, r.id);
      // Stamp the contentHash even on the "original" — otherwise it has
      // no contentHash on record (or a stale one from before this fix),
      // and worker.js's live duplicate check for any FUTURE re-published
      // alert has nothing to match against.
      await reports.updateOne({ id: r.id }, { $set: { contentHash: hash } });
      continue; // first report in this cluster — leave its status as-is
    }

    await reports.updateOne(
      { id: r.id },
      {
        $set: {
          status: "flagged",
          decidedBy: "AI (duplicate content, backfill)",
          duplicateOf: originalId,
          contentHash: hash,
        },
      }
    );
    flaggedCount += 1;
    console.log(`  #${r.id} (${r.city}, ${r.state}) → flagged as duplicate of #${originalId}`);
  }

  console.log(`Done. ${flaggedCount} report(s) flagged as duplicates out of ${pending.length} pending.`);

  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Backfill failed:", err);
  process.exit(1);
});
