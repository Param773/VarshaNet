// One-time cleanup: server/worker.js now catches text-duplicate reports
// (same city+event+wording — e.g. the same SACHET/IMD bulletin
// re-published under a new guid) the moment they're ingested, see
// server/textDedup.js. That only protects reports ingested AFTER this
// change shipped — anything already sitting in the review queue from
// before then (like several "Patna, Bihar" rows with identical text) was
// never checked and needs a one-time pass.
//
// Scans BOTH "pending" and "flagged" reports — not just "pending". A
// report whose own trust score already lands under 42 becomes "flagged"
// straight away (see scoring.js/statusFromTrust), skipping "pending"
// entirely — so a low-trust duplicate can already be "flagged" for
// reasons that have nothing to do with being a duplicate, and a
// pending-only scan would silently miss it (and never stamp its
// contentHash, so future re-published copies of it can't match it
// either). Both statuses together match exactly what the admin console's
// "Needs review" tab shows (server/../public/index.html: pending OR
// flagged) — this backfill covers everything currently sitting there.
//
// This script re-runs that same content-hash check across every report
// currently in the queue, oldest first. For each duplicate cluster it
// finds, the EARLIEST report is left alone (it's the original — its
// contentHash is still refreshed, so later re-published copies of it can
// be matched going forward) and every later one is re-stamped:
//   status:     "flagged"              (visible but dimmed, never deleted)
//   decidedBy:  "AI (duplicate content, backfill)"
//   duplicateOf: <id of the earliest report in its cluster>
//
// Nothing is ever deleted or rejected outright — same "flag, don't hide"
// principle as autoResolve.js. An admin can still Approve or Reject each
// one from the Review Queue afterwards.
//
// Usage (from the repo root, with MONGODB_URI available in your
// environment or a .env file):
//   node scripts/backfill-duplicate-reports.js
//
// Safe to re-run — reports that are already correctly stamped just get
// the same values written again.

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

  const inQueue = await reports
    .find(
      { status: { $in: ["pending", "flagged"] } },
      { projection: { mediaHash: 0, perceptualHash: 0 } }
    )
    .sort({ ts: 1 }) // oldest first, so the first one seen in each cluster is treated as the original
    .toArray();

  console.log(`Scanning ${inQueue.length} report(s) (pending + flagged) for text duplicates...`);

  const seenByHash = new Map(); // contentHash -> earliest report's id in this cluster
  let flaggedCount = 0;

  for (const r of inQueue) {
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

  console.log(`Done. ${flaggedCount} report(s) flagged as duplicates out of ${inQueue.length} scanned.`);

  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Backfill failed:", err);
  process.exit(1);
});
