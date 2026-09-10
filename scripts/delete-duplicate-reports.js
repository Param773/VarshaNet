// One-time cleanup: PERMANENTLY deletes every report that's currently
// marked as a duplicate (duplicateOf is set — see server/textDedup.js /
// scripts/backfill-duplicate-reports.js for how that gets set).
//
// This is the destructive alternative to public/index.html's queue
// filter (which just hides duplicates from the actionable tabs without
// ever deleting anything). Reach for that instead unless you specifically
// want duplicate rows gone from the database entirely — a wrong content-
// hash match here can't be undone the way an unhidden report can.
//
// Safety: defaults to a DRY RUN — it only lists what it WOULD delete.
// Nothing is removed unless you pass --confirm.
//
// Usage (from the repo root, with MONGODB_URI available in your
// environment or a .env file):
//   node scripts/delete-duplicate-reports.js            (dry run — lists only)
//   node scripts/delete-duplicate-reports.js --confirm   (actually deletes)

require("dotenv").config();
const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = "varshanet";
const CONFIRMED = process.argv.includes("--confirm");

async function main() {
  if (!MONGODB_URI) {
    console.error("MONGODB_URI is not set — add it to your environment or .env file.");
    process.exit(1);
  }

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const reports = client.db(DB_NAME).collection("reports");

  const filter = { duplicateOf: { $ne: null } };
  const duplicates = await reports
    .find(filter, { projection: { id: 1, city: 1, state: 1, event: 1, duplicateOf: 1 } })
    .sort({ id: 1 })
    .toArray();

  if (duplicates.length === 0) {
    console.log("No reports currently marked as duplicates. Nothing to do.");
    await client.close();
    process.exit(0);
  }

  console.log(`Found ${duplicates.length} report(s) marked as duplicates:`);
  duplicates.forEach((r) => {
    console.log(`  #${r.id} (${r.city}, ${r.state}) — duplicate of #${r.duplicateOf}`);
  });

  if (!CONFIRMED) {
    console.log(
      `\nDRY RUN — nothing deleted. Re-run with --confirm to permanently delete ` +
        `these ${duplicates.length} report(s):\n  node scripts/delete-duplicate-reports.js --confirm`
    );
    await client.close();
    process.exit(0);
  }

  const result = await reports.deleteMany(filter);
  console.log(`\nDeleted ${result.deletedCount} report(s) permanently.`);

  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Delete failed:", err);
  process.exit(1);
});
