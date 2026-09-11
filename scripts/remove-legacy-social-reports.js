// One-time cleanup: permanently deletes every report tagged with a source
// from a social platform that no longer has a live ingestion pipeline in
// this project.
//
//   - "X / Twitter" — never had a working adapter in this project; dead
//     since Twitter/X locked its search API behind a paid tier.
//   - "Social Media (Bluesky)" — blueskyIngest.js, retired after Bluesky
//     closed unauthenticated post search (see git history).
//   - "Social Media (Reddit)" — the original social adapter (socialIngest.js,
//     since deleted), stopped being viable when Reddit closed free
//     unauthenticated .json access platform-wide on 28-30 May 2026.
//
// NOTE: "Social Media (Mastodon)" is deliberately NOT in this list — that's
// the current live social source (see server/mastodonIngest.js, wired up in
// server/index.js via runMastodonIngestion). Deleting it would wipe live
// data, not dead data.
//
// None of the three removed sources has any live code left in this repo,
// so any report with one of these `source` values is old data that will
// never be refreshed — removing them stops them cluttering the Review
// Queue and the Admin Console's stats widgets (Top source, Reports by
// Source, etc).
//
// Usage (from the repo root, with MONGODB_URI available in your
// environment or a .env file):
//   node scripts/remove-legacy-social-reports.js
//
// Safe to re-run — if there's nothing left to delete, it just reports 0.

require("dotenv").config();
const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = "varshanet";
const SOURCES_TO_REMOVE = ["X / Twitter", "Social Media (Bluesky)", "Social Media (Reddit)"];

async function main() {
  if (!MONGODB_URI) {
    console.error("MONGODB_URI is not set — add it to your environment or .env file.");
    process.exit(1);
  }

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const reports = client.db(DB_NAME).collection("reports");

  const filter = { source: { $in: SOURCES_TO_REMOVE } };
  const matching = await reports.countDocuments(filter);
  console.log(`Found ${matching} report(s) from a removed social source (${SOURCES_TO_REMOVE.join(", ")}).`);

  if (matching === 0) {
    console.log("Nothing to delete.");
    await client.close();
    process.exit(0);
  }

  const result = await reports.deleteMany(filter);
  console.log(`Deleted ${result.deletedCount} report(s).`);

  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Cleanup failed:", err);
  process.exit(1);
});
