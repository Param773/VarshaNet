// One-time cleanup: permanently deletes every report tagged
// source: "Social Media (Mastodon)" from MongoDB.
//
// Context: mastodonIngest.js (the live ingestion job that created these)
// has been removed from the project, but that only stops NEW Mastodon
// reports from being created — it doesn't touch rows already sitting in
// the database. This script removes those existing rows directly, so they
// stop showing up in the Review Queue, the Admin Console's "Top source" /
// stats widgets, and the public dashboard.
//
// Usage (from the repo root, with MONGODB_URI available in your
// environment or a .env file):
//   node scripts/remove-mastodon-reports.js
//
// Safe to re-run — if there's nothing left to delete, it just reports 0.

require("dotenv").config();
const { MongoClient } = require("mongodb");

const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = "varshanet";
const SOURCE_TO_REMOVE = "Social Media (Mastodon)";

async function main() {
  if (!MONGODB_URI) {
    console.error("MONGODB_URI is not set — add it to your environment or .env file.");
    process.exit(1);
  }

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const reports = client.db(DB_NAME).collection("reports");

  const matching = await reports.countDocuments({ source: SOURCE_TO_REMOVE });
  console.log(`Found ${matching} report(s) with source "${SOURCE_TO_REMOVE}".`);

  if (matching === 0) {
    console.log("Nothing to delete.");
    await client.close();
    process.exit(0);
  }

  const result = await reports.deleteMany({ source: SOURCE_TO_REMOVE });
  console.log(`Deleted ${result.deletedCount} report(s).`);

  await client.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Cleanup failed:", err);
  process.exit(1);
});
