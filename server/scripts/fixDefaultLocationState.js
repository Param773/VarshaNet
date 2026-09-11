// One-time migration: fixes reports already written to the database with a
// state value that doesn't match what the live ingestion pipelines (and
// now the Live Dashboard's State filter) actually use — so they get
// stranded outside state-level aggregates (Top States chart, State filter,
// CSV exports) even though the report itself is perfectly valid.
//
// Two known cases, both fixed here:
//
//   1. state: "India" — the old value of server/stateLocations.js's
//      DEFAULT_LOCATION, used whenever a SACHET/IMD alert's state couldn't
//      be resolved from its text/author. That put the country's name in a
//      *state* field, as if "India" were itself one of the 28 states/UTs.
//      DEFAULT_LOCATION now correctly falls back to "Delhi" (matching the
//      New Delhi coordinates it already used, and the same convention
//      server/socialShared.js already used correctly for Mastodon).
//
//   2. state: "Jammu & Kashmir" — the old spelling used by
//      server/seedData.js's demo CITIES list and the frontend's own copy
//      of it, while every live-ingestion source (ingest.js's WATCH_CITIES,
//      server/stateLocations.js) spells it "Jammu and Kashmir". The State
//      filter does an exact string match, so a report seeded/labeled with
//      the "&" spelling could never be found by selecting "Jammu and
//      Kashmir" from the dropdown (and vice versa) — the two spellings
//      also split what should be one state into two separate bars on the
//      Top States chart.
//
// Reports created going forward already get the correct state in both
// cases; this script only fixes rows written before those fixes existed.
//
// Safe to re-run: once no report has either old value left, it's a no-op.
//
// Usage: node server/scripts/fixDefaultLocationState.js
// Needs the same MONGODB_URI as the running server (reads from .env via
// dotenv, same as server/index.js, or from the shell environment).

require("dotenv").config();

const db = require("../db");

const RENAMES = [
  { from: "India", to: "Delhi" },
  { from: "Jammu & Kashmir", to: "Jammu and Kashmir" },
];

async function main() {
  for (const { from, to } of RENAMES) {
    console.log(`Renaming reports with state "${from}" to "${to}"...`);
    const { modifiedCount } = await db.renameReportState(from, to);
    console.log(`  Updated ${modifiedCount} report(s).`);
  }
  console.log("Done.");
  process.exit(0);
}

main().catch((e) => {
  console.error("Migration failed:", e);
  process.exit(1);
});
