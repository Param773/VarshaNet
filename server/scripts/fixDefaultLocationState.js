// One-time migration: fixes reports already written to the database with
// state: "India" — the old value of server/stateLocations.js's
// DEFAULT_LOCATION, used whenever a SACHET/IMD alert's state couldn't be
// resolved from its text/author. That put the country's name in a *state*
// field, which then flowed into every state-level aggregate (the Live
// Dashboard's "Top States" chart, the State filter dropdown, CSV exports)
// as if "India" were itself one of the 28 states/UTs.
//
// DEFAULT_LOCATION now correctly falls back to "Delhi" (matching the New
// Delhi coordinates it already used, and the same convention
// server/socialShared.js already used correctly for Mastodon). Reports
// created going forward already get the correct state; this script only
// fixes rows written before that fix existed.
//
// Safe to re-run: once no report has state "India" left, it's a no-op.
//
// Usage: node server/scripts/fixDefaultLocationState.js
// Needs the same MONGODB_URI as the running server (reads from .env via
// dotenv, same as server/index.js, or from the shell environment).

require("dotenv").config();

const db = require("../db");

const OLD_STATE = "India";
const NEW_STATE = "Delhi";

async function main() {
  console.log(`Renaming reports with state "${OLD_STATE}" to "${NEW_STATE}"...`);
  const { modifiedCount } = await db.renameReportState(OLD_STATE, NEW_STATE);
  console.log(`Done. Updated ${modifiedCount} report(s).`);
  process.exit(0);
}

main().catch((e) => {
  console.error("Migration failed:", e);
  process.exit(1);
});
