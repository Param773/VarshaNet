// One-time migration: re-score every existing report from an automated
// feed source (IMD API, Public Dataset / NDMA SACHET, Weather API) using
// the fixed, source-aware scoreReport() logic — see server/scoring.js.
//
// Why this exists: those three sources were previously scored with the
// exact same rules as an individual citizen's own claim (penalized for no
// photo attached, penalized for no independent weather feed to
// cross-check against), which understated their real trustworthiness —
// they're either the government's own official alert (IMD/SACHET) or
// already cross-checked against live weather data at ingest time (Weather
// API). Reports created going forward already get the correct score from
// worker.js / autoResolve.js; this script only fixes ones that were
// already written to the database before that logic existed.
//
// This does NOT delete or fabricate anything — it only recomputes each
// report's trust/status fields from its own existing text/event/media,
// exactly the same way a fresh ingest would today. Safe to re-run; it's
// idempotent (running it twice just recomputes the same numbers again).
//
// Usage: node server/scripts/rescoreOfficialReports.js
// Needs the same MONGODB_URI as the running server (reads from .env via
// dotenv, same as server/index.js, or from the shell environment).

require("dotenv").config();

const db = require("../db");
const { scoreReport, statusFromTrust } = require("../scoring");

const SOURCES_TO_RESCORE = ["IMD API", "Public Dataset", "Weather API"];

async function main() {
  console.log(`Fetching existing reports for sources: ${SOURCES_TO_RESCORE.join(", ")}...`);
  const reports = await db.getReportsBySource(SOURCES_TO_RESCORE);
  console.log(`Found ${reports.length} report(s) to re-score.`);

  let toUpdate = [];
  let unchanged = 0;

  for (const r of reports) {
    const { trustScore } = scoreReport({
      description: r.text,
      event: r.event,
      source: r.source,
      hasMedia: !!(r.hasPhoto || r.hasVideo),
      mediaReused: false, // not re-checking media dedup here — original hasMedia/text is unchanged
      officialMain: null, // matches what these sources always had at ingest time (see imdCapIngest.js/sachetIngest.js)
      city: r.city,
    });

    // A confirmed duplicate (see worker.js) must stay "flagged" regardless
    // of score, same rule as at ingest time — never let a re-score
    // silently un-flag a known duplicate.
    const isConfirmedDuplicate = r.duplicateOf !== null && r.duplicateOf !== undefined;
    const newStatus = isConfirmedDuplicate ? "flagged" : statusFromTrust(trustScore);

    if (trustScore !== r.trust || newStatus !== r.status) {
      toUpdate.push({ id: r.id, trust: trustScore, status: newStatus });
    } else {
      unchanged += 1;
    }
  }

  const { modifiedCount } = await db.bulkUpdateReportTrust(toUpdate);
  console.log(`Done. Updated ${modifiedCount} report(s), ${unchanged} already had the correct score.`);
  process.exit(0);
}

main().catch((e) => {
  console.error("Rescore failed:", e);
  process.exit(1);
});
