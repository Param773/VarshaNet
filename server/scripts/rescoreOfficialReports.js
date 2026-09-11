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
// Weather API + duplicateOf: worker.js used to run every automated feed's
// text through the 24h content-hash dedup check, which force-flagged any
// Weather API report whose templated text ("Ongoing rainfall recorded
// over X...") matched an earlier report for a still-ongoing event — not a
// real duplicate, just the same real weather still happening (see
// worker.js's SKIP_CONTENT_DEDUP_SOURCES). worker.js no longer does this
// for new reports, but existing rows already have a stale duplicateOf
// pointing at that false match. This script clears duplicateOf for
// Weather API rows before rescoring so they can actually leave "flagged"
// instead of a leftover duplicateOf silently forcing them right back.
// IMD API / Public Dataset keep their duplicateOf as-is — that dedup
// check is still correct for them (catches a bulletin genuinely
// re-published under a new guid) and wasn't changed.
//
// Usage: node server/scripts/rescoreOfficialReports.js
// Needs the same MONGODB_URI as the running server (reads from .env via
// dotenv, same as server/index.js, or from the shell environment).

require("dotenv").config();

const db = require("../db");
const { scoreReport, statusFromTrust } = require("../scoring");

const SOURCES_TO_RESCORE = ["IMD API", "Public Dataset", "Weather API"];
const NO_LONGER_DEDUPED_SOURCES = new Set(["Weather API"]);

async function main() {
  console.log(`Fetching existing reports for sources: ${SOURCES_TO_RESCORE.join(", ")}...`);
  const reports = await db.getReportsBySource(SOURCES_TO_RESCORE);
  console.log(`Found ${reports.length} report(s) to re-score.`);

  let toUpdate = [];
  let unchanged = 0;
  let clearedDuplicateFlag = 0;

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

    const staleDuplicateFlag =
      NO_LONGER_DEDUPED_SOURCES.has(r.source) && r.duplicateOf !== null && r.duplicateOf !== undefined;

    // A confirmed duplicate (see worker.js) must stay "flagged" regardless
    // of score, same rule as at ingest time — never let a re-score
    // silently un-flag a known duplicate. Exception: a Weather API row's
    // duplicateOf is now known stale (see comment above), so it no longer
    // counts as "confirmed" here.
    const isConfirmedDuplicate =
      r.duplicateOf !== null && r.duplicateOf !== undefined && !staleDuplicateFlag;
    const newStatus = isConfirmedDuplicate ? "flagged" : statusFromTrust(trustScore);
    const newDuplicateOf = staleDuplicateFlag ? null : r.duplicateOf;

    if (trustScore !== r.trust || newStatus !== r.status || newDuplicateOf !== r.duplicateOf) {
      toUpdate.push({ id: r.id, trust: trustScore, status: newStatus, duplicateOf: newDuplicateOf });
      if (staleDuplicateFlag) clearedDuplicateFlag += 1;
    } else {
      unchanged += 1;
    }
  }

  const { modifiedCount } = await db.bulkUpdateReportTrust(toUpdate);
  console.log(
    `Done. Updated ${modifiedCount} report(s) (${clearedDuplicateFlag} had a stale Weather API duplicate flag cleared), ${unchanged} already had the correct score.`
  );
  process.exit(0);
}

main().catch((e) => {
  console.error("Rescore failed:", e);
  process.exit(1);
});
