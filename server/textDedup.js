// Content-based duplicate detection for TEXT reports.
//
// server/perceptualHash.js already catches near-identical PHOTOS/VIDEOS.
// Nothing in the pipeline did the equivalent for text — so when NDMA's
// SACHET feed (or IMD CAP) re-publishes the same bulletin under a brand
// new <guid> (which it does — the per-source seenGuids check in
// sachetIngest.js/imdCapIngest.js only catches an EXACT guid repeat, not
// the same alert reissued with a different one), every reissue sailed
// through as a fresh, independent report: same city, same event, same
// text, three separate rows in the review queue.
//
// This module gives worker.js a single, source-agnostic way to ask
// "have we already stored this exact alert recently?" — normalize the
// text so trivial formatting differences (case, whitespace, a trailing
// "…" from truncation) don't cause a false miss, then hash it together
// with city+event so "same wording, different place" isn't a false hit.
//
// Why a PREFIX, not the whole text: server/stateLocations.js only
// resolves alerts to a state's one representative city (e.g. every IMD
// Patna-office bulletin becomes "Patna, Bihar", regardless of which
// district it's actually about — see its own comment for why). Two
// bulletins that are genuinely the same re-issued alert share IMD's
// boilerplate nowcast opening (time window, event, wind speed) — which is
// exactly what a reader sees before the review queue's card truncates the
// text — but can still differ later in the string (a trailing district
// list, a "valid till" clause), which made a full-text hash miss real
// duplicates that were visually indistinguishable in the queue. Hashing
// just the opening keeps genuinely different bulletins apart (different
// time window or wind speed shows up within this many characters) while
// no longer being defeated by a trailing difference nobody can even see.
const CONTENT_HASH_PREFIX_CHARS = 140;

const crypto = require("crypto");

function normalizeText(text) {
  return (text || "")
    .toLowerCase()
    .replace(/[…"'".,!?]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function buildContentHash(city, event, text) {
  const prefix = normalizeText(text).slice(0, CONTENT_HASH_PREFIX_CHARS);
  const normalized = `${(city || "").toLowerCase()}|${event || ""}|${prefix}`;
  return crypto.createHash("sha1").update(normalized).digest("hex");
}

module.exports = { normalizeText, buildContentHash, CONTENT_HASH_PREFIX_CHARS };
