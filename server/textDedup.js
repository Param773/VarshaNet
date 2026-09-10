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

const crypto = require("crypto");

function normalizeText(text) {
  return (text || "")
    .toLowerCase()
    .replace(/[…"'".,!?]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function buildContentHash(city, event, text) {
  const normalized = `${(city || "").toLowerCase()}|${event || ""}|${normalizeText(text)}`;
  return crypto.createHash("sha1").update(normalized).digest("hex");
}

module.exports = { normalizeText, buildContentHash };
