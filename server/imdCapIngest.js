// Real ingestion directly from India Meteorological Department's own
// official alert feed. This is IMD's genuine CAP (Common Alerting Protocol)
// feed — linked as "Latest CAP Alerts" from IMD's own official website
// (mausam.imd.gov.in) — hosted on a plain static file host (AWS S3), so
// unlike social media platforms it has no anti-bot protection to work
// around: it's meant to be machine-read.
//
// Unlike the NDMA SACHET feed (server/sachetIngest.js), which aggregates
// alerts from IMD, CWC and state authorities across 12 regional languages,
// this feed is IMD's own national feed, in English, with a clean
// <description> field — no state-by-state URLs needed, one feed covers
// all of India. Reports from here are tagged source: "IMD API", since this
// is, genuinely, IMD's own API.

const db = require("./db");
const { scoreReport, statusFromTrust } = require("./scoring");
const { BoundedSet } = require("./boundedSet");
const { DEFAULT_LOCATION, detectState } = require("./stateLocations");

const FEED_URL = "https://cap-sources.s3.amazonaws.com/in-imd-en/rss.xml";

const CATEGORY_SIGNALS = [
  { key: "flooding", words: ["flood"] },
  { key: "thunderstorm", words: ["thunderstorm", "lightning", "thunder"] },
  { key: "heatwave", words: ["heat wave", "heatwave"] },
  { key: "fog", words: ["fog", "mist"] },
  { key: "dust_storm", words: ["dust storm", "duststorm", "dust"] },
  { key: "strong_winds", words: ["wind", "gale", "gust", "cyclone", "squall"] },
  { key: "rainfall", words: ["rain", "rainfall", "downpour", "drizzle"] },
];

function detectCategory(text) {
  const lower = (text || "").toLowerCase();
  for (const { key, words } of CATEGORY_SIGNALS) {
    if (words.some((w) => lower.indexOf(w) > -1)) return key;
  }
  return null;
}

// Minimal, dependency-free RSS <item> parser — good enough for this feed's
// simple, well-formed structure (no CDATA, no nested items).
function parseRssItems(xml) {
  const items = [];
  const itemBlocks = xml.split("<item>").slice(1);
  itemBlocks.forEach((block) => {
    const titleMatch = block.match(/<title>([\s\S]*?)<\/title>/);
    const descMatch = block.match(/<description>([\s\S]*?)<\/description>/);
    const guidMatch = block.match(/<guid[^>]*>([\s\S]*?)<\/guid>/);
    if (!titleMatch) return;
    items.push({
      title: titleMatch[1].trim(),
      description: descMatch ? descMatch[1].trim() : "",
      guid: guidMatch ? guidMatch[1].trim() : null,
    });
  });
  return items;
}

// Avoid re-creating a report for an alert already ingested — IMD's guids
// (CAP OIDs) are stable per-alert. Bounded so this doesn't grow forever over
// the process's lifetime.
const seenGuids = new BoundedSet(5000);

async function runImdCapIngestion() {
  const created = [];
  try {
    const res = await fetch(FEED_URL, {
      headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
    });
    if (!res.ok) {
      console.log(`IMD CAP feed unavailable (HTTP ${res.status}) — skipping this run.`);
      return created;
    }
    const xml = await res.text();
    const items = parseRssItems(xml).slice(0, 15); // newest 15 per run

    for (const item of items) {
      if (!item.guid || seenGuids.has(item.guid)) continue;

      const combinedText = `${item.title} ${item.description}`;
      const category = detectCategory(combinedText);
      if (!category) continue;

      const loc = detectState(combinedText) || DEFAULT_LOCATION;
      const description = (item.description || item.title).slice(0, 300);

      const { trustScore } = scoreReport({
        description,
        event: category,
        hasMedia: false,
        mediaReused: false,
        officialMain: null,
        city: loc.city,
      });
      const status = statusFromTrust(trustScore);

      const report = await db.addReport({
        city: loc.city,
        state: loc.state || loc.name,
        lat: loc.lat,
        lng: loc.lng,
        event: category,
        autoCategory: category,
        source: "IMD API",
        ts: Date.now(),
        trust: trustScore,
        status,
        hasPhoto: false,
        hasVideo: false,
        text: description,
        duplicateOf: null,
        mediaHash: null,
        mediaPath: null,
        perceptualHash: null,
      });

      seenGuids.add(item.guid);
      created.push(report);
    }
  } catch (e) {
    console.error("IMD CAP ingestion failed:", e.message);
  }

  if (created.length) {
    console.log(`IMD CAP ingestion: created ${created.length} report(s) from IMD's official alert feed.`);
  }
  return created;
}

module.exports = { runImdCapIngestion };