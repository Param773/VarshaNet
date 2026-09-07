// Real public-dataset ingestion: pulls live disaster/weather alerts from
// NDMA's SACHET portal (sachet.ndma.gov.in) — India's official, government
// -run Common Alerting Protocol feed that aggregates warnings from IMD, CWC,
// and state disaster authorities. This is a genuinely different,
// independently live public data source from the Open-Meteo weather feed,
// matching the problem statement's "public datasets" / government-source
// requirement. Reports from here are tagged source: "Public Dataset".
//
// Feed format: state-wise RSS/XML, e.g.
//   https://sachet.ndma.gov.in/cap_public_website/rss/rss_kerala.xml
// The Kerala feed is confirmed live and working. Other states follow the
// same documented naming convention but haven't each been individually
// verified — any state whose feed doesn't resolve is silently skipped, the
// same way a failed city is handled in the weather ingestion job.
//
// Many alert titles are in a regional language (SACHET supports 12 Indian
// languages) with only fragments of English/technical terms (e.g. "40 kmph",
// "IMD"). Rather than guess, a report is only created when an English
// disaster-type keyword is confidently found in the title — anything else
// is skipped so we never show a mis-categorized report.

const db = require("./db");
const { scoreReport, statusFromTrust } = require("./scoring");
const { BoundedSet } = require("./boundedSet");

const STATES = [
  { slug: "kerala", state: "Kerala", city: "Thiruvananthapuram", lat: 8.5241, lng: 76.9366 },
  { slug: "tamilnadu", state: "Tamil Nadu", city: "Chennai", lat: 13.0827, lng: 80.2707 },
  { slug: "karnataka", state: "Karnataka", city: "Bengaluru", lat: 12.9716, lng: 77.5946 },
  { slug: "maharashtra", state: "Maharashtra", city: "Mumbai", lat: 19.076, lng: 72.8777 },
  { slug: "gujarat", state: "Gujarat", city: "Ahmedabad", lat: 23.0225, lng: 72.5714 },
  { slug: "andhrapradesh", state: "Andhra Pradesh", city: "Vijayawada", lat: 16.5062, lng: 80.648 },
  { slug: "telangana", state: "Telangana", city: "Hyderabad", lat: 17.385, lng: 78.4867 },
  { slug: "odisha", state: "Odisha", city: "Bhubaneswar", lat: 20.2961, lng: 85.8245 },
  { slug: "westbengal", state: "West Bengal", city: "Kolkata", lat: 22.5726, lng: 88.3639 },
  { slug: "assam", state: "Assam", city: "Guwahati", lat: 26.1445, lng: 91.7362 },
  { slug: "bihar", state: "Bihar", city: "Patna", lat: 25.5941, lng: 85.1376 },
  { slug: "rajasthan", state: "Rajasthan", city: "Jaipur", lat: 26.9124, lng: 75.7873 },
  { slug: "punjab", state: "Punjab", city: "Amritsar", lat: 31.634, lng: 74.8723 },
  { slug: "delhi", state: "Delhi", city: "Delhi", lat: 28.7041, lng: 77.1025 },
  { slug: "uttarpradesh", state: "Uttar Pradesh", city: "Lucknow", lat: 26.8467, lng: 80.9462 },
];

const CATEGORY_SIGNALS = [
  { key: "flooding", words: ["flood"] },
  { key: "thunderstorm", words: ["thunderstorm", "lightning", "thunder"] },
  { key: "heatwave", words: ["heat wave", "heatwave"] },
  { key: "fog", words: ["fog", "mist", "visibility"] },
  { key: "dust_storm", words: ["dust storm", "duststorm", "dust"] },
  { key: "strong_winds", words: ["wind", "kmph", "gust", "cyclone", "squall"] },
  { key: "rainfall", words: ["rain", "rainfall", "downpour", "drizzle"] },
];

function detectCategoryFromTitle(title) {
  const lower = (title || "").toLowerCase();
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
    const guidMatch = block.match(/<guid[^>]*>([\s\S]*?)<\/guid>/);
    if (!titleMatch) return;
    items.push({
      title: titleMatch[1].trim(),
      guid: guidMatch ? guidMatch[1].trim() : null,
    });
  });
  return items;
}

// Avoid re-creating a report for an alert already ingested — SACHET guids
// are stable per-alert, so this is exact, not fuzzy like the weather
// ingestion job's per-city cooldown. Bounded so this doesn't grow forever
// over the process's lifetime (15 states x 5 items/run is small per run,
// but "forever" adds up).
const seenGuids = new BoundedSet(5000);

async function ingestState(entry) {
  const url = `https://sachet.ndma.gov.in/cap_public_website/rss/rss_${entry.slug}.xml`;
  const created = [];
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
    });
    if (!res.ok) {
      console.log(`SACHET feed not available for ${entry.state} (HTTP ${res.status}) — skipping.`);
      return created;
    }
    const xml = await res.text();
    const items = parseRssItems(xml).slice(0, 5); // newest 5 per state, per run

    for (const item of items) {
      if (!item.guid || seenGuids.has(item.guid)) continue;
      const category = detectCategoryFromTitle(item.title);
      if (!category) continue; // can't confidently categorize — often regional-language-only

      const description = item.title.length > 300 ? item.title.slice(0, 300) + "…" : item.title;
      const { trustScore } = scoreReport({
        description,
        event: category,
        hasMedia: false,
        mediaReused: false,
        officialMain: null,
        city: entry.city,
      });
      const status = statusFromTrust(trustScore);

      const report = await db.addReport({
        city: entry.city,
        state: entry.state,
        lat: entry.lat,
        lng: entry.lng,
        event: category,
        autoCategory: category,
        source: "Public Dataset",
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
    console.error(`SACHET ingestion failed for ${entry.state}:`, e.message);
  }
  return created;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runSachetIngestion() {
  const created = [];
  for (const entry of STATES) {
    const reports = await ingestState(entry);
    created.push(...reports);
    await sleep(300); // gentle pacing, same courtesy as the weather ingestion job
  }
  if (created.length) {
    console.log(`SACHET ingestion: created ${created.length} report(s) from NDMA public alerts.`);
  }
  return created;
}

module.exports = { runSachetIngestion };