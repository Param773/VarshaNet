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

const FEED_URL = "https://cap-sources.s3.amazonaws.com/in-imd-en/rss.xml";

// Used to guess which state an alert is about from its description text
// (e.g. "...very likely over East Madhya Pradesh"), and to give that state
// a representative city/coordinate for the map. Checked longest-name-first
// so "Uttar Pradesh" doesn't get shadowed by a shorter partial match.
const STATE_LOCATIONS = [
  { name: "Andhra Pradesh", city: "Vijayawada", lat: 16.5062, lng: 80.648 },
  { name: "Arunachal Pradesh", city: "Itanagar", lat: 27.0844, lng: 93.6053 },
  { name: "Himachal Pradesh", city: "Shimla", lat: 31.1048, lng: 77.1734 },
  { name: "Madhya Pradesh", city: "Bhopal", lat: 23.2599, lng: 77.4126 },
  { name: "Uttar Pradesh", city: "Lucknow", lat: 26.8467, lng: 80.9462 },
  { name: "West Bengal", city: "Kolkata", lat: 22.5726, lng: 88.3639 },
  { name: "Tamil Nadu", city: "Chennai", lat: 13.0827, lng: 80.2707 },
  { name: "Jammu and Kashmir", city: "Srinagar", lat: 34.0837, lng: 74.7973 },
  { name: "Chhattisgarh", city: "Raipur", lat: 21.2514, lng: 81.6296 },
  { name: "Uttarakhand", city: "Dehradun", lat: 30.3165, lng: 78.0322 },
  { name: "Maharashtra", city: "Mumbai", lat: 19.076, lng: 72.8777 },
  { name: "Karnataka", city: "Bengaluru", lat: 12.9716, lng: 77.5946 },
  { name: "Rajasthan", city: "Jaipur", lat: 26.9124, lng: 75.7873 },
  { name: "Telangana", city: "Hyderabad", lat: 17.385, lng: 78.4867 },
  { name: "Jharkhand", city: "Ranchi", lat: 23.3441, lng: 85.3096 },
  { name: "Meghalaya", city: "Shillong", lat: 25.5788, lng: 91.8933 },
  { name: "Nagaland", city: "Kohima", lat: 25.6751, lng: 94.1086 },
  { name: "Mizoram", city: "Aizawl", lat: 23.7271, lng: 92.7176 },
  { name: "Manipur", city: "Imphal", lat: 24.817, lng: 93.9368 },
  { name: "Tripura", city: "Agartala", lat: 23.8315, lng: 91.2868 },
  { name: "Sikkim", city: "Gangtok", lat: 27.3389, lng: 88.6065 },
  { name: "Gujarat", city: "Ahmedabad", lat: 23.0225, lng: 72.5714 },
  { name: "Haryana", city: "Chandigarh", lat: 30.7333, lng: 76.7794 },
  { name: "Punjab", city: "Amritsar", lat: 31.634, lng: 74.8723 },
  { name: "Kerala", city: "Thiruvananthapuram", lat: 8.5241, lng: 76.9366 },
  { name: "Odisha", city: "Bhubaneswar", lat: 20.2961, lng: 85.8245 },
  { name: "Assam", city: "Guwahati", lat: 26.1445, lng: 91.7362 },
  { name: "Bihar", city: "Patna", lat: 25.5941, lng: 85.1376 },
  { name: "Goa", city: "Panaji", lat: 15.4909, lng: 73.8278 },
  { name: "Delhi", city: "Delhi", lat: 28.7041, lng: 77.1025 },
];

const DEFAULT_LOCATION = { state: "India", city: "New Delhi", lat: 28.6139, lng: 77.209 };

function detectState(text) {
  const lower = (text || "").toLowerCase();
  const match = STATE_LOCATIONS.find((s) => lower.indexOf(s.name.toLowerCase()) > -1);
  return match || null;
}

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