// Real public-dataset ingestion: pulls live disaster/weather alerts from
// NDMA's SACHET portal (sachet.ndma.gov.in) — India's official, government
// -run Common Alerting Protocol feed that aggregates warnings from IMD, CWC,
// and state disaster authorities. Reports from here are tagged
// source: "Public Dataset".
//
// Feed used: https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml
// — this is SACHET's own single ALL-INDIA feed (linked straight off NDMA's
// official "RSS Feed" page, sachet.ndma.gov.in/CapFeed), not a per-state
// guess. It was fetched and manually verified live while building this
// file: it returns real, current CAP alerts from IMD regional offices, CWC
// (river-level warnings) and state SDMAs, dated to the day of the check.
//
// An earlier version of this file looped over 15 individually-guessed
// per-state URLs (rss_<state>.xml) and only had one of them (Kerala)
// actually verified — the rest were an unverified assumption riding on the
// same naming pattern, silently skipped if wrong. That's been replaced with
// this single documented national feed: it's simpler, doesn't depend on
// guessing state-slug spelling, and covers all states/UTs in one call
// instead of just 15.
//
// Many alert titles are in a regional language (SACHET supports 12 Indian
// languages) with only fragments of English/technical terms. Rather than
// guess, a report is only created when an English disaster-type keyword is
// confidently found in the title — anything else is skipped so we never
// show a mis-categorized report. When the title *is* categorizable but
// still in a regional script, it's translated to English via MyMemory's
// free translation API (no key required) before being stored, using the
// alert's detected state to pick the right source language — otherwise the
// admin console just shows unreadable-to-most-readers regional text.

const db = require("./db");
const { scoreReport, statusFromTrust } = require("./scoring");
const { BoundedSet } = require("./boundedSet");
const { DEFAULT_LOCATION, detectState, detectStateFromAuthor } = require("./stateLocations");

const FEED_URL = "https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml";

const CATEGORY_SIGNALS = [
  { key: "flooding", words: ["flood", "river", "water level", "submerged", "overflow"] },
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

// Best-effort state -> majority-language mapping, used only to pick the
// right source language for translation. States not listed here simply
// don't get translated (original text is kept) rather than risk a wrong
// source-language guess producing garbled output.
const STATE_LANGUAGE = {
  Kerala: "ml",
  "Tamil Nadu": "ta",
  Karnataka: "kn",
  "Andhra Pradesh": "te",
  Telangana: "te",
  Maharashtra: "mr",
  Gujarat: "gu",
  Odisha: "or",
  "West Bengal": "bn",
  Assam: "as",
  Punjab: "pa",
  Bihar: "hi",
  Rajasthan: "hi",
  "Uttar Pradesh": "hi",
  "Madhya Pradesh": "hi",
  Delhi: "hi",
  Haryana: "hi",
  Chhattisgarh: "hi",
  Jharkhand: "hi",
  Uttarakhand: "hi",
  "Himachal Pradesh": "hi",
};

// True if the text contains characters from any major Indian script block —
// i.e. it's not plain English, even if it contains scattered English
// fragments like "40 kmph".
function containsIndicScript(text) {
  return /[\u0900-\u0D7F]/.test(text || "");
}

// Translates regional-language alert text to English using MyMemory's free
// translation API (no key required, genuinely meant to be called
// programmatically — unlike scraping a platform that doesn't want to be
// machine-read). Free tier is rate-limited (5,000 characters/day per IP),
// so this only runs on text that's actually non-English. Any failure just
// falls back to the original text rather than blocking the report.
async function translateToEnglish(text, sourceLang) {
  if (!text || !sourceLang) return text;
  try {
    const truncated = text.slice(0, 480); // MyMemory's per-request limit is 500 bytes
    const url =
      "https://api.mymemory.translated.net/get?q=" +
      encodeURIComponent(truncated) +
      "&langpair=" +
      sourceLang +
      "|en";
    const res = await fetch(url, {
      headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
    });
    if (!res.ok) return text;
    const data = await res.json();
    const translated = data && data.responseData && data.responseData.translatedText;
    return translated || text;
  } catch (e) {
    console.error("Translation failed, using original text:", e.message);
    return text;
  }
}

// Minimal, dependency-free RSS <item> parser — this feed's items are simple
// and well-formed (no CDATA, no nested items). description is present but
// always empty on this feed; title carries the actual alert text.
function parseRssItems(xml) {
  const items = [];
  const itemBlocks = xml.split("<item>").slice(1);
  itemBlocks.forEach((block) => {
    const titleMatch = block.match(/<title>([\s\S]*?)<\/title>/);
    const authorMatch = block.match(/<author>([\s\S]*?)<\/author>/);
    const guidMatch = block.match(/<guid[^>]*>([\s\S]*?)<\/guid>/);
    if (!titleMatch) return;
    items.push({
      title: titleMatch[1].trim(),
      author: authorMatch ? authorMatch[1].trim() : "",
      guid: guidMatch ? guidMatch[1].trim() : null,
    });
  });
  return items;
}

const seenGuids = new BoundedSet(5000);

// The national feed easily carries 60-100+ live items per poll (IMD +
// CWC + state SDMAs combined) — capped here so one ingestion run stays
// bounded, same spirit as the old per-state cap of 5 x 15 states.
const MAX_ITEMS_PER_RUN = 60;

async function runSachetIngestion() {
  const created = [];
  try {
    const res = await fetch(FEED_URL, {
      headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
    });
    if (!res.ok) {
      console.log(`SACHET national feed unavailable (HTTP ${res.status}) — skipping this run.`);
      return created;
    }
    const xml = await res.text();
    const items = parseRssItems(xml).slice(0, MAX_ITEMS_PER_RUN);

    for (const item of items) {
      if (!item.guid || seenGuids.has(item.guid)) continue;
      const category = detectCategoryFromTitle(item.title);
      if (!category) continue;

      const loc = detectStateFromAuthor(item.author) || detectState(item.title) || DEFAULT_LOCATION;

      let description = item.title.length > 300 ? item.title.slice(0, 300) + "…" : item.title;
      if (containsIndicScript(description)) {
        const stateName = loc.state || loc.name;
        const sourceLang = STATE_LANGUAGE[stateName];
        if (sourceLang) {
          description = await translateToEnglish(description, sourceLang);
        }
      }

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
    console.error("SACHET ingestion failed:", e.message);
  }

  if (created.length) {
    console.log(`SACHET ingestion: created ${created.length} report(s) from NDMA's national CAP feed.`);
  }
  return created;
}

module.exports = { runSachetIngestion };