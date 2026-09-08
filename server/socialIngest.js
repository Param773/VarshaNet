// Real social-media ingestion pipeline.
//
// Why Reddit and not Twitter/X: Twitter/X API v2's search endpoint (the one
// that can look up #IMD-style hashtags) has required a paid Basic-tier
// developer plan since 2023 — there is no free, keyless way to search
// live tweets anymore. Rather than fake it (the way the old seed data
// labelled random demo rows "X / Twitter"), this module hits a social
// platform that is genuinely free, keyless, and live: Reddit's public
// read-only search JSON endpoint. It's a real "social media platform"
// per the problem statement, and posts are picked up the same way a
// hashtag-based Twitter listener would — by matching weather keywords
// and hashtag-style tokens in post text.
//
// This is written so a Twitter/X adapter can be dropped in later without
// touching scoring/db code: swap fetchRedditSearch()'s call site for a
// Twitter v2 recent-search call, keep everything from `category =
// detectCategory(...)` onward exactly the same, and reports will keep
// flowing into the same pipeline under a new `source` label.
//
// Reports from here are tagged source: "Social Media (Reddit)" — kept
// distinct from the "X / Twitter" label still used only in historical
// demo/seed data, so the two are never confused as the same kind of proof.

const db = require("./db");
const { scoreReport, statusFromTrust, detectCategory } = require("./scoring");
const { BoundedSet } = require("./boundedSet");

// Mix of English keywords, hashtag-style terms, and a couple of Hindi
// words (rain / flood) so this isn't purely English-only — a small step
// toward the multi-language gap, though full regional-language NLP is
// still separate future work, not solved here.
const SEARCH_QUERIES = [
  "IMD rain India",
  "IMD flood alert",
  "Mumbai rains",
  "Delhi flooding",
  "Chennai rain today",
  "Bengaluru flood",
  "heatwave India",
  "dust storm India",
  "\u092C\u093E\u0930\u093F\u0936 \u0905\u0932\u0930\u094D\u091F", // बारिश अलर्ट (rain alert)
  "\u092C\u093E\u0922\u093C \u092D\u093E\u0930\u0924", // बाढ़ भारत (flood India)
];

// Used to guess which city a post is about from its text, same approach
// as imdCapIngest.js's STATE_LOCATIONS. Checked in order, so a post
// mentioning multiple cities resolves to whichever is listed first.
const CITY_HINTS = [
  { name: "Mumbai", state: "Maharashtra", lat: 19.076, lng: 72.8777 },
  { name: "Delhi", state: "Delhi", lat: 28.7041, lng: 77.1025 },
  { name: "Chennai", state: "Tamil Nadu", lat: 13.0827, lng: 80.2707 },
  { name: "Bengaluru", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { name: "Bangalore", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { name: "Kolkata", state: "West Bengal", lat: 22.5726, lng: 88.3639 },
  { name: "Hyderabad", state: "Telangana", lat: 17.385, lng: 78.4867 },
  { name: "Pune", state: "Maharashtra", lat: 18.5204, lng: 73.8567 },
  { name: "Ahmedabad", state: "Gujarat", lat: 23.0225, lng: 72.5714 },
  { name: "Jaipur", state: "Rajasthan", lat: 26.9124, lng: 75.7873 },
  { name: "Lucknow", state: "Uttar Pradesh", lat: 26.8467, lng: 80.9462 },
  { name: "Kochi", state: "Kerala", lat: 9.9312, lng: 76.2673 },
  { name: "Guwahati", state: "Assam", lat: 26.1445, lng: 91.7362 },
];

const DEFAULT_LOCATION = { name: "New Delhi", state: "Delhi", lat: 28.6139, lng: 77.209 };

function detectCity(text) {
  const lower = (text || "").toLowerCase();
  return CITY_HINTS.find((c) => lower.indexOf(c.name.toLowerCase()) > -1) || null;
}

// Pulls out #hashtag-style tokens (Latin + Devanagari) so they can be
// stored alongside the report — this is the literal "#IMD and other
// relevant weather hashtags" metadata the problem statement asks for.
function extractHashtags(text) {
  const matches = (text || "").match(/#[\w\u0900-\u097F]+/g) || [];
  return matches.map((h) => h.toLowerCase());
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Reddit's unauthenticated JSON endpoint is real and keyless, but it is
// rate-limit sensitive and occasionally 429s under bursty/shared-IP
// conditions (hackathon wifi, shared hosting) — retry with backoff before
// giving up, same pattern as weather.js's fetchWithRetry.
const MAX_RETRIES = 2;
const RETRY_BASE_DELAY_MS = 1500;

async function fetchRedditSearch(query, attempt = 0) {
  const url =
    "https://www.reddit.com/search.json?q=" +
    encodeURIComponent(query) +
    "&sort=new&limit=10";
  const res = await fetch(url, {
    headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
  });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    await sleep(RETRY_BASE_DELAY_MS * Math.pow(2, attempt));
    return fetchRedditSearch(query, attempt + 1);
  }
  if (!res.ok) {
    throw new Error(`Reddit search failed for "${query}" (HTTP ${res.status})`);
  }
  const json = await res.json();
  const children = (json.data && json.data.children) || [];
  return children.map((c) => c.data);
}

// Avoid re-creating a report for a post already ingested. Bounded so this
// doesn't grow forever over the process's lifetime, same pattern as the
// other ingestion jobs' seenGuids sets.
const seenPostIds = new BoundedSet(5000);

async function ingestQuery(query) {
  const created = [];
  try {
    const posts = await fetchRedditSearch(query);
    for (const post of posts) {
      const id = post.id || post.name;
      if (!id || seenPostIds.has(id)) continue;
      seenPostIds.add(id); // mark seen even if skipped below, so we don't re-check it every run

      const title = post.title || "";
      const body = post.selftext || "";
      const combinedText = `${title} ${body}`.trim();
      if (!combinedText) continue;

      // Only ever create a report when a real weather-category keyword is
      // confidently found — same conservative rule sachetIngest.js and
      // imdCapIngest.js use, so an off-topic post never becomes a
      // mis-categorized "weather report".
      const category = detectCategory(combinedText);
      if (!category) continue;

      const hashtags = extractHashtags(combinedText);
      const cityHint = detectCity(combinedText) || DEFAULT_LOCATION;
      const description =
        combinedText.length > 300 ? combinedText.slice(0, 300) + "\u2026" : combinedText;
      const hasThumbnail = !!post.thumbnail && post.thumbnail.indexOf("http") === 0;

      const { trustScore } = scoreReport({
        description,
        event: category,
        hasMedia: hasThumbnail,
        mediaReused: false,
        officialMain: null,
        city: cityHint.name,
      });
      const status = statusFromTrust(trustScore);

      const report = await db.addReport({
        city: cityHint.name,
        state: cityHint.state,
        lat: cityHint.lat,
        lng: cityHint.lng,
        event: category,
        autoCategory: category,
        source: "Social Media (Reddit)",
        sourceUrl: post.permalink ? `https://reddit.com${post.permalink}` : null,
        hashtags,
        ts: post.created_utc ? post.created_utc * 1000 : Date.now(),
        trust: trustScore,
        status,
        hasPhoto: hasThumbnail,
        hasVideo: false,
        text: description,
        duplicateOf: null,
        mediaHash: null,
        mediaPath: null,
        perceptualHash: null,
      });

      created.push(report);
    }
  } catch (e) {
    console.error(`Social ingestion failed for query "${query}":`, e.message);
  }
  return created;
}

async function runSocialIngestion() {
  const created = [];
  for (const query of SEARCH_QUERIES) {
    const reports = await ingestQuery(query);
    created.push(...reports);
    await sleep(1200); // stay well under Reddit's unauthenticated rate limit
  }
  if (created.length) {
    console.log(`Social ingestion: created ${created.length} report(s) from public social posts.`);
  }
  return created;
}

module.exports = { runSocialIngestion, SEARCH_QUERIES };
